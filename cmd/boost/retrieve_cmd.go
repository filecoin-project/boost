package main

import (
	"context"
	crand "crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/filecoin-project/boost/lib/keystore"
	"github.com/filecoin-project/boost/markets/utils"
	"github.com/filecoin-project/boost/retrieve"

	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/wallet"
	lcli "github.com/filecoin-project/lotus/cli"

	"github.com/filecoin-project/go-address"

	"github.com/ipfs/go-bitswap"
	bsnet "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	flatfs "github.com/ipfs/go-ds-flatfs"
	levelds "github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	files "github.com/ipfs/go-ipfs-files"
	"github.com/ipfs/go-merkledag"
	unixfile "github.com/ipfs/go-unixfs/file"
	"github.com/ipld/go-car"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	textselector "github.com/ipld/go-ipld-selector-text-lite"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/metrics"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var flagMiners = &cli.StringSliceFlag{
	Name:    "miners",
	Aliases: []string{"miner", "m"},
}

var flagOutput = &cli.StringFlag{
	Name:    "output",
	Aliases: []string{"o"},
}

var flagCar = &cli.BoolFlag{
	Name: "car",
}

var flagDmPathSel = &cli.StringFlag{
	Name:  "datamodel-path-selector",
	Usage: "a rudimentary (DM-level-only) text-path selector, allowing for sub-selection within a deal",
}

var retrieveCmd = &cli.Command{
	Name:        "retrieve",
	Usage:       "Retrieve a file by CID from a miner",
	Description: "Retrieve a file by CID from a miner. If desired, multiple miners can be specified as fallbacks in case of a failure (comma-separated, no spaces).",
	ArgsUsage:   "<cid>",
	Flags: []cli.Flag{
		flagMiners,
		flagOutput,
		flagDmPathSel,
		flagCar,
	},
	Action: func(cctx *cli.Context) error {

		// Store config dir in metadata
		ddir, err := homedir.Expand("~/.boost-client")
		if err != nil {
			fmt.Println("could not set config dir: ", err)
		}

		// Parse command input

		cidStr := cctx.Args().First()
		if cidStr == "" {
			return fmt.Errorf("please specify a CID to retrieve")
		}

		dmSelText := textselector.Expression(cctx.String(flagDmPathSel.Name))

		miners, err := parseMiners(cctx)
		if err != nil {
			return err
		}

		output, err := parseOutput(cctx)
		if err != nil {
			return err
		}
		if output == "" {
			output = cidStr
			if dmSelText != "" {
				output += "_" + url.QueryEscape(string(dmSelText))
			}
		}

		c, err := cid.Decode(cidStr)
		if err != nil {
			return err
		}

		// Get subselector node

		var selNode ipld.Node
		if dmSelText != "" {
			ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)

			selspec, err := textselector.SelectorSpecFromPath(
				dmSelText,
				true,

				// URGH - this is a direct copy from https://github.com/filecoin-project/go-fil-markets/blob/v1.12.0/shared/selectors.go#L10-L16
				// Unable to use it because we need the SelectorSpec, and markets exposes just a reified node
				ssb.ExploreRecursive(
					selector.RecursionLimitNone(),
					ssb.ExploreAll(ssb.ExploreRecursiveEdge()),
				),
			)
			if err != nil {
				return xerrors.Errorf("failed to parse text-selector '%s': %w", dmSelText, err)
			}

			selNode = selspec.Node()
		}

		// Set up node and retrieval client
		node, err := setup(cctx.Context, ddir)
		if err != nil {
			return err
		}

		fc, closer, err := clientFromNode(cctx, node, ddir)
		if err != nil {
			return err
		}
		defer closer()

		// Collect retrieval candidates and config. If one or more miners are
		// provided, use those with the requested cid as the root cid as the
		// candidate list. Otherwise, we can use the auto retrieve API endpoint
		// to automatically find some candidates to retrieve from.

		var candidates []FILRetrievalCandidate
		if len(miners) > 0 {
			for _, miner := range miners {
				candidates = append(candidates, FILRetrievalCandidate{
					Miner:   miner,
					RootCid: c,
				})
			}
		} else {
			endpoint := "https://api.estuary.tech/retrieval-candidates" // TODO: don't hard code
			candidates_, err := node.GetRetrievalCandidates(endpoint, c)
			if err != nil {
				return fmt.Errorf("failed to get retrieval candidates: %w", err)
			}

			candidates = candidates_
		}

		// Do the retrieval
		var networks = []RetrievalAttempt{&FILRetrievalAttempt{
			Client:     fc,
			Cid:        c,
			Candidates: candidates,
			SelNode:    selNode,
		}}

		stats, err := node.RetrieveFromBestCandidate(cctx.Context, networks)
		if err != nil {
			return err
		}

		printRetrievalStats(stats)

		// Save the output

		dservOffline := merkledag.NewDAGService(blockservice.New(node.Blockstore, offline.Exchange(node.Blockstore)))

		// if we used a selector - need to find the sub-root the user actually wanted to retrieve
		if dmSelText != "" {
			var subRootFound bool

			// no err check - we just compiled this before starting, but now we do not wrap a `*`
			selspec, _ := textselector.SelectorSpecFromPath(dmSelText, true, nil) //nolint:errcheck
			if err := utils.TraverseDag(
				cctx.Context,
				dservOffline,
				c,
				selspec.Node(),
				func(p traversal.Progress, n ipld.Node, r traversal.VisitReason) error {
					if r == traversal.VisitReason_SelectionMatch {

						if p.LastBlock.Path.String() != p.Path.String() {
							return xerrors.Errorf("unsupported selection path '%s' does not correspond to a node boundary (a.k.a. CID link)", p.Path.String())
						}

						cidLnk, castOK := p.LastBlock.Link.(cidlink.Link)
						if !castOK {
							return xerrors.Errorf("cidlink cast unexpectedly failed on '%s'", p.LastBlock.Link.String())
						}

						c = cidLnk.Cid
						subRootFound = true
					}
					return nil
				},
			); err != nil {
				return xerrors.Errorf("error while locating partial retrieval sub-root: %w", err)
			}

			if !subRootFound {
				return xerrors.Errorf("path selection '%s' does not match a node within %s", dmSelText, c)
			}
		}

		dnode, err := dservOffline.Get(cctx.Context, c)
		if err != nil {
			return err
		}

		if cctx.Bool(flagCar.Name) {
			// Write file as car file
			file, err := os.Create(output + ".car")
			if err != nil {
				return err
			}
			_ = car.WriteCar(cctx.Context, dservOffline, []cid.Cid{c}, file)

			fmt.Println("Saved .car output to", output+".car")
		} else {
			// Otherwise write file as UnixFS File
			ufsFile, err := unixfile.NewUnixfsFile(cctx.Context, dservOffline, dnode)
			if err != nil {
				return err
			}

			if err := files.WriteTo(ufsFile, output); err != nil {
				return err
			}

			fmt.Println("Saved output to", output)
		}

		return nil
	},
}

// Read a comma-separated or multi flag list of miners from the CLI.
func parseMiners(cctx *cli.Context) ([]address.Address, error) {
	// Each minerStringsRaw element may contain multiple comma-separated values
	minerStringsRaw := cctx.StringSlice(flagMiners.Name)

	// Split any comma-separated minerStringsRaw elements
	var minerStrings []string
	for _, raw := range minerStringsRaw {
		minerStrings = append(minerStrings, strings.Split(raw, ",")...)
	}

	var miners []address.Address
	for _, ms := range minerStrings {
		miner, err := address.NewFromString(ms)
		if err != nil {
			return nil, fmt.Errorf("failed to parse miner %s: %w", ms, err)
		}

		miners = append(miners, miner)
	}

	if len(miners) == 0 {
		return nil, errors.New("you must specify at least one miner address")
	}

	return miners, nil
}

// Get the destination file to write the output to, erroring if not a valid
// path. This early error check is important because you don't want to do a
// bunch of work, only to end up crashing when you try to write the file.
func parseOutput(cctx *cli.Context) (string, error) {
	path := cctx.String(flagOutput.Name)

	if path != "" && !fs.ValidPath(path) {
		return "", fmt.Errorf("invalid output location '%s'", path)
	}

	return path, nil
}

func keyPath(baseDir string) string {
	return filepath.Join(baseDir, "libp2p.key")
}

func blockstorePath(baseDir string) string {
	return filepath.Join(baseDir, "blockstore")
}

func datastorePath(baseDir string) string {
	return filepath.Join(baseDir, "datastore")
}

func walletPath(baseDir string) string {
	return filepath.Join(baseDir, "wallet")
}

func clientFromNode(cctx *cli.Context, nd *Node, dir string) (*retrieve.Client, func(), error) {
	api, closer, err := lcli.GetGatewayAPI(cctx)
	if err != nil {
		return nil, nil, err
	}

	addr, err := nd.Wallet.GetDefault()
	if err != nil {
		return nil, nil, err
	}

	c, err := retrieve.NewClient(nd.Host, api, nd.Wallet, addr, nd.Blockstore, nd.Datastore, dir)
	if err != nil {
		return nil, nil, err
	}

	return c, closer, nil
}

type Node struct {
	Host host.Host

	Datastore  datastore.Batching
	DHT        *dht.IpfsDHT
	Blockstore blockstore.Blockstore
	Bitswap    *bitswap.Bitswap

	Wallet *wallet.LocalWallet
}

func setup(ctx context.Context, cfgdir string) (*Node, error) {
	peerkey, err := loadOrInitPeerKey(keyPath(cfgdir))
	if err != nil {
		return nil, err
	}

	bwc := metrics.NewBandwidthCounter()

	h, err := libp2p.New(
		//libp2p.ConnectionManager(connmgr.NewConnManager(500, 800, time.Minute)),
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/6755"),
		libp2p.Identity(peerkey),
		libp2p.BandwidthReporter(bwc),
	)
	if err != nil {
		return nil, err
	}

	bstoreDatastore, err := flatfs.CreateOrOpen(blockstorePath(cfgdir), flatfs.NextToLast(3), false)
	bstore := blockstore.NewBlockstoreNoPrefix(bstoreDatastore)
	if err != nil {
		return nil, fmt.Errorf("blockstore could not be opened (it may be incompatible after an update - try running the clear blockstore subcommand to delete the blockstore and try again): %v", err)
	}

	ds, err := levelds.NewDatastore(datastorePath(cfgdir), nil)
	if err != nil {
		return nil, err
	}

	dht, err := dht.New(
		ctx,
		h,
		dht.Mode(dht.ModeClient),
		dht.QueryFilter(dht.PublicQueryFilter),
		dht.RoutingTableFilter(dht.PublicRoutingTableFilter),
		dht.BootstrapPeersFunc(dht.GetDefaultBootstrapPeerAddrInfos),
		dht.Datastore(ds),
		dht.RoutingTablePeerDiversityFilter(dht.NewRTPeerDiversityFilter(h, 2, 3)),
	)
	if err != nil {
		return nil, err
	}

	bsnet := bsnet.NewFromIpfsHost(h, dht)
	bswap := bitswap.New(ctx, bsnet, bstore)

	wallet, err := setupWallet(walletPath(cfgdir))
	if err != nil {
		return nil, err
	}

	return &Node{
		Host:       h,
		Blockstore: bstore,
		DHT:        dht,
		Datastore:  ds,
		Bitswap:    bswap,
		Wallet:     wallet,
	}, nil
}

func loadOrInitPeerKey(kf string) (crypto.PrivKey, error) {
	data, err := ioutil.ReadFile(kf)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		k, _, err := crypto.GenerateEd25519Key(crand.Reader)
		if err != nil {
			return nil, err
		}

		data, err := crypto.MarshalPrivateKey(k)
		if err != nil {
			return nil, err
		}

		if err := ioutil.WriteFile(kf, data, 0600); err != nil {
			return nil, err
		}

		return k, nil
	}
	return crypto.UnmarshalPrivateKey(data)
}

func setupWallet(dir string) (*wallet.LocalWallet, error) {
	kstore, err := keystore.OpenOrInitKeystore(dir)
	if err != nil {
		return nil, err
	}

	wallet, err := wallet.NewWallet(kstore)
	if err != nil {
		return nil, err
	}

	addrs, err := wallet.WalletList(context.TODO())
	if err != nil {
		return nil, err
	}

	if len(addrs) == 0 {
		_, err := wallet.WalletNew(context.TODO(), types.KTBLS)
		if err != nil {
			return nil, err
		}
	}

	return wallet, nil
}
