package main

import (
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"strings"

	"github.com/filecoin-project/boost/retrieve"
	"github.com/filecoin-project/go-address"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
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
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var flagMiner = &cli.StringFlag{
	Name:    "miner",
	Aliases: []string{"m"},
}

var flagMinerRequired = &cli.StringFlag{
	Name:     flagMiner.Name,
	Aliases:  flagMiner.Aliases,
	Required: true,
}

var flagMiners = &cli.StringSliceFlag{
	Name:    "miners",
	Aliases: []string{"miner", "m"},
}

var flagMinersRequired = &cli.StringSliceFlag{
	Name:     flagMiners.Name,
	Aliases:  flagMiners.Aliases,
	Required: true,
}

var flagOutput = &cli.StringFlag{
	Name:    "output",
	Aliases: []string{"o"},
}

var flagNetwork = &cli.StringFlag{
	Name:        "network",
	Aliases:     []string{"n"},
	Usage:       "which network to retrieve from [fil|ipfs|auto]",
	DefaultText: NetworkAuto,
	Value:       NetworkAuto,
}

var flagCar = &cli.BoolFlag{
	Name: "car",
}

const (
	NetworkFIL  = "fil"
	NetworkIPFS = "ipfs"
	NetworkAuto = "auto"
)

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
		flagNetwork,
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

		network := strings.ToLower(strings.TrimSpace(cctx.String("network")))

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

		// Set up node and filclient

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

		var networks []RetrievalAttempt

		if network == NetworkIPFS || network == NetworkAuto {
			if selNode != nil && !selNode.IsNull() {
				// Selector nodes are not compatible with IPFS
				if network == NetworkIPFS {
					log.Fatal("IPFS is not compatible with selector node")
				} else {
					log.Info("A selector node has been specified, skipping IPFS")
				}
			} else {
				networks = append(networks, &IPFSRetrievalAttempt{
					Cid: c,
				})
			}
		}

		if network == NetworkFIL || network == NetworkAuto {
			networks = append(networks, &FILRetrievalAttempt{
				FilClient:  fc,
				Cid:        c,
				Candidates: candidates,
				SelNode:    selNode,
			})
		}

		if len(networks) == 0 {
			log.Fatalf("Unknown --network value \"%s\"", network)
		}

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
			if err := retrieve.TraverseDag(
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
			car.WriteCar(cctx.Context, dservOffline, []cid.Cid{c}, file)

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

// Read a single miner from the CLI, returning address.Undef if none is
// provided.
func parseMiner(cctx *cli.Context) (address.Address, error) {
	minerStringRaw := cctx.String(flagMiner.Name)

	miner, err := address.NewFromString(minerStringRaw)
	if err != nil {
		return address.Undef, fmt.Errorf("failed to parse miner: %s: %w", minerStringRaw, err)
	}

	return miner, nil
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
