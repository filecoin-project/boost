package main

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	clinode "github.com/filecoin-project/boost/cli/node"
	"github.com/filecoin-project/boost/cmd"
	"github.com/filecoin-project/boost/extern/boostd-data/shared/cliutil"
	"github.com/filecoin-project/boost/markets/utils"
	rc "github.com/filecoin-project/boost/retrievalmarket/client"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/ipld/merkledag"
	unixfile "github.com/ipfs/boxo/ipld/unixfs/file"
	"github.com/ipfs/go-cid"
	flatfs "github.com/ipfs/go-ds-flatfs"
	levelds "github.com/ipfs/go-ds-leveldb"
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
	"golang.org/x/term"
	"golang.org/x/xerrors"
)

var flagProvider = &cli.StringFlag{
	Name:     "provider",
	Aliases:  []string{"p"},
	Usage:    "The miner ID of the Storage Provider",
	Required: true,
}

var flagOutput = &cli.StringFlag{
	Name:    "output",
	Aliases: []string{"o"},
	Usage:   "The path to the output file",
}

var flagCar = &cli.BoolFlag{
	Name:  "car",
	Usage: "Whether to output the data as a CAR file",
}

var flagDmPathSel = &cli.StringFlag{
	Name:  "datamodel-path-selector",
	Usage: "a rudimentary (DM-level-only) text-path selector, allowing for sub-selection within a deal",
}

var retrieveCmd = &cli.Command{
	Name:      "retrieve",
	Usage:     "Retrieve a file by payload CID from a miner",
	ArgsUsage: "<cid>",
	Flags: []cli.Flag{
		flagProvider,
		flagOutput,
		flagDmPathSel,
		flagCar,
	},
	Action: func(cctx *cli.Context) error {
		ctx := cliutil.ReqContext(cctx)

		cidStr := cctx.Args().First()
		if cidStr == "" {
			return fmt.Errorf("please specify a payload CID to retrieve")
		}
		c, err := cid.Decode(cidStr)
		if err != nil {
			return fmt.Errorf("decoding retrieval cid %s: %w", cidStr, err)
		}

		cfgdir := cctx.String(cmd.FlagRepo.Name)

		cfgdir, err = homedir.Expand(cfgdir)
		if err != nil {
			return fmt.Errorf("expanding homedir: %w", err)
		}

		node, err := clinode.Setup(cfgdir)
		if err != nil {
			return fmt.Errorf("setting up CLI node: %w", err)
		}

		dmSelText := textselector.Expression(cctx.String(flagDmPathSel.Name))

		miner, err := address.NewFromString(cctx.String(flagProvider.Name))
		if err != nil {
			return fmt.Errorf("failed to parse miner %s: %w", cctx.String(flagProvider.Name), err)
		}

		// Get the output path of the file
		output := cctx.String("output")
		if output == "" {
			output = cidStr
			if dmSelText != "" {
				output += "_" + url.QueryEscape(string(dmSelText))
			}
		}

		outputPath, err := homedir.Expand(output)
		if err != nil {
			return fmt.Errorf("expanding output path: %w", err)
		}

		// The output path must not exist already
		_, err = os.Stat(outputPath)
		if err == nil {
			return fmt.Errorf("there is already a file at output path %s", outputPath)
		}
		if !os.IsNotExist(err) {
			return fmt.Errorf("checking output path %s: %w", outputPath, err)
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

		api, closer, err := lcli.GetGatewayAPI(cctx)
		if err != nil {
			return fmt.Errorf("setting up gateway connection: %w", err)
		}
		defer closer()

		addr, err := node.Wallet.GetDefault()
		if err != nil {
			return err
		}

		// Set up a blockstore in a temp directory
		bstoreTmpDir, err := os.MkdirTemp("", "retrieve-blockstore")
		if err != nil {
			return fmt.Errorf("setting up temp dir: %w", err)
		}
		// Clean up the temp directory before exiting
		defer os.RemoveAll(bstoreTmpDir)

		bstoreDatastore, err := flatfs.CreateOrOpen(bstoreTmpDir, flatfs.NextToLast(3), false)
		bstore := blockstore.NewBlockstore(bstoreDatastore, blockstore.NoPrefix())
		if err != nil {
			return fmt.Errorf("could not open blockstore: %w", err)
		}

		// Set up a datastore in a temp directory
		datastoreTmpDir, err := os.MkdirTemp("", "retrieve-datastore")
		if err != nil {
			return fmt.Errorf("setting up temp dir: %w", err)
		}
		// Clean up the temp directory before exiting
		defer os.RemoveAll(datastoreTmpDir)

		ds, err := levelds.NewDatastore(datastoreTmpDir, nil)
		if err != nil {
			return fmt.Errorf("could not create datastore: %w", err)
		}

		// Create the retrieval client
		fc, err := rc.NewClient(node.Host, api, node.Wallet, addr, bstore, ds, cfgdir)
		if err != nil {
			return err
		}

		query, err := fc.RetrievalQuery(ctx, miner, c)
		if err != nil {
			return fmt.Errorf("retrieval query for miner %s failed: %v", miner, err)
		}

		proposal, err := rc.RetrievalProposalForAsk(query, c, selNode)
		if err != nil {
			return fmt.Errorf("Failed to create retrieval proposal with candidate miner %s: %v", miner, err)
		}

		// Retrieve the data
		stats, err := fc.RetrieveContentWithProgressCallback(
			ctx,
			miner,
			proposal,
			func(bytesReceived_ uint64) {
				printProgress(bytesReceived_)
			},
		)
		if err != nil {
			return fmt.Errorf("Failed to retrieve content with candidate miner %s: %v", miner, err)
		}

		printRetrievalStats(&FILRetrievalStats{RStats: *stats})

		dservOffline := merkledag.NewDAGService(blockservice.New(bstore, offline.Exchange(bstore)))

		// if we used a selector - need to find the sub-root the user actually wanted to retrieve
		if dmSelText != "" {
			var subRootFound bool

			// no err check - we just compiled this before starting, but now we do not wrap a `*`
			selspec, _ := textselector.SelectorSpecFromPath(dmSelText, true, nil) //nolint:errcheck
			if err := utils.TraverseDag(
				ctx,
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

		dnode, err := dservOffline.Get(ctx, c)
		if err != nil {
			return err
		}

		if cctx.Bool(flagCar.Name) {
			// Write file as car file
			file, err := os.Create(outputPath + ".car")
			if err != nil {
				return err
			}
			_ = car.WriteCar(ctx, dservOffline, []cid.Cid{c}, file)

			fmt.Println("Saved .car output to", outputPath+".car")
		} else {
			// Otherwise write file as UnixFS File
			ufsFile, err := unixfile.NewUnixfsFile(ctx, dservOffline, dnode)
			if err != nil {
				return err
			}

			if err := files.WriteTo(ufsFile, outputPath); err != nil {
				return err
			}

			fmt.Println("Saved output to", outputPath)
		}

		return nil
	},
}

type RetrievalStats interface {
	GetByteSize() uint64
	GetDuration() time.Duration
	GetAverageBytesPerSecond() uint64
}

type FILRetrievalStats struct {
	RStats rc.RetrievalStats
}

func (stats *FILRetrievalStats) GetByteSize() uint64 {
	return stats.RStats.Size
}

func (stats *FILRetrievalStats) GetDuration() time.Duration {
	return stats.RStats.Duration
}

func (stats *FILRetrievalStats) GetAverageBytesPerSecond() uint64 {
	return stats.RStats.AverageSpeed
}

func printProgress(bytesReceived uint64) {
	str := fmt.Sprintf("%v (%v)", bytesReceived, humanize.IBytes(bytesReceived))

	termWidth, _, err := term.GetSize(int(os.Stdin.Fd()))
	strLen := len(str)
	if err == nil {

		if strLen < termWidth {
			// If the string is shorter than the terminal width, pad right side
			// with spaces to remove old text
			str = strings.Join([]string{str, strings.Repeat(" ", termWidth-strLen)}, "")
		} else if strLen > termWidth {
			// If the string doesn't fit in the terminal, cut it down to a size
			// that fits
			str = str[:termWidth]
		}
	}

	fmt.Fprintf(os.Stderr, "%s\r", str)
}

func printRetrievalStats(stats RetrievalStats) {
	switch stats := stats.(type) {
	case *FILRetrievalStats:
		fmt.Printf(`RETRIEVAL STATS (FIL)
-----
Size:          %v (%v)
Duration:      %v
Average Speed: %v (%v/s)
Ask Price:     %v (%v)
Total Payment: %v (%v)
Num Payments:  %v
Peer:          %v
`,
			stats.RStats.Size, humanize.IBytes(stats.RStats.Size),
			stats.RStats.Duration,
			stats.RStats.AverageSpeed, humanize.IBytes(stats.RStats.AverageSpeed),
			stats.RStats.AskPrice, types.FIL(stats.RStats.AskPrice),
			stats.RStats.TotalPayment, types.FIL(stats.RStats.TotalPayment),
			stats.RStats.NumPayments,
			stats.RStats.Peer,
		)
	}
}
