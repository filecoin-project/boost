package main

import (
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	clinode "github.com/filecoin-project/boost/cli/node"
	"github.com/filecoin-project/boost/cmd"
	"github.com/filecoin-project/boost/markets/utils"
	"github.com/filecoin-project/boost/retrieve"
	"github.com/filecoin-project/boostd-data/shared/cliutil"

	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"

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
	"github.com/urfave/cli/v2"
	"golang.org/x/term"
	"golang.org/x/xerrors"
)

var flagMiner = &cli.StringFlag{
	Name:     "miner",
	Aliases:  []string{"m"},
	Required: true,
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
	ArgsUsage:   "<cid>",
	Flags: []cli.Flag{
		flagMiner,
		flagOutput,
		flagDmPathSel,
		flagCar,
	},
	Action: func(cctx *cli.Context) error {
		ctx := cliutil.ReqContext(cctx)

		cfgdir := cctx.String(cmd.FlagRepo.Name)
		node, err := clinode.Setup(cfgdir)
		if err != nil {
			return fmt.Errorf("setting up CLI node: %w", err)
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

		fc, err := retrieve.NewClient(node.Host, api, node.Wallet, addr, node.Blockstore, node.Datastore, cfgdir)
		if err != nil {
			return err
		}

		cidStr := cctx.Args().First()
		if cidStr == "" {
			return fmt.Errorf("please specify a CID to retrieve")
		}
		c, err := cid.Decode(cidStr)
		if err != nil {
			return err
		}

		dmSelText := textselector.Expression(cctx.String(flagDmPathSel.Name))

		miner, err := address.NewFromString(cctx.String("miner"))
		if err != nil {
			return fmt.Errorf("failed to parse miner %s: %w", cctx.String("miner"), err)
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

		query, err := fc.RetrievalQuery(ctx, miner, c)
		if err != nil {
			return fmt.Errorf("retrieval query for miner %s failed: %v", miner, err)
		}

		proposal, err := retrieve.RetrievalProposalForAsk(query, c, selNode)
		if err != nil {
			return fmt.Errorf("Failed to create retrieval proposal with candidate miner %s: %v", miner, err)
		}

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

		printRetrievalStats(&FILRetrievalStats{RetrievalStats: *stats})

		dservOffline := merkledag.NewDAGService(blockservice.New(node.Blockstore, offline.Exchange(node.Blockstore)))

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
			file, err := os.Create(output + ".car")
			if err != nil {
				return err
			}
			_ = car.WriteCar(ctx, dservOffline, []cid.Cid{c}, file)

			fmt.Println("Saved .car output to", output+".car")
		} else {
			// Otherwise write file as UnixFS File
			ufsFile, err := unixfile.NewUnixfsFile(ctx, dservOffline, dnode)
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

type RetrievalStats interface {
	GetByteSize() uint64
	GetDuration() time.Duration
	GetAverageBytesPerSecond() uint64
}

type FILRetrievalStats struct {
	retrieve.RetrievalStats
}

func (stats *FILRetrievalStats) GetByteSize() uint64 {
	return stats.Size
}

func (stats *FILRetrievalStats) GetDuration() time.Duration {
	return stats.Duration
}

func (stats *FILRetrievalStats) GetAverageBytesPerSecond() uint64 {
	return stats.AverageSpeed
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
			stats.Size, humanize.IBytes(stats.Size),
			stats.Duration,
			stats.AverageSpeed, humanize.IBytes(stats.AverageSpeed),
			stats.AskPrice, types.FIL(stats.AskPrice),
			stats.TotalPayment, types.FIL(stats.TotalPayment),
			stats.NumPayments,
			stats.Peer,
		)
	}
}
