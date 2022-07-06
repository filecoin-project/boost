package main

import (
	"fmt"
	"os"
	"strings"

	bcli "github.com/filecoin-project/boost/cli"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/lib/tablewriter"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
)

var inspectCmd = &cli.Command{
	Name:  "inspect",
	Usage: "Inspect pieces and deals within Boost",
	Subcommands: []*cli.Command{
		inspectPieceCmd,
	},
}

var inspectPieceCmd = &cli.Command{
	Name:        "piece",
	ArgsUsage:   "[piece cid]",
	Description: "Get information about a piece",
	Flags:       []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		if cctx.NArg() != 1 {
			return fmt.Errorf("must provide the piece cid to lookup")
		}

		pieceCidStr := cctx.Args().First()
		pieceCid, err := cid.Parse(pieceCidStr)
		if err != nil {
			return fmt.Errorf("parsing piece cid '%s': %w", pieceCidStr, err)
		}

		ctx := lcli.ReqContext(cctx)
		bapi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		fmt.Printf("Piece %s:\n", pieceCid)
		pieceInfo, err := bapi.PiecesGetPieceInfo(ctx, pieceCid)
		if err != nil && isNotFoundErr(err) {
			err = fmt.Errorf("could not find piece '%s' in the piece store: %w", pieceCid, err)
		}
		if err != nil {
			fmt.Printf("Error getting piece info from Piece Store: %s\n", err)
		} else {
			// Write out all the deals with this piece
			fmt.Printf("Deals with this piece in Piece Store: %d\n\n", len(pieceInfo.Deals))
			tw := tablewriter.New(
				tablewriter.Col("Deal ID"),
				tablewriter.Col("Sector ID"),
				tablewriter.Col("Offset"),
				tablewriter.Col("Length"),
			)
			for _, dl := range pieceInfo.Deals {
				tw.Write(map[string]interface{}{
					"Deal ID":   dl.DealID,
					"Sector ID": dl.SectorID,
					"Offset":    dl.Length,
					"Length":    dl.Length,
				})
			}
			tw.Flush(os.Stdout) //nolint:errcheck
		}
		return nil
	},
}

// isNotFoundErr just checks if the error message contains the words "not found"
// Unfortunately we can't use errors.Is() because the error loses its type when
// it crosses the RPC boundary
func isNotFoundErr(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "not found")
}
