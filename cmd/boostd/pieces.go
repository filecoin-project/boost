package main

import (
	"fmt"
	"os"
	"text/tabwriter"

	bcli "github.com/filecoin-project/boost/cli"
	"github.com/filecoin-project/boost/cmd"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/lib/tablewriter"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
)

var piecesCmd = &cli.Command{
	Name:        "pieces",
	Usage:       "Interact with the Piece Store",
	Description: "The piecestore is a database that tracks and manages data that is made available to the retrieval market",
	Subcommands: []*cli.Command{
		piecesListPiecesCmd,
		piecesListCidInfosCmd,
		piecesInfoCmd,
		piecesCidInfoCmd,
	},
}

var piecesListPiecesCmd = &cli.Command{
	Name:  "list-pieces",
	Usage: "List registered pieces",
	Action: func(cctx *cli.Context) error {
		nodeApi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := lcli.ReqContext(cctx)

		pieceCids, err := nodeApi.PiecesListPieces(ctx)
		if err != nil {
			return err
		}

		if cctx.Bool("json") {

			pieceCidsJson := map[string]interface{}{
				"pieceCids": pieceCids,
			}
			return cmd.PrintJson(pieceCidsJson)
		}

		for _, pc := range pieceCids {
			fmt.Println(pc)
		}

		return nil
	},
}

var piecesListCidInfosCmd = &cli.Command{
	Name:  "list-cids",
	Usage: "List registered payload CIDs",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:    "verbose",
			Aliases: []string{"v"},
		},
	},
	Action: func(cctx *cli.Context) error {
		nodeApi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := lcli.ReqContext(cctx)

		cids, err := nodeApi.PiecesListCidInfos(ctx)
		if err != nil {
			return err
		}

		if cctx.Bool("json") && !cctx.Bool("verbose") {
			dataCidsJson := map[string]interface{}{
				"dataCids": cids,
			}

			return cmd.PrintJson(dataCidsJson)
		}

		w := tablewriter.New(tablewriter.Col("CID"),
			tablewriter.Col("Piece"),
			tablewriter.Col("BlockOffset"),
			tablewriter.Col("BlockLen"),
			tablewriter.Col("Deal"),
			tablewriter.Col("Sector"),
			tablewriter.Col("DealOffset"),
			tablewriter.Col("DealLen"),
		)

		type dataCids map[string]interface{}
		var out []dataCids

		for _, c := range cids {
			if !cctx.Bool("verbose") {
				fmt.Println(c)
				continue
			}

			type pbl map[string]interface{}
			var pbls []pbl

			ci, err := nodeApi.PiecesGetCIDInfo(ctx, c)
			if err != nil {
				fmt.Printf("Error getting CID info: %s\n", err)
				continue
			}

			for _, location := range ci.PieceBlockLocations {
				pi, err := nodeApi.PiecesGetPieceInfo(ctx, location.PieceCID)
				if err != nil {
					fmt.Printf("Error getting piece info: %s\n", err)
					continue
				}

				for _, deal := range pi.Deals {
					w.Write(map[string]interface{}{
						"CID":         c,
						"Piece":       location.PieceCID,
						"BlockOffset": location.RelOffset,
						"BlockLen":    location.BlockSize,
						"Deal":        deal.DealID,
						"Sector":      deal.SectorID,
						"DealOffset":  deal.Offset,
						"DealLen":     deal.Length,
					})

					deal := map[string]interface{}{
						"ID":     deal.DealID,
						"Sector": deal.SectorID,
						"Offset": deal.Offset,
						"Length": deal.Length,
					}
					tpbl := pbl{
						"PieceCid":    pi.PieceCID,
						"BlockOffset": location.RelOffset,
						"BlockLength": location.BlockSize,
						"Deal":        deal,
					}

					pbls = append(pbls, tpbl)
				}
			}

			tdataCids := dataCids{
				"DataCid":            c,
				"PieceBlockLocation": pbls,
			}

			out = append(out, tdataCids)
		}

		if cctx.Bool("json") && cctx.Bool("verbose") {
			return cmd.PrintJson(out)
		}

		if cctx.Bool("verbose") {
			return w.Flush(os.Stdout)
		}

		return nil
	},
}

var piecesInfoCmd = &cli.Command{
	Name:  "piece-info",
	Usage: "Get registered information for a given piece CID",
	Action: func(cctx *cli.Context) error {
		if !cctx.Args().Present() {
			return lcli.ShowHelp(cctx, fmt.Errorf("must specify piece cid"))
		}

		nodeApi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := lcli.ReqContext(cctx)

		c, err := cid.Decode(cctx.Args().First())
		if err != nil {
			return err
		}

		pi, err := nodeApi.PiecesGetPieceInfo(ctx, c)
		if err != nil {
			return err
		}

		if cctx.Bool("json") {

			type deal map[string]interface{}
			var deals []deal
			for _, d := range pi.Deals {
				dl := deal{
					"ID":       d.DealID,
					"SectorID": d.SectorID,
					"Offset":   d.Offset,
					"Length":   d.Length,
				}
				deals = append(deals, dl)
			}

			pieceinfo := map[string]interface{}{
				"PieceCid": pi.PieceCID,
				"Deals":    deals,
			}

			return cmd.PrintJson(pieceinfo)
		}

		fmt.Println("Piece: ", pi.PieceCID)
		w := tabwriter.NewWriter(os.Stdout, 4, 4, 2, ' ', 0)
		fmt.Fprintln(w, "Deals:\nDealID\tSectorID\tLength\tOffset")
		for _, d := range pi.Deals {
			fmt.Fprintf(w, "%d\t%d\t%d\t%d\n", d.DealID, d.SectorID, d.Length, d.Offset)
		}
		return w.Flush()
	},
}

var piecesCidInfoCmd = &cli.Command{
	Name:  "cid-info",
	Usage: "Get registered information for a given payload CID",
	Action: func(cctx *cli.Context) error {
		if !cctx.Args().Present() {
			return lcli.ShowHelp(cctx, fmt.Errorf("must specify payload cid"))
		}

		nodeApi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := lcli.ReqContext(cctx)

		c, err := cid.Decode(cctx.Args().First())
		if err != nil {
			return err
		}

		ci, err := nodeApi.PiecesGetCIDInfo(ctx, c)
		if err != nil {
			return err
		}

		if cctx.Bool("json") {

			type pbl map[string]interface{}
			type bl map[string]interface{}
			var pbls []pbl
			for _, d := range ci.PieceBlockLocations {
				tbp := bl{
					"RelOffset": d.RelOffset,
					"BlockSize": d.BlockSize,
				}
				tpbl := pbl{
					"PieceCid":      d.PieceCID,
					"BlockLocation": tbp,
				}
				pbls = append(pbls, tpbl)
			}

			pieceinfo := map[string]interface{}{
				"DataCid":            ci.CID,
				"PieceBlockLocation": pbls,
			}

			return cmd.PrintJson(pieceinfo)
		}

		fmt.Println("Info for: ", ci.CID)

		w := tabwriter.NewWriter(os.Stdout, 4, 4, 2, ' ', 0)
		fmt.Fprintf(w, "PieceCid\tOffset\tSize\n")
		for _, loc := range ci.PieceBlockLocations {
			fmt.Fprintf(w, "%s\t%d\t%d\n", loc.PieceCID, loc.RelOffset, loc.BlockSize)
		}
		return w.Flush()
	},
}
