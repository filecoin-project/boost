package main

import (
	"fmt"

	bcli "github.com/filecoin-project/boost/cli"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
)

var storageDealsCmd = &cli.Command{
	Name:     "storage-deals",
	Usage:    "Manage legacy storage deals and related configuration (Markets V1)",
	Category: "legacy",
	Subcommands: []*cli.Command{
		dealsImportDataCmd,
	},
}

var dealsImportDataCmd = &cli.Command{
	Name:      "import-data",
	Usage:     "Manually import data for a deal (Markets V1)",
	ArgsUsage: "<proposal CID> <file>",
	Action: func(cctx *cli.Context) error {
		api, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := bcli.ReqContext(cctx)

		if cctx.Args().Len() < 2 {
			return fmt.Errorf("must specify proposal CID and file path")
		}

		propCid, err := cid.Decode(cctx.Args().Get(0))
		if err != nil {
			return err
		}

		fpath := cctx.Args().Get(1)

		return api.MarketImportDealData(ctx, propCid, fpath)
	},
}
