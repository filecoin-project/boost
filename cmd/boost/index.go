package main

import (
	"fmt"

	bcli "github.com/filecoin-project/boost/cli"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/urfave/cli/v2"
)

var indexProvCmd = &cli.Command{
	Name:  "index",
	Usage: "Manage the index provider on Boost",
	Subcommands: []*cli.Command{
		indexProvAnnounceAllCmd,
	},
}

var indexProvAnnounceAllCmd = &cli.Command{
	Name:  "announce-all",
	Usage: "Announce all active deals to indexers so they can download the indices",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:        "color",
			Usage:       "use color in display output",
			DefaultText: "depends on output being a TTY",
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)

		// announce markets deals
		nodeApi, closer, err := lcli.GetMarketsAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		if err := nodeApi.IndexerAnnounceAllDeals(ctx); err != nil {
			fmt.Printf("some errors while announcing legacy markets deals: %s", err)
		}

		napi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		return napi.IndexerAnnounceAllDeals(ctx)
	},
}
