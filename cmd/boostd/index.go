package main

import (
	"fmt"
	bcli "github.com/filecoin-project/boost/cli"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
)

var indexProvCmd = &cli.Command{
	Name:  "index",
	Usage: "Manage the index provider on Boost",
	Subcommands: []*cli.Command{
		indexProvAnnounceAllCmd,
		indexProvListMultihashesCmd,
	},
}

var indexProvAnnounceAllCmd = &cli.Command{
	Name:  "announce-all",
	Usage: "Announce all active deals to indexers so they can download the indices",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)

		// get boost api
		napi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		// announce markets and boost deals
		return napi.BoostIndexerAnnounceAllDeals(ctx)
	},
}

var indexProvListMultihashesCmd = &cli.Command{
	Name:      "list-multihashes",
	Usage:     "list-multihashes <proposal cid>",
	UsageText: "List multihashes for a deal by proposal cid",
	Action: func(cctx *cli.Context) error {
		if cctx.NArg() != 1 {
			return fmt.Errorf("must supply proposal cid")
		}

		propCid, err := cid.Parse(cctx.Args().First())
		if err != nil {
			return fmt.Errorf("parsing proposal cid %s: %w", cctx.Args().First(), err)
		}

		ctx := lcli.ReqContext(cctx)

		// get boost api
		napi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		// get list of multihashes
		mhs, err := napi.BoostIndexerListMultihashes(ctx, propCid)
		if err != nil {
			return err
		}

		fmt.Printf("Found %d multihashes for deal with proposal cid %s:\n", len(mhs), propCid)
		for _, mh := range mhs {
			fmt.Println("  " + mh.String())
		}
		return nil
	},
}
