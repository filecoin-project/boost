package main

import (
	"fmt"

	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	lcli "github.com/filecoin-project/boost/cli"
)

var dummydealCmd = &cli.Command{
	Name:   "dummydeal",
	Usage:  "Trigger a sample deal",
	Before: before,
	Action: func(cctx *cli.Context) error {
		boostApi, ncloser, err := lcli.GetBoostAPI(cctx)
		if err != nil {
			return xerrors.Errorf("getting boost api: %w", err)
		}
		defer ncloser()

		ctx := lcli.DaemonContext(cctx)

		log.Debug("Make API call to start dummy deal")
		rej, err := boostApi.MarketDummyDeal(ctx, nil)
		if err != nil {
			return xerrors.Errorf("creating dummy deal: %w", err)
		}

		if rej != nil && rej.Reason != "" {
			fmt.Printf("Dummy deal rejected: %s\n", rej.Reason)
			return nil
		}
		fmt.Println("Made dummy deal")

		return nil
	},
}
