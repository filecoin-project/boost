package main

import (
	"github.com/davecgh/go-spew/spew"
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

		log.Debug("Get boost identity")

		res, err := boostApi.ID(ctx)
		if err != nil {
			return xerrors.Errorf("couldnt get boost identity: %w", err)
		}

		spew.Dump(res)

		return nil
	},
}
