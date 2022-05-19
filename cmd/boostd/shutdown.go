package main

import (
	_ "net/http/pprof"

	"github.com/urfave/cli/v2"

	bcli "github.com/filecoin-project/boost/cli"
)

var stopCmd = &cli.Command{
	Name:  "stop",
	Usage: "Stop a running boostd process",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		api, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		err = api.Shutdown(bcli.ReqContext(cctx))
		if err != nil {
			return err
		}

		return nil
	},
}
