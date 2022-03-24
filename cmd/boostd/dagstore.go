package main

import (
	"fmt"
	"os"

	"github.com/fatih/color"
	bapi "github.com/filecoin-project/boost/api"
	bcli "github.com/filecoin-project/boost/cli"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/urfave/cli/v2"
)

var dagstoreCmd = &cli.Command{
	Name:  "dagstore",
	Usage: "Manage the dagstore on the markets subsystem",
	Subcommands: []*cli.Command{
		dagstoreInitializeShardCmd,
		dagstoreInitializeAllCmd,
	},
}

var dagstoreInitializeShardCmd = &cli.Command{
	Name:      "initialize-shard",
	ArgsUsage: "[key]",
	Usage:     "Initialize the specified shard",
	Flags:     []cli.Flag{},
	Action: func(cctx *cli.Context) error {

		if cctx.NArg() != 1 {
			return fmt.Errorf("must provide a single shard key")
		}

		marketsApi, closer, err := lcli.GetMarketsAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		return marketsApi.DagstoreInitializeShard(ctx, cctx.Args().First())
	},
}

var dagstoreInitializeAllCmd = &cli.Command{
	Name:  "initialize-all",
	Usage: "Initialize all uninitialized shards, streaming results as they're produced; only shards for unsealed pieces are initialized by default",
	Flags: []cli.Flag{
		&cli.UintFlag{
			Name:     "concurrency",
			Usage:    "maximum shards to initialize concurrently at a time; use 0 for unlimited",
			Required: true,
		},
		&cli.BoolFlag{
			Name:  "include-sealed",
			Usage: "initialize sealed pieces as well",
		},
	},
	Action: func(cctx *cli.Context) error {
		concurrency := cctx.Uint("concurrency")
		sealed := cctx.Bool("sealed")

		ctx := lcli.ReqContext(cctx)

		// announce markets and boost deals
		napi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		params := bapi.DagstoreInitializeAllParams{
			MaxConcurrency: int(concurrency),
			IncludeSealed:  sealed,
		}

		ch, err := napi.DagstoreInitializeAll(ctx, params)
		if err != nil {
			return err
		}

		for {
			select {
			case evt, ok := <-ch:
				if !ok {
					return nil
				}
				_, _ = fmt.Fprint(os.Stdout, color.New(color.BgHiBlack).Sprintf("(%d/%d)", evt.Current, evt.Total))
				_, _ = fmt.Fprint(os.Stdout, " ")
				if evt.Event == "start" {
					_, _ = fmt.Fprintln(os.Stdout, evt.Key, color.New(color.Reset).Sprint("STARTING"))
				} else {
					if evt.Success {
						_, _ = fmt.Fprintln(os.Stdout, evt.Key, color.New(color.FgGreen).Sprint("SUCCESS"))
					} else {
						_, _ = fmt.Fprintln(os.Stdout, evt.Key, color.New(color.FgRed).Sprint("ERROR"), evt.Error)
					}
				}

			case <-ctx.Done():
				return fmt.Errorf("aborted")
			}
		}
	},
}
