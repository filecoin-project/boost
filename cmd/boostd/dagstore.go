package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/filecoin-project/lotus/lib/tablewriter"
	"github.com/ipfs/go-cid"

	"github.com/fatih/color"
	bapi "github.com/filecoin-project/boost/api"
	bcli "github.com/filecoin-project/boost/cli"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/urfave/cli/v2"
)

var dagstoreCmd = &cli.Command{
	Name:  "dagstore",
	Usage: "Manage the dagstore on the Boost subsystem",
	Subcommands: []*cli.Command{
		dagstoreRegisterShardCmd,
		dagstoreInitializeShardCmd,
		dagstoreRecoverShardCmd,
		dagstoreInitializeAllCmd,
		dagstoreListShardsCmd,
		dagstoreGcCmd,
		dagstoreDestroyShardCmd,
		dagstoreLookupCmd,
	},
}

var dagstoreGcCmd = &cli.Command{
	Name:  "gc",
	Usage: "Garbage collect the dagstore",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)
		napi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		collected, err := napi.BoostDagstoreGC(ctx)
		if err != nil {
			return err
		}

		if len(collected) == 0 {
			_, _ = fmt.Fprintln(os.Stdout, "no shards collected")
			return nil
		}

		for _, e := range collected {
			if e.Error == "" {
				_, _ = fmt.Fprintln(os.Stdout, e.Key, color.New(color.FgGreen).Sprint("SUCCESS"))
			} else {
				_, _ = fmt.Fprintln(os.Stdout, e.Key, color.New(color.FgRed).Sprint("ERROR"), e.Error)
			}
		}

		return nil
	},
}

var dagstoreListShardsCmd = &cli.Command{
	Name:  "list-shards",
	Usage: "List all shards known to the dagstore, with their current status",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)
		napi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		shards, err := napi.BoostDagstoreListShards(ctx)
		if err != nil {
			return err
		}

		return printTableShards(shards)
	},
}

func printTableShards(shards []bapi.DagstoreShardInfo) error {
	if len(shards) == 0 {
		return nil
	}

	tw := tablewriter.New(
		tablewriter.Col("Key"),
		tablewriter.Col("State"),
		tablewriter.Col("Error"),
	)

	colors := map[string]color.Attribute{
		"ShardStateAvailable": color.FgGreen,
		"ShardStateServing":   color.FgBlue,
		"ShardStateErrored":   color.FgRed,
		"ShardStateNew":       color.FgYellow,
	}

	for _, s := range shards {
		m := map[string]interface{}{
			"Key": s.Key,
			"State": func() string {
				trimmedState := strings.TrimPrefix(s.State, "ShardState")
				if c, ok := colors[s.State]; ok {
					return color.New(c).Sprint(trimmedState)
				}
				return trimmedState
			}(),
			"Error": s.Error,
		}
		tw.Write(m)
	}
	return tw.Flush(os.Stdout)
}

var dagstoreRegisterShardCmd = &cli.Command{
	Name:      "register-shard",
	ArgsUsage: "[key]",
	Usage:     "Register a shard",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:        "color",
			Usage:       "use color in display output",
			DefaultText: "depends on output being a TTY",
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.IsSet("color") {
			color.NoColor = !cctx.Bool("color")
		}

		if cctx.NArg() != 1 {
			return fmt.Errorf("must provide a single shard key")
		}

		napi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		shardKey := cctx.Args().First()
		err = napi.BoostDagstoreRegisterShard(ctx, shardKey)
		if err != nil {
			return err
		}

		fmt.Println("Registered shard " + shardKey)
		return nil
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

		ctx := lcli.ReqContext(cctx)
		napi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		return napi.BoostDagstoreInitializeShard(ctx, cctx.Args().First())
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
		sealed := cctx.Bool("include-sealed")

		ctx := lcli.ReqContext(cctx)

		napi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		params := bapi.DagstoreInitializeAllParams{
			MaxConcurrency: int(concurrency),
			IncludeSealed:  sealed,
		}

		ch, err := napi.BoostDagstoreInitializeAll(ctx, params)
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

var dagstoreRecoverShardCmd = &cli.Command{
	Name:      "recover-shard",
	ArgsUsage: "[key]",
	Usage:     "Attempt to recover a shard in errored state",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:        "color",
			Usage:       "use color in display output",
			DefaultText: "depends on output being a TTY",
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.IsSet("color") {
			color.NoColor = !cctx.Bool("color")
		}

		if cctx.NArg() != 1 {
			return fmt.Errorf("must provide a single shard key")
		}

		napi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		return napi.BoostDagstoreRecoverShard(ctx, cctx.Args().First())
	},
}

var dagstoreDestroyShardCmd = &cli.Command{
	Name:      "destroy-shard",
	ArgsUsage: "[key]",
	Usage:     "Destroy a shard",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:        "color",
			Usage:       "use color in display output",
			DefaultText: "depends on output being a TTY",
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.IsSet("color") {
			color.NoColor = !cctx.Bool("color")
		}

		if cctx.NArg() != 1 {
			return fmt.Errorf("must provide a single shard key")
		}

		napi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		shardKey := cctx.Args().First()
		err = napi.BoostDagstoreDestroyShard(ctx, shardKey)
		if err != nil {
			return err
		}

		fmt.Println("Destroyed shard " + shardKey)
		return nil
	},
}

var dagstoreLookupCmd = &cli.Command{
	Name:      "lookup-piece-cid",
	ArgsUsage: "[key]",
	Usage:     "Performs a reverse lookup with payload CID to get Piece CID",
	Aliases:   []string{"lpc"},
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:        "color",
			Usage:       "use color in display output",
			DefaultText: "depends on output being a TTY",
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.IsSet("color") {
			color.NoColor = !cctx.Bool("color")
		}

		if cctx.NArg() != 1 {
			return fmt.Errorf("must provide a single payload CID")
		}

		napi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		shardKey := cctx.Args().First()
		payloadCid, err := cid.Parse(shardKey)
		if err != nil {
			return fmt.Errorf("Unable to parse the provided CID: %s", shardKey)
		}
		pieceCid, err := napi.BoostDagstorePiecesContainingMultihash(ctx, payloadCid.Hash())
		if err != nil {
			return err
		}

		fmt.Printf("Given CID was found in the following pieces: %s", pieceCid)
		return nil
	},
}
