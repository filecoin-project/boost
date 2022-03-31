package main

import (
	"fmt"
	"sort"
	"strings"

	bcli "github.com/filecoin-project/boost/cli"
	"github.com/urfave/cli/v2"
)

var logCmd = &cli.Command{
	Name:  "log",
	Usage: "Show and set log levels",
	Subcommands: []*cli.Command{
		logListCmd,
		logSetLevelCmd,
	},
}

var logListCmd = &cli.Command{
	Name:  "list",
	Usage: "List all available log subsystems",
	Action: func(cctx *cli.Context) error {
		ctx := bcli.ReqContext(cctx)

		boostApi, ncloser, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return fmt.Errorf("getting boost api: %w", err)
		}
		defer ncloser()

		logs, err := boostApi.LogList(ctx)
		if err != nil {
			return fmt.Errorf("getting boost log list: %w", err)
		}

		sort.Slice(logs, func(i, j int) bool {
			return logs[i] < logs[j]
		})

		for _, l := range logs {
			fmt.Println(l)
		}

		return nil
	},
}

var logSetLevelCmd = &cli.Command{
	Name:  "set-level",
	Usage: "Set the level of all logs or particular logs",
	Description: `
boost log set-level level
For example:
$ boost log set-level info

boost log set-level subsystem=level [subsystem=level]...
For example:
$ boost log set-level provider=debug

Note that levels are applied in order, and * is used to set all logs.
So for example to set all logs to info, provider to warn and gql to debug:
$ boost log set-level *=info provider=warn gql=debug`,
	Action: func(cctx *cli.Context) error {
		if !cctx.Args().Present() {
			return fmt.Errorf("must specify level or subsystem=level, eg boost log set-level boost=debug")
		}

		ctx := bcli.ReqContext(cctx)

		boostApi, ncloser, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return fmt.Errorf("getting boost api: %w", err)
		}
		defer ncloser()

		// If there's only one argument, assume the user wants to set all logs to
		// the same level
		args := cctx.Args().Slice()
		if len(args) == 1 && !strings.Contains(args[0], "=") {
			level := args[0]
			subsystem := "*"
			err = boostApi.LogSetLevel(ctx, subsystem, level)
			if err != nil {
				return fmt.Errorf("setting subsystem %s to level %s: %w", subsystem, level, err)
			}

			return nil
		}

		// Split each subsystem=level argument and apply it
		for _, arg := range args {
			parts := strings.Split(arg, "=")
			if len(parts) != 2 {
				return fmt.Errorf("invalid format '%s': expected subsystem=level", arg)
			}
			subsystem := parts[0]
			level := parts[1]

			err = boostApi.LogSetLevel(ctx, subsystem, level)
			if err != nil {
				return fmt.Errorf("setting subsystem %s to level %s: %w", subsystem, level, err)
			}
		}

		return nil
	},
}
