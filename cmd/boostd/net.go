package main

import (
	"github.com/filecoin-project/boost/node/repo"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/urfave/cli/v2"
)

var netCmd = &cli.Command{
	Name:  "net",
	Usage: "Manage P2P Network",
	Before: func(cctx *cli.Context) error {
		cctx.App.Metadata["repoType"] = repo.Boost
		return nil
	},
	Subcommands: lcli.NetCmd.Subcommands,
}
