package main

import (
	"fmt"

	"github.com/urfave/cli/v2"
)

var cleanupLevelDBCmd = &cli.Command{
	Name:        "leveldb",
	Description: "Removes the indexes and other metadata leveldb based LID store",
	Usage:       "migrate-curio cleanup leveldb",
	Before:      before,
	Action: func(cctx *cli.Context) error {
		fmt.Println("Please remove the directory called 'LID' in the boost repo path to remove leveldb based LID")
		fmt.Println("This directory can also be present outside of Boost repo if 'boostd-data' was running with a custom repo path")
		return nil
	},
}
