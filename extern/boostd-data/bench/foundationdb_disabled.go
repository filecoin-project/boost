//go:build nofoundationdb

package main

import (
	"fmt"
	"github.com/urfave/cli/v2"
)

var foundationCmd = &cli.Command{
	Name:   "foundation",
	Before: before,
	Flags:  commonFlags,
	Action: func(cctx *cli.Context) error {
		return fmt.Errorf("foundationdb not supported in this build")
	},
}
