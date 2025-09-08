package main

import (
	"fmt"

	"github.com/docker/go-units"
	"github.com/filecoin-project/boost/extern/boostd-data/shared/cliutil"
	"github.com/filecoin-project/go-paramfetch"
	"github.com/filecoin-project/lotus/build"
	"github.com/urfave/cli/v2"
)

var fetchParamCmd = &cli.Command{
	Name:      "fetch-params",
	Usage:     "Fetch proving parameters",
	ArgsUsage: "[sectorSize]",
	Action: func(cctx *cli.Context) error {
		ctx := cliutil.ReqContext(cctx)

		if !cctx.Args().Present() {
			return fmt.Errorf("must pass sector size to fetch params for (specify as \"32GiB\", for instance)")
		}
		sectorSizeInt, err := units.RAMInBytes(cctx.Args().First())
		if err != nil {
			return fmt.Errorf("error parsing sector size (specify as \"32GiB\", for instance): %w", err)
		}
		sectorSize := uint64(sectorSizeInt)

		err = paramfetch.GetParams(ctx, build.ParametersJSON(), build.SrsJSON(), sectorSize)
		if err != nil {
			return fmt.Errorf("fetching proof parameters: %w", err)
		}

		return nil
	},
}
