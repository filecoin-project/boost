package main

import (
	"io"
	llog "log"
	"os"

	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"

	"github.com/filecoin-project/boost/build"
	"github.com/filecoin-project/boost/cmd"
	"github.com/filecoin-project/boost/extern/boostd-data/shared/cliutil"
)

const (
	FlagBoostRepo = "boost-repo"
)

var log = logging.Logger("boostx")

func init() {
	llog.SetOutput(io.Discard)
}

func main() {
	app := &cli.App{
		Name:                 "boostx",
		Usage:                "Various experimental utilities for Boost",
		EnableBashCompletion: true,
		Version:              build.UserVersion(),
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    FlagBoostRepo,
				EnvVars: []string{"BOOST_PATH"},
				Usage:   "boostd repo path",
				Value:   "~/.boost",
			},
			cliutil.FlagVeryVerbose,
			cmd.FlagRepo,
		},
		Commands: []*cli.Command{
			commpCmd,
			generatecarCmd,
			generateRandCar,
			marketAddCmd,
			marketWithdrawCmd,
			statsCmd,
			sectorCmd,
			boostdCmd,
		},
	}
	app.Setup()

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func before(cctx *cli.Context) error {
	_ = logging.SetLogLevel("boostx", "INFO")

	if cliutil.IsVeryVerbose {
		_ = logging.SetLogLevel("boostx", "DEBUG")
	}

	return nil
}
