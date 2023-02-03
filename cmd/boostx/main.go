package main

import (
	"io/ioutil"
	"os"

	llog "log"

	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"

	"github.com/filecoin-project/boost/build"
	cliutil "github.com/filecoin-project/boost/cli/util"
	"github.com/filecoin-project/boost/cmd"
)

var log = logging.Logger("boostx")

func init() {
	llog.SetOutput(ioutil.Discard)
}

func main() {
	app := &cli.App{
		Name:                 "boostx",
		Usage:                "Various experimental utilities for Boost",
		EnableBashCompletion: true,
		Version:              build.UserVersion(),
		Flags: []cli.Flag{
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
