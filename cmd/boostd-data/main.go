package main

import (
	"github.com/filecoin-project/boost/build"
	cliutil "github.com/filecoin-project/boost/cli/util"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"os"
)

var log = logging.Logger("boostd-data")

func main() {
	app := &cli.App{
		Name:                 "boostd-data",
		Usage:                "Service that implements the boost indexing data API",
		EnableBashCompletion: true,
		Version:              build.UserVersion(),
		Flags: []cli.Flag{
			cliutil.FlagVeryVerbose,
		},
		Commands: []*cli.Command{
			runCmd,
		},
	}
	app.Setup()

	if err := app.Run(os.Args); err != nil {
		os.Stderr.WriteString("Error: " + err.Error() + "\n")
	}
}

func before(cctx *cli.Context) error {
	_ = logging.SetLogLevel("boostd-data", "INFO")

	if cliutil.IsVeryVerbose {
		_ = logging.SetLogLevel("boostd-data", "DEBUG")
	}

	return nil
}
