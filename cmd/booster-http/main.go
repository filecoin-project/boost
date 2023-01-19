package main

import (
	"os"

	"github.com/filecoin-project/boost/build"
	"github.com/filecoin-project/boostd-data/shared/cliutil"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("booster")

func main() {
	app := &cli.App{
		Name:                 "booster-http",
		Usage:                "HTTP endpoint for retrieval from Filecoin",
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
	_ = logging.SetLogLevel("booster", "INFO")

	if cliutil.IsVeryVerbose {
		_ = logging.SetLogLevel("booster", "DEBUG")
	}

	return nil
}
