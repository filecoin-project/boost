package main

import (
	"os"

	"github.com/filecoin-project/boost/build"
	"github.com/filecoin-project/boost/extern/boostd-data/shared/cliutil"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("booster")

var FlagRepo = &cli.StringFlag{
	Name:    "repo",
	Usage:   "repo directory for booster-http",
	Value:   "~/.booster-http",
	EnvVars: []string{"BOOSTER_HTTP_REPO"},
}

var app = &cli.App{
	Name:                 "booster-http",
	Usage:                "HTTP endpoint for retrieval from Filecoin",
	EnableBashCompletion: true,
	Version:              build.UserVersion(),
	Flags: []cli.Flag{
		cliutil.FlagVeryVerbose,
		FlagRepo,
	},
	Commands: []*cli.Command{
		runCmd,
	},
}

func main() {
	app.Setup()

	if err := app.Run(os.Args); err != nil {
		_, _ = os.Stderr.WriteString("Error: " + err.Error() + "\n")
	}
}

func before(cctx *cli.Context) error {
	_ = logging.SetLogLevel("booster", "INFO")
	_ = logging.SetLogLevel("remote-blockstore", "INFO")

	if cliutil.IsVeryVerbose {
		_ = logging.SetLogLevel("booster", "DEBUG")
		_ = logging.SetLogLevel("remote-blockstore", "DEBUG")
		_ = logging.SetLogLevel("piecedirectory", "DEBUG")
	}

	return nil
}
