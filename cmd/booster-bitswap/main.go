package main

import (
	"os"

	"github.com/filecoin-project/boost/build"
	cliutil "github.com/filecoin-project/boost/cli/util"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("booster")

var FlagRepo = &cli.StringFlag{
	Name:    "repo",
	Usage:   "repo directory for Booster bitswap",
	Value:   "~/.booster-bitswap",
	EnvVars: []string{"BOOSTER_BITSWAP_REPO"},
}

func main() {
	app := &cli.App{
		Name:                 "booster-bitswap",
		Usage:                "Bitswap endpoint for retrieval from Filecoin",
		EnableBashCompletion: true,
		Version:              build.UserVersion(),
		Flags: []cli.Flag{
			cliutil.FlagVeryVerbose,
			FlagRepo,
		},
		Commands: []*cli.Command{
			initCmd,
			runCmd,
			fetchCmd,
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
		_ = logging.SetLogLevel("remote-blockstore", "DEBUG")
	}

	return nil
}
