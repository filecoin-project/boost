package main

import (
	"os"

	"github.com/filecoin-project/boost/build"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("migrate")

const (
	FlagBoostRepo = "boost-repo"
)

var FlagRepo = &cli.StringFlag{
	Name:    FlagBoostRepo,
	EnvVars: []string{"BOOST_PATH"},
	Usage:   "boost repo path",
	Value:   "~/.boost",
}

var IsVeryVerbose bool

var FlagVeryVerbose = &cli.BoolFlag{
	Name:        "vv",
	Usage:       "enables very verbose mode, useful for debugging the CLI",
	Destination: &IsVeryVerbose,
}

func main() {
	app := &cli.App{
		Name:                 "migrate-lid",
		Usage:                "Migrate boost to Local Index Directory",
		EnableBashCompletion: true,
		Version:              build.UserVersion(),
		Flags: []cli.Flag{
			FlagRepo,
			FlagVeryVerbose,
		},
		Commands: []*cli.Command{
			migrateLevelDBCmd,
			migrateYugabyteDBCmd,
			migrateReverseCmd,
		},
	}
	app.Setup()

	if err := app.Run(os.Args); err != nil {
		_, _ = os.Stderr.WriteString("Error: " + err.Error() + "\n")
	}
}

func before(cctx *cli.Context) error {
	_ = logging.SetLogLevel("migrate", "INFO")

	if IsVeryVerbose {
		_ = logging.SetLogLevel("migrate", "DEBUG")
	}

	return nil
}
