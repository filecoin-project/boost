package main

import (
	"os"

	"github.com/filecoin-project/boost/cmd"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"

	"github.com/filecoin-project/boost/build"
	"github.com/filecoin-project/boostd-data/shared/cliutil"
)

var log = logging.Logger("boostd")

const (
	FlagBoostRepo = "boost-repo"
)

func main() {
	app := &cli.App{
		Name:                 "boostd",
		Usage:                "Markets V2 module for Filecoin",
		EnableBashCompletion: true,
		Version:              build.UserVersion(),
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    FlagBoostRepo,
				EnvVars: []string{"BOOST_PATH"},
				Usage:   "boost repo path",
				Value:   "~/.boost",
			},
			cmd.FlagJson,
			cliutil.FlagVeryVerbose,
		},
		Commands: []*cli.Command{
			authCmd,
			runCmd,
			initCmd,
			migrateMonolithCmd,
			migrateMarketsCmd,
			backupCmd,
			restoreCmd,
			dummydealCmd,
			dataTransfersCmd,
			retrievalDealsCmd,
			indexProvCmd,
			importDataCmd,
			logCmd,
			dagstoreCmd,
			piecesCmd,
			netCmd,
		},
	}
	app.Setup()

	if err := app.Run(os.Args); err != nil {
		os.Stderr.WriteString("Error: " + err.Error() + "\n")
	}
}

func before(cctx *cli.Context) error {
	_ = logging.SetLogLevel("boostd", "INFO")
	_ = logging.SetLogLevel("db", "INFO")
	_ = logging.SetLogLevel("boost-prop", "INFO")
	_ = logging.SetLogLevel("modules", "INFO")
	_ = logging.SetLogLevel("cfg", "INFO")
	_ = logging.SetLogLevel("boost-storage-deal", "INFO")
	_ = logging.SetLogLevel("index-provider-wrapper", "INFO")
	_ = logging.SetLogLevel("unsmgr", "INFO")

	if cliutil.IsVeryVerbose {
		_ = logging.SetLogLevel("boostd", "DEBUG")
		_ = logging.SetLogLevel("provider", "DEBUG")
		_ = logging.SetLogLevel("boost-net", "DEBUG")
		_ = logging.SetLogLevel("boost-provider", "DEBUG")
		_ = logging.SetLogLevel("storagemanager", "DEBUG")
		_ = logging.SetLogLevel("index-provider-wrapper", "DEBUG")
		_ = logging.SetLogLevel("boost-migrator", "DEBUG")
		_ = logging.SetLogLevel("dagstore", "DEBUG")
		_ = logging.SetLogLevel("migrator", "DEBUG")
		_ = logging.SetLogLevel("unsmgr", "DEBUG")
	}

	return nil
}
