package main

import (
	"os"

	"github.com/filecoin-project/boost/build"
	"github.com/filecoin-project/boost/cmd"
	"github.com/filecoin-project/boost/extern/boostd-data/shared/cliutil"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
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
			backupCmd,
			restoreCmd,
			configCmd,
			dummydealCmd,
			indexProvCmd,
			importDataCmd,
			importDirectDataCmd,
			logCmd,
			netCmd,
			pieceDirCmd,
		},
	}
	app.Setup()

	if err := app.Run(os.Args); err != nil {
		_, _ = os.Stderr.WriteString("Error: " + err.Error() + "\n")
	}
}

func before(cctx *cli.Context) error {
	_ = logging.SetLogLevel("boostd", "INFO")
	_ = logging.SetLogLevel("db", "INFO")
	_ = logging.SetLogLevel("boost-prop", "INFO")
	_ = logging.SetLogLevel("modules", "INFO")
	_ = logging.SetLogLevel("cfg", "INFO")
	_ = logging.SetLogLevel("boost-storage-deal", "INFO")
	_ = logging.SetLogLevel("piecedir", "INFO")
	_ = logging.SetLogLevel("index-provider-wrapper", "INFO")
	_ = logging.SetLogLevel("unsmgr", "INFO")
	_ = logging.SetLogLevel("piecedoc", "INFO")
	_ = logging.SetLogLevel("piecedirectory", "INFO")
	_ = logging.SetLogLevel("sectorstatemgr", "INFO")
	_ = logging.SetLogLevel("migrations", "INFO")
	_ = logging.SetLogLevel("rpc", "ERROR")

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
		_ = logging.SetLogLevel("piecedir", "DEBUG")
		_ = logging.SetLogLevel("fxlog", "DEBUG")
		_ = logging.SetLogLevel("unsmgr", "DEBUG")
		_ = logging.SetLogLevel("piecedoc", "DEBUG")
		_ = logging.SetLogLevel("piecedirectory", "DEBUG")
		_ = logging.SetLogLevel("sectorstatemgr", "DEBUG")
	}

	return nil
}
