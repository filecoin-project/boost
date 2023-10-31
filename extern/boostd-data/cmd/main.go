package main

import (
	"os"

	"github.com/filecoin-project/boost/extern/boostd-data/build"
	"github.com/filecoin-project/boost/extern/boostd-data/shared/cliutil"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("boostd-data")

// This service exposes an RPC API that is called by boostd in order to manage
// boostd's data. The service provides different implementations of the data
// interface according to the technologies that a user chooses (eg leveldb vs
// yugabyte)
func main() {
	app := &cli.App{
		Name:                 "boostd-data",
		Usage:                "Service that implements boostd data API",
		Version:              build.UserVersion(),
		EnableBashCompletion: true,
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
	_ = logging.SetLogLevel("boostd-data-yb", "INFO")

	if cliutil.IsVeryVerbose {
		_ = logging.SetLogLevel("boostd-data", "DEBUG")
		_ = logging.SetLogLevel("boostd-data-ldb", "DEBUG")
		_ = logging.SetLogLevel("boostd-data-yb", "DEBUG")
	}

	return nil
}
