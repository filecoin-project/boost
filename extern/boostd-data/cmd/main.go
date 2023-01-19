package main

import (
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"os"
)

var log = logging.Logger("boostd-data")

// This service exposes an RPC API that is called by boostd in order to manage
// boostd's data. The service provides different implementations of the data
// interface according to the technologies that a user chooses (eg leveldb vs
// couchbase)
func main() {
	app := &cli.App{
		Name:                 "boostd-data",
		Usage:                "Service that implements boostd data API",
		EnableBashCompletion: true,
		Flags: []cli.Flag{
			FlagVeryVerbose,
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

	if IsVeryVerbose {
		_ = logging.SetLogLevel("boostd-data", "DEBUG")
	}

	return nil
}
