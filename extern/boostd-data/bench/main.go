package main

import (
	"github.com/filecoin-project/boostd-data/shared/cliutil"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"os"
)

var log = logging.Logger("bench")

func main() {
	app := &cli.App{
		Name:                 "bench",
		Usage:                "Benchmark LID databases",
		EnableBashCompletion: true,
		Flags: []cli.Flag{
			cliutil.FlagVeryVerbose,
		},
		Commands: []*cli.Command{
			cassandraCmd,
			foundationCmd,
			postgresCmd,
		},
	}
	app.Setup()

	if err := app.Run(os.Args); err != nil {
		log.Errorf("Error: %s", err.Error())
		os.Exit(1)
	}
}

func before(cctx *cli.Context) error {
	_ = logging.SetLogLevel("bench", "info")

	if cliutil.IsVeryVerbose {
		_ = logging.SetLogLevel("bench", "debug")
	}

	return nil
}
