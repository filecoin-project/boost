package main

import (
	"io/ioutil"
	"os"

	llog "log"

	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"

	"github.com/filecoin-project/boost/build"
	cliutil "github.com/filecoin-project/boost/cli/util"
)

var log = logging.Logger("boost")

func init() {
	llog.SetOutput(ioutil.Discard)
}

func main() {
	app := &cli.App{
		Name:                 "boost",
		Usage:                "Boost client for Filecoin",
		EnableBashCompletion: true,
		Version:              build.UserVersion(),
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "gateway-url",
				Usage: "Filecoin gateway url",
				Value: "https://api.node.glif.io",
			},
			cliutil.FlagVeryVerbose,
		},
		Commands: []*cli.Command{
			initCmd,
			dealCmd,
		},
	}
	app.Setup()

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func before(cctx *cli.Context) error {
	_ = logging.SetLogLevel("boost", "INFO")

	if cliutil.IsVeryVerbose {
		_ = logging.SetLogLevel("boost", "DEBUG")
	}

	return nil
}
