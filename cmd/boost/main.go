package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/filecoin-project/boost/cmd"

	llog "log"

	"github.com/filecoin-project/boost/build"
	cliutil "github.com/filecoin-project/boost/cli/util"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
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
			cmd.FlagRepo,
			cliutil.FlagVeryVerbose,
			cmd.FlagJson,
		},
		Commands: []*cli.Command{
			initCmd,
			dealCmd,
			dealStatusCmd,
			offlineDealCmd,
			providerCmd,
			walletCmd,
		},
	}
	app.Setup()

	if err := app.Run(os.Args); err != nil {
		if isJson, ok := app.Metadata["json"].(bool); isJson && ok {
			resJson, err := json.Marshal(map[string]string{"error": err.Error()})
			if err != nil {
				fmt.Fprintln(os.Stderr, fmt.Errorf("marshalling json: %w", err))
			} else {
				fmt.Fprintln(os.Stderr, string(resJson))
			}
		} else {
			fmt.Fprintln(os.Stderr, "Error: "+err.Error())
		}
		os.Exit(1)
	}
}

func before(cctx *cli.Context) error {
	_ = logging.SetLogLevel("boost", "INFO")

	if cliutil.IsVeryVerbose {
		_ = logging.SetLogLevel("boost", "DEBUG")
		_ = logging.SetLogLevel("boost-net", "DEBUG")
	}

	cctx.App.Metadata["json"] = cctx.Bool("json")

	return nil
}
