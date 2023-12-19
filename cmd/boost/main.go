package main

import (
	"encoding/json"
	"fmt"
	"io"
	llog "log"
	"os"

	"github.com/filecoin-project/boost/cmd"

	"github.com/filecoin-project/boost/build"
	"github.com/filecoin-project/boost/extern/boostd-data/shared/cliutil"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("boost")

func init() {
	llog.SetOutput(io.Discard)
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
			retrieveCmd,
			offlineDealCmd,
			providerCmd,
			walletCmd,
			//TODO: enable when DDO ends up in a network upgrade
			//directDealAllocate,
			//directDealGetAllocations,
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
