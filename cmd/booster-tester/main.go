package main

import (
	"os"
	"os/exec"

	"github.com/filecoin-project/boost/build"
	"github.com/urfave/cli/v2"

	cliutil "github.com/filecoin-project/boost/cli/util"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("booster-tester")

func main() {
	app := &cli.App{
		Name:                 "booster-tester",
		Usage:                "Various utilities for testing retrievals",
		EnableBashCompletion: true,
		Version:              build.UserVersion(),
		Flags: []cli.Flag{
			cliutil.FlagVeryVerbose,
		},
		Before: before,
		Commands: []*cli.Command{
			generateCarsCmd,
			graphsyncCmd,
			helloworldCmd,
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func before(cctx *cli.Context) error {
	_ = logging.SetLogLevel("booster-tester", "INFO")

	if cliutil.IsVeryVerbose {
		_ = logging.SetLogLevel("booster-tester", "DEBUG")
	}

	return nil
}

var helloworldCmd = &cli.Command{
	Name:        "helloworld",
	Usage:       "",
	Description: "Echos \"Hello World!\"",
	Action: func(cctx *cli.Context) error {

		cmd := StartHelloWorldCommand()
		cmd.Wait()

		return nil
	},
}

func StartHelloWorldCommand() *exec.Cmd {
	cmd := exec.Command("echo", "Hello World!")

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	cmd.Start()

	return cmd
}
