package main

import (
	"context"
	"log"
	"os"

	"github.com/filecoin-project/boost/pkg/devnet"

	"github.com/urfave/cli/v2"

	logging "github.com/ipfs/go-log/v2"
)

func init() {
	_ = logging.SetLogLevel("devnet", "DEBUG")
}

func main() {
	const initFlag = "initialize"
	app := &cli.App{
		Name:  "devnet",
		Usage: "Run a local devnet",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Aliases: []string{"i"},
				Name:    initFlag,
				Value:   true,
				Usage:   "Whether to initialize the devnet or attempt to use the existing state directories and config.",
			},
		},
		Action: func(cctx *cli.Context) error {

			home, err := os.UserHomeDir()
			if err != nil {
				return err
			}

			done := make(chan struct{})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go devnet.Run(ctx, home, done, cctx.Bool(initFlag))

			<-done
			return nil
		},
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)

	}

}
