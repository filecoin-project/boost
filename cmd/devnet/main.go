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

	app := &cli.App{
		Name:  "devnet",
		Usage: "Run a local devnet",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Aliases: []string{"bs"},
				Name:    "bootstrap",
				Value:   true,
				Usage:   "bootstrap the devnet",
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

			go devnet.Run(ctx, home, done, cctx.Bool("bootstrap"))

			<-done
			return nil
		},
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)

	}

}
