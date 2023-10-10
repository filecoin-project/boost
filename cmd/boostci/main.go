package main

import (
	"io"
	"log"
	"os"

	"github.com/urfave/cli/v2"
)

func init() {
	log.SetOutput(io.Discard)
}

func main() {
	app := &cli.App{
		Name:                 "boostci",
		Usage:                "Boost CI tools",
		EnableBashCompletion: true,
		Commands: []*cli.Command{
			fetchParamCmd,
		},
	}
	app.Setup()

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
