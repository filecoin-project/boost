package main

import (
	"fmt"
	"os/exec"
	"sync"
	"time"

	"github.com/urfave/cli/v2"
)

var graphsyncCmd = &cli.Command{
	Name:        "graphsync",
	Usage:       "",
	Description: "Executes numerous graphsync requests to a provider in parallel",
	Flags:       []cli.Flag{},
	ArgsUsage:   "<provider> <...cids>",
	Action: func(cctx *cli.Context) error {
		log.Debugln("args", cctx.Args().Slice())

		if cctx.Args().Len() < 2 {
			return fmt.Errorf("must provide a provider ID and at least one CID")
		}

		args := cctx.Args().Slice()
		providerId := args[0]
		cids := args[1:]

		var wg sync.WaitGroup

		for _, cid := range cids {
			wg.Add(1)

			cid := cid

			go func() {
				defer wg.Done()
				runLotusClientRetrieve(providerId, cid)
			}()
		}

		wg.Wait()

		return nil
	},
}

func runLotusClientRetrieve(providerId string, cid string) {
	outfile := fmt.Sprintf("load-test-%d", time.Now().Unix())
	cmd := exec.Command("lotus", "client", "retrieve", "--provider", providerId, cid, outfile)
	cmd.Run()
}
