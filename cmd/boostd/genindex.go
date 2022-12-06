package main

import (
	"fmt"
	_ "net/http/pprof"
	"time"

	bcli "github.com/filecoin-project/boost/cli"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
)

var genindexCmd = &cli.Command{
	Name:  "genindex",
	Usage: "Generate index for a given piececid and store it in the piece directory",
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:  "add-index-throttle",
			Usage: "the maximum number of add index operations that can run in parallel",
			Value: 4,
		},
		&cli.StringFlag{
			Name:     "api-fullnode",
			Usage:    "the endpoint for the full node API",
			Required: false,
		},
		&cli.StringFlag{
			Name:     "api-storage",
			Usage:    "the endpoint for the storage node API",
			Required: false,
		},
		&cli.StringFlag{
			Name:     "api-piece-directory",
			Usage:    "the endpoint for the piece directory API",
			Required: false,
		},
		&cli.StringFlag{
			Name:     "piece-cid",
			Usage:    "piece-cid to index",
			Required: true,
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)

		// parse piececid
		piececid, err := cid.Decode(cctx.String("piece-cid"))
		if err != nil {
			return err
		}
		fmt.Println("piece-cid to index: ", piececid)

		boostApi, ncloser, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return fmt.Errorf("getting boost api: %w", err)
		}
		defer ncloser()

		addStart := time.Now()

		fmt.Printf("about to generate and add index for piece-cid %s to the piece directory\n", piececid)
		err = boostApi.PdBuildIndexForPieceCid(ctx, piececid)
		if err != nil {
			return err
		}

		fmt.Println("adding index took", time.Since(addStart).String())

		fmt.Printf("successfully generated and added index for piece-cid %s to the piece directory\n", piececid)

		return nil
	},
}
