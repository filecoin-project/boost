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

var pieceDirCmd = &cli.Command{
	Name:  "lid",
	Usage: "Manage Local Index Directory",
	Subcommands: []*cli.Command{
		pdIndexGenerate,
	},
}

var pdIndexGenerate = &cli.Command{
	Name:      "gen-index",
	Usage:     "Generate index for a given piece from the piece data stored in a sector",
	ArgsUsage: "<piece CID>",
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)

		if cctx.Args().Len() != 1 {
			return fmt.Errorf("must specify piece CID")
		}

		// parse piececid
		pieceCid, err := cid.Decode(cctx.Args().Get(0))
		if err != nil {
			return fmt.Errorf("parsing piece CID: %w", err)
		}
		fmt.Println("Generating index for piece", pieceCid)

		boostApi, ncloser, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return fmt.Errorf("getting boost api: %w", err)
		}
		defer ncloser()

		addStart := time.Now()

		progressch, err := boostApi.PdBuildIndexForPieceCid(ctx, pieceCid)
		if err != nil {
			return err
		}
		for {
			select {
			case <-ctx.Done():
				return fmt.Errorf("command cancelled after %s: %w", time.Since(addStart), ctx.Err())
			case progress, ok := <-progressch:
				if !ok {
					fmt.Println("Generated index in", time.Since(addStart).String())
					return nil
				}
				if progress.Error != "" {
					return fmt.Errorf(progress.Error)
				}
				fmt.Printf("Progress: %.0f%%\n", progress.Progress*100)
			}
		}

		return nil
	},
}
