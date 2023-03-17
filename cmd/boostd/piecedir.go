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
		pdIndexMarkErroredCmd,
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
		piececid, err := cid.Decode(cctx.Args().Get(0))
		if err != nil {
			return fmt.Errorf("parsing piece CID: %w", err)
		}
		fmt.Println("Generating index for piece", piececid)

		boostApi, ncloser, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return fmt.Errorf("getting boost api: %w", err)
		}
		defer ncloser()

		addStart := time.Now()

		err = boostApi.PdBuildIndexForPieceCid(ctx, piececid)
		if err != nil {
			return err
		}

		fmt.Println("Generated index in", time.Since(addStart).String())

		return nil
	},
}

var pdIndexMarkErroredCmd = &cli.Command{
	Name:  "mark-index",
	Usage: "Mark an index errored for a given piece in the local index directory",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "piece-cid",
			Usage:    "piece-cid of the index that will be marked as errored",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "error",
			Usage:    "error message",
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

		boostApi, ncloser, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return fmt.Errorf("getting boost api: %w", err)
		}
		defer ncloser()

		errMsg := cctx.String("error")
		err = boostApi.PdMarkIndexErrored(ctx, piececid, errMsg)
		if err != nil {
			return err
		}

		fmt.Printf("Marked %s as errored with \"%s\"\n", piececid, errMsg)

		return nil
	},
}
