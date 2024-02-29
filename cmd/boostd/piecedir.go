package main

import (
	"fmt"
	_ "net/http/pprof"
	"time"

	bcli "github.com/filecoin-project/boost/cli"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
)

var pieceDirCmd = &cli.Command{
	Name:  "lid",
	Usage: "Manage Local Index Directory",
	Subcommands: []*cli.Command{
		pdIndexGenerate,
		recoverCmd,
		removeDealCmd,
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

var removeDealCmd = &cli.Command{
	Name:      "remove-deal",
	Usage:     "Removes a deal from piece metadata in LID. If the specified deal is the only one in piece metadata, index and metadata are also removed",
	ArgsUsage: "<piece CID> <deal UUID or Proposal CID>",
	Action: func(cctx *cli.Context) error {

		ctx := lcli.ReqContext(cctx)

		if cctx.Args().Len() > 2 {
			return fmt.Errorf("must specify piece CID and deal UUID/Proposal CID")
		}

		napi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		// parse piececid
		piececid, err := cid.Decode(cctx.Args().Get(0))
		if err != nil {
			return fmt.Errorf("parsing piece CID: %w", err)
		}

		id := cctx.Args().Get(1)

		// Parse to avoid sending garbage data to API
		dealUuid, err := uuid.Parse(id)
		if err != nil {
			propCid, err := cid.Decode(id)
			if err != nil {
				return fmt.Errorf("could not parse '%s' as deal uuid or proposal cid", id)
			}
			err = napi.PdRemoveDealForPiece(ctx, piececid, propCid.String())
			if err != nil {
				return err
			}
			fmt.Printf("Deal %s removed for piece %s\n", propCid, piececid)
			return nil
		}

		err = napi.PdRemoveDealForPiece(ctx, piececid, dealUuid.String())
		if err != nil {
			return err
		}
		fmt.Printf("Deal %s removed for piece %s\n", dealUuid, piececid)
		return nil

	},
}
