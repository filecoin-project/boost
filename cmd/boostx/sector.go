package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/filecoin-project/boost/cmd/lib"
	"github.com/filecoin-project/go-state-types/abi"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/urfave/cli/v2"
)

var sectorCmd = &cli.Command{
	Name:  "sector",
	Usage: "Sector commands",
	Subcommands: []*cli.Command{
		sectorUnsealCmd,
		isUnsealedCmd,
	},
}

var sectorUnsealCmd = &cli.Command{
	Name:      "unseal",
	Usage:     "Unseal a sector",
	ArgsUsage: "<sector ID>",
	Before:    before,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "api-fullnode",
			Usage:    "the endpoint for the full node API",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "api-storage",
			Usage:    "the endpoint for the storage node API",
			Required: true,
		},
	},
	Action: func(cctx *cli.Context) error {
		if !cctx.Args().Present() {
			return fmt.Errorf("must pass sector ID as first parameter")
		}

		sectorIDInt, err := strconv.Atoi(cctx.Args().First())
		if err != nil {
			return fmt.Errorf("parsing sector ID %s: %w", cctx.Args().First(), err)
		}
		sectorID := abi.SectorNumber(sectorIDInt)

		ctx := lcli.ReqContext(cctx)

		// Connect to the full node API
		fnApiInfo := cctx.String("api-fullnode")
		fullnodeApi, ncloser, err := lib.GetFullNodeApi(ctx, fnApiInfo, log)
		if err != nil {
			return fmt.Errorf("getting full node API: %w", err)
		}
		defer ncloser()

		err = lib.CheckFullNodeApiVersion(ctx, fullnodeApi)
		if err != nil {
			fmt.Printf("Warning: %s\n", err.Error())
		}

		// Connect to the storage API and create a sector accessor
		storageApiInfo := cctx.String("api-storage")
		sa, storageCloser, err := lib.CreateSectorAccessor(ctx, storageApiInfo, fullnodeApi, log)
		if err != nil {
			return err
		}
		defer storageCloser()

		pieceLength := abi.PaddedPieceSize(1024).Unpadded()
		isUnsealed, err := sa.IsUnsealed(ctx, sectorID, 0, pieceLength)
		if err != nil {
			return fmt.Errorf("getting sealed state of sector: %w", err)
		}

		if isUnsealed {
			fmt.Printf("Sector %d is already unsealed\n", sectorID)
			return nil
		}

		start := time.Now()
		fmt.Printf("Unsealing sector %d\n", sectorID)
		_, err = sa.UnsealSectorAt(ctx, sectorID, 0, pieceLength)
		if err != nil {
			return fmt.Errorf("unsealing sector %d: %w", sectorID, err)
		}

		fmt.Printf("Successfully unsealed sector %d after %s\n", sectorID, time.Since(start).String())

		return nil
	},
}

var isUnsealedCmd = &cli.Command{
	Name:        "is-unsealed",
	Usage:       "boostx sector is-unsealed <Sector ID>",
	Description: "Check if a particular sector is unsealed using the miner API. This is a definitive method to check for unsealed copies",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "api-fullnode",
			Usage:    "the endpoint for the full node API",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "api-storage",
			Usage:    "the endpoint for the storage node API",
			Required: true,
		},
	},
	Action: func(cctx *cli.Context) error {

		if !cctx.Args().Present() {
			return fmt.Errorf("must pass sector ID as first parameter")
		}

		sectorIDInt, err := strconv.Atoi(cctx.Args().First())
		if err != nil {
			return fmt.Errorf("parsing sector ID %s: %w", cctx.Args().First(), err)
		}
		sectorID := abi.SectorNumber(sectorIDInt)

		ctx := lcli.ReqContext(cctx)

		// Connect to the full node API
		fnApiInfo := cctx.String("api-fullnode")
		fullnodeApi, ncloser, err := lib.GetFullNodeApi(ctx, fnApiInfo, log)
		if err != nil {
			return fmt.Errorf("getting full node API: %w", err)
		}
		defer ncloser()

		// Connect to the storage API and create a sector accessor
		storageApiInfo := cctx.String("api-storage")
		sa, storageCloser, err := lib.CreateSectorAccessor(ctx, storageApiInfo, fullnodeApi, log)
		if err != nil {
			return err
		}
		defer storageCloser()

		pieceLength := abi.PaddedPieceSize(1024).Unpadded()
		isUnsealed, err := sa.IsUnsealed(ctx, sectorID, 0, pieceLength)
		if err != nil {
			return fmt.Errorf("getting sealed state of sector: %w", err)
		}

		if isUnsealed {
			fmt.Printf("Sector %d is unsealed\n", sectorID)
			return nil
		}

		fmt.Printf("Sector %d is NOT unsealed\n", sectorID)

		return nil
	},
}
