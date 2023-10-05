package main

import (
	"fmt"
	"os"
	"path/filepath"

	bcli "github.com/filecoin-project/boost/cli"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
)

var importDirectDataCmd = &cli.Command{
	Name:      "import-direct",
	Usage:     "Import data for direct onboarding flow with Boost",
	ArgsUsage: "<piececid> <file>",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "delete-after-import",
			Usage: "whether to delete the data for the import after the data has been added to a sector",
			Value: false,
		},
		&cli.StringFlag{
			Name:     "client-addr",
			Usage:    "",
			Required: true,
		},
		&cli.Uint64Flag{
			Name:     "allocation-id",
			Usage:    "",
			Required: true,
		},
		&cli.BoolFlag{
			Name:  "remove-unsealed-copy",
			Usage: "",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "skip-ipni-announce",
			Usage: "indicates that deal index should not be announced to the IPNI(Network Indexer)",
			Value: false,
		},
		&cli.IntFlag{
			Name:  "start-epoch",
			Usage: "start epoch by when the deal should be proved by provider on-chain",
			Value: 35000, // default is 35000, handy for tests with 2k/devnet build
		},
		&cli.IntFlag{
			Name:  "duration",
			Usage: "duration of the deal in epochs",
			Value: 518400, // default is 2880 * 180 == 180 days
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() < 2 {
			return fmt.Errorf("must specify piececid and file path")
		}

		piececidStr := cctx.Args().Get(0)
		path := cctx.Args().Get(1)

		fullpath, err := homedir.Expand(path)
		if err != nil {
			return fmt.Errorf("expanding file path: %w", err)
		}

		filepath, err := filepath.Abs(fullpath)
		if err != nil {
			return fmt.Errorf("failed to get absolute path for file: %w", err)
		}

		_, err = os.Stat(filepath)
		if err != nil {
			return fmt.Errorf("opening file %s: %w", filepath, err)
		}

		piececid, err := cid.Decode(piececidStr)
		if err != nil {
			return fmt.Errorf("could not parse piececid: %w", err)
		}

		napi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		clientAddr, err := address.NewFromString(cctx.String("client-addr"))
		if err != nil {
			return fmt.Errorf("failed to parse clientaddr param: %w", err)
		}

		allocationId := cctx.Uint64("allocation-id")

		startEpoch := abi.ChainEpoch(cctx.Int("start-epoch"))
		endEpoch := startEpoch + abi.ChainEpoch(cctx.Int("duration"))

		ddParams := types.DirectDealParams{
			DealUUID:           uuid.New(),
			AllocationID:       verifreg.AllocationId(allocationId),
			PieceCid:           piececid,
			ClientAddr:         clientAddr,
			StartEpoch:         startEpoch,
			EndEpoch:           endEpoch,
			FilePath:           filepath,
			DeleteAfterImport:  cctx.Bool("delete-after-import"),
			RemoveUnsealedCopy: cctx.Bool("remove-unsealed-copy"),
			SkipIPNIAnnounce:   cctx.Bool("skip-ipni-announce"),
		}
		rej, err := napi.BoostDirectDeal(cctx.Context, ddParams)
		if err != nil {
			return fmt.Errorf("failed to execute direct data import: %w", err)
		}
		if rej != nil && rej.Reason != "" {
			return fmt.Errorf("direct data import rejected: %s", rej.Reason)
		}
		fmt.Println("Direct data import scheduled for execution")
		return nil
	},
}
