package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	bcli "github.com/filecoin-project/boost/cli"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin"
	"github.com/filecoin-project/go-state-types/builtin/v13/miner"
	"github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	lcli "github.com/filecoin-project/lotus/cli"
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
			Usage: "start epoch by when the deal should be proved by provider on-chain (default: 2 days from now)",
		},
		&cli.StringSliceFlag{
			Name:  "notify",
			Usage: "DDO notifications in format 'address:payload",
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() < 2 {
			return fmt.Errorf("must specify piececid and file path")
		}

		ctx := cctx.Context

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

		lapi, lcloser, err := lcli.GetFullNodeAPIV1(cctx)
		if err != nil {
			return err
		}
		defer lcloser()

		head, err := lapi.ChainHead(ctx)
		if err != nil {
			return fmt.Errorf("getting chain head: %w", err)
		}

		clientAddr, err := address.NewFromString(cctx.String("client-addr"))
		if err != nil {
			return fmt.Errorf("failed to parse clientaddr param: %w", err)
		}

		allocationId := cctx.Uint64("allocation-id")

		startEpoch := abi.ChainEpoch(cctx.Int("start-epoch"))
		// Set Default if not specified by the user
		if startEpoch == 0 {
			startEpoch = head.Height() + (builtin.EpochsInDay * 2)
		}
		alloc, err := lapi.StateGetAllocation(ctx, clientAddr, verifreg.AllocationId(allocationId), head.Key())
		if err != nil {
			return fmt.Errorf("getting allocation details from chain: %w", err)
		}
		if alloc == nil {
			return fmt.Errorf("no allocation found with ID %d", allocationId)
		}

		if alloc.Expiration < startEpoch {
			return fmt.Errorf("allocation will expire on %d before start epoch %d", alloc.Expiration, startEpoch)
		}

		// Since StartEpoch is more than Head+StartEpochSealingBuffer, we can set end epoch as start+TermMin
		endEpoch := startEpoch + alloc.TermMin

		var notifications []miner.DataActivationNotification
		for _, notifyStr := range cctx.StringSlice("notify") {
			parts := strings.SplitN(notifyStr, ":", 2)
			if len(parts) != 2 {
				return fmt.Errorf("invalid notify format '%s': expected 'address:payload'", notifyStr)
			}

			// Parse address
			addr, err := address.NewFromString(parts[0])
			if err != nil {
				return fmt.Errorf("invalid address in notify '%s': %w", parts[0], err)
			}

			// Parse payload (hex string)
			payload, err := hex.DecodeString(strings.TrimPrefix(parts[1], "0x"))
			if err != nil {
				return fmt.Errorf("invalid hex payload in notify '%s': %w", parts[1], err)
			}

			notifications = append(notifications, miner.DataActivationNotification{
				Address: addr,
				Payload: payload,
			})
		}

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
			Notifications:      notifications,
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
