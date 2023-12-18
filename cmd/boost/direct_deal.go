package main

import (
	"fmt"
	"os"
	"strings"

	bcli "github.com/filecoin-project/boost/cli"
	clinode "github.com/filecoin-project/boost/cli/node"
	"github.com/filecoin-project/boost/cmd"
	"github.com/filecoin-project/boost/cmd/boost/util"
	"github.com/filecoin-project/boost/cmd/lib"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	verifregst "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors/builtin/verifreg"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/lib/tablewriter"
	"github.com/urfave/cli/v2"
)

var directDealAllocate = &cli.Command{
	Name:  "allocate",
	Usage: "Create new allocation[s] for verified deals",
	Flags: []cli.Flag{
		&cli.StringSliceFlag{
			Name:     "miner",
			Usage:    "storage provider address[es]",
			Required: true,
			Aliases:  []string{"m", "provider", "p"},
		},
		&cli.StringSliceFlag{
			Name:     "piece-info",
			Usage:    "data piece-info[s] to create the allocation. The format must be --piece-info pieceCid1=pieceSize1 --piece-info pieceCid2=pieceSize2",
			Required: true,
			Aliases:  []string{"pi"},
		},
		&cli.StringFlag{
			Name:  "wallet",
			Usage: "the wallet address that will used create the allocation",
		},
		&cli.BoolFlag{
			Name:  "quiet",
			Usage: "do not print the allocation list",
			Value: false,
		},
		&cli.Int64Flag{
			Name: "term-min",
			Usage: "The minimum duration which the provider must commit to storing the piece to avoid early-termination penalties (epochs).\n" +
				"Default is 180 days.",
			Aliases: []string{"tmin"},
			Value:   verifregst.MinimumVerifiedAllocationTerm,
		},
		&cli.Int64Flag{
			Name: "term-max",
			Usage: "The maximum period for which a provider can earn quality-adjusted power for the piece (epochs).\n" +
				"Default is 5 years.",
			Aliases: []string{"tmax"},
			Value:   verifregst.MaximumVerifiedAllocationTerm,
		},
		&cli.Int64Flag{
			Name: "expiration",
			Usage: "The latest epoch by which a provider must commit data before the allocation expires (epochs).\n" +
				"Default is 60 days.",
			Value: verifregst.MaximumVerifiedAllocationExpiration,
		},
	},
	Before: before,
	Action: func(cctx *cli.Context) error {
		ctx := bcli.ReqContext(cctx)

		n, err := clinode.Setup(cctx.String(cmd.FlagRepo.Name))
		if err != nil {
			return err
		}

		gapi, closer, err := lcli.GetGatewayAPI(cctx)
		if err != nil {
			return fmt.Errorf("can't setup gateway connection: %w", err)
		}
		defer closer()

		// Get wallet address from input
		walletAddr, err := n.GetProvidedOrDefaultWallet(ctx, cctx.String("wallet"))
		if err != nil {
			return err
		}

		log.Debugw("selected wallet", "wallet", walletAddr)

		msg, err := util.CreateAllocationMsg(ctx, gapi, cctx.StringSlice("piece-info"), cctx.StringSlice("miner"), walletAddr, abi.ChainEpoch(cctx.Int64("term-min")), abi.ChainEpoch(cctx.Int64("term-max")), abi.ChainEpoch(cctx.Int64("expiration")))

		if err != nil {
			return err
		}

		oldallocations, err := gapi.StateGetAllocations(ctx, walletAddr, types.EmptyTSK)
		if err != nil {
			return fmt.Errorf("failed to get allocations: %w", err)
		}

		mcid, sent, err := lib.SignAndPushToMpool(cctx, ctx, gapi, n, msg)
		if err != nil {
			return err
		}
		if !sent {
			return nil
		}

		log.Infow("submitted data cap allocation message", "cid", mcid.String())
		log.Info("waiting for message to be included in a block")

		res, err := gapi.StateWaitMsg(ctx, mcid, 1, lapi.LookbackNoLimit, true)
		if err != nil {
			return fmt.Errorf("waiting for message to be included in a block: %w", err)
		}

		if !res.Receipt.ExitCode.IsSuccess() {
			return fmt.Errorf("failed to execute the message with error: %s", res.Receipt.ExitCode.Error())
		}

		// Return early of quiet flag is set
		if cctx.Bool("quiet") {
			return nil
		}

		newallocations, err := gapi.StateGetAllocations(ctx, walletAddr, types.EmptyTSK)
		if err != nil {
			return fmt.Errorf("failed to get allocations: %w", err)
		}

		// Generate a diff to find new allocations
		for i := range newallocations {
			_, ok := oldallocations[i]
			if ok {
				delete(newallocations, i)
			}
		}

		return printAllocation(newallocations, cctx.Bool("json"))
	},
}

var directDealGetAllocations = &cli.Command{
	Name:  "list-allocations",
	Usage: "Lists all allocations for a client address(wallet)",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "miner",
			Usage:   "Storage provider address. If provided, only allocations against this minerID will be printed",
			Aliases: []string{"m", "provider", "p"},
		},
		&cli.StringFlag{
			Name:  "wallet",
			Usage: "the wallet address that will used create the allocation",
		},
	},
	Before: before,
	Action: func(cctx *cli.Context) error {
		ctx := bcli.ReqContext(cctx)

		n, err := clinode.Setup(cctx.String(cmd.FlagRepo.Name))
		if err != nil {
			return err
		}

		gapi, closer, err := lcli.GetGatewayAPI(cctx)
		if err != nil {
			return fmt.Errorf("cant setup gateway connection: %w", err)
		}
		defer closer()

		// Get wallet address from input
		walletAddr, err := n.GetProvidedOrDefaultWallet(ctx, cctx.String("wallet"))
		if err != nil {
			return err
		}

		log.Debugw("selected wallet", "wallet", walletAddr)

		allocations, err := gapi.StateGetAllocations(ctx, walletAddr, types.EmptyTSK)
		if err != nil {
			return fmt.Errorf("failed to get allocations: %w", err)
		}

		if cctx.String("miner") != "" {
			// Get all minerIDs from input
			minerId := cctx.String("miner")
			maddr, err := address.NewFromString(minerId)
			if err != nil {
				return err
			}

			// Verify that minerID exists
			_, err = gapi.StateMinerInfo(ctx, maddr, types.EmptyTSK)
			if err != nil {
				return err
			}

			mid, err := address.IDFromAddress(maddr)
			if err != nil {
				return err
			}

			for i, v := range allocations {
				if v.Provider != abi.ActorID(mid) {
					delete(allocations, i)
				}
			}
		}

		return printAllocation(allocations, cctx.Bool("json"))
	},
}

func printAllocation(allocations map[verifreg.AllocationId]verifreg.Allocation, json bool) error {
	// Map Keys. Corresponds to the standard tablewriter output
	allocationID := "AllocationID"
	client := "Client"
	provider := "Miner"
	pieceCid := "PieceCid"
	pieceSize := "PieceSize"
	tMin := "TermMin"
	tMax := "TermMax"
	expr := "Expiration"

	// One-to-one mapping between tablewriter keys and JSON keys
	tableKeysToJsonKeys := map[string]string{
		allocationID: strings.ToLower(allocationID),
		client:       strings.ToLower(client),
		provider:     strings.ToLower(provider),
		pieceCid:     strings.ToLower(pieceCid),
		pieceSize:    strings.ToLower(pieceSize),
		tMin:         strings.ToLower(tMin),
		tMax:         strings.ToLower(tMax),
		expr:         strings.ToLower(expr),
	}

	var allocs []map[string]interface{}

	for key, val := range allocations {
		alloc := map[string]interface{}{
			allocationID: key,
			client:       val.Client,
			provider:     val.Provider,
			pieceCid:     val.Data,
			pieceSize:    val.Size,
			tMin:         val.TermMin,
			tMax:         val.TermMax,
			expr:         val.Expiration,
		}
		allocs = append(allocs, alloc)
	}

	if json {
		// get a new list of wallets with json keys instead of tablewriter keys
		var jsonAllocs []map[string]interface{}
		for _, alloc := range allocs {
			jsonAlloc := make(map[string]interface{})
			for k, v := range alloc {
				jsonAlloc[tableKeysToJsonKeys[k]] = v
			}
			jsonAllocs = append(jsonAllocs, jsonAlloc)
		}
		// then return this!
		return cmd.PrintJson(jsonAllocs)
	} else {
		// Init the tablewriter's columns
		tw := tablewriter.New(
			tablewriter.Col(allocationID),
			tablewriter.Col(client),
			tablewriter.Col(provider),
			tablewriter.Col(pieceCid),
			tablewriter.Col(pieceSize),
			tablewriter.Col(tMin),
			tablewriter.Col(tMax),
			tablewriter.NewLineCol(expr))
		// populate it with content
		for _, alloc := range allocs {
			tw.Write(alloc)
		}
		// return the corresponding string
		return tw.Flush(os.Stdout)
	}
}
