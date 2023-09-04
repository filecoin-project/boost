package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	bcli "github.com/filecoin-project/boost/cli"
	clinode "github.com/filecoin-project/boost/cli/node"
	"github.com/filecoin-project/boost/cmd"
	"github.com/filecoin-project/boost/cmd/lib"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/builtin"
	datacap2 "github.com/filecoin-project/go-state-types/builtin/v9/datacap"
	verifregst "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/actors/builtin/datacap"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/lib/tablewriter"
	"github.com/ipfs/go-cid"
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
			Name:     "pieceInfo",
			Usage:    "data pieceInfo[s] to create the allocation. The format must be --pieceInfo pieceCid1=pieceSize1 --pieceInfo pieceCid2=pieceSize2",
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
			Name: "termmin",
			Usage: "The minimum duration which the provider must commit to storing the piece to avoid early-termination penalties (epochs).\n" +
				"Default is 180 days.",
			Aliases: []string{"tmin"},
		},
		&cli.Int64Flag{
			Name: "termmax",
			Usage: "The maximum period for which a provider can earn quality-adjusted power for the piece (epochs).\n" +
				"Default is 5 years.",
			Aliases: []string{"tmax"},
		},
		&cli.Int64Flag{
			Name: "expiration",
			Usage: "The latest epoch by which a provider must commit data before the allocation expires (epochs).\n" +
				"Default is 60 days.",
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

		// Get all minerIDs from input
		maddrs := make(map[abi.ActorID]lapi.MinerInfo)
		minerIds := cctx.StringSlice("miner")
		for _, id := range minerIds {
			maddr, err := address.NewFromString(id)
			if err != nil {
				return err
			}

			// Verify that minerID exists
			m, err := gapi.StateMinerInfo(ctx, maddr, types.EmptyTSK)
			if err != nil {
				return err
			}

			mid, err := address.IDFromAddress(maddr)
			if err != nil {
				return err
			}

			maddrs[abi.ActorID(mid)] = m
		}

		// Get all pieceCIDs from input
		rDataCap := big.NewInt(0)
		var pieceInfos []*abi.PieceInfo
		pieces := cctx.StringSlice("pieceInfo")
		for _, p := range pieces {
			pieceDetail := strings.Split(p, "=")
			if len(pieceDetail) > 2 {
				return fmt.Errorf("incorrect pieceInfo format: %s", pieceDetail)
			}

			n, err := strconv.ParseInt(pieceDetail[1], 10, 64)
			if err != nil {
				return fmt.Errorf("failed to parse the piece size for %s for pieceCid %s: %w", pieceDetail[0], pieceDetail[1], err)
			}
			pcid, err := cid.Parse(pieceDetail[0])
			if err != nil {
				return fmt.Errorf("failed to parse the pieceCid for %s: %w", pieceDetail[0], err)
			}

			pieceInfos = append(pieceInfos, &abi.PieceInfo{
				Size:     abi.PaddedPieceSize(n),
				PieceCID: pcid,
			})
			rDataCap.Add(big.NewInt(n).Int, rDataCap.Int)
		}

		// Get datacap balance
		aDataCap, err := gapi.StateVerifiedClientStatus(ctx, walletAddr, types.EmptyTSK)
		if err != nil {
			return err
		}

		// Check that we have enough data cap to make the allocation
		if rDataCap.GreaterThan(big.NewInt(aDataCap.Int64())) {
			return fmt.Errorf("requested datacap is greater then the available datacap")
		}

		// Setup policy for allocations
		termMin := abi.ChainEpoch(verifregst.MinimumVerifiedAllocationTerm)
		termMax := abi.ChainEpoch(verifregst.MaximumVerifiedAllocationTerm)
		exp := abi.ChainEpoch(verifregst.MaximumVerifiedAllocationExpiration)

		if cctx.IsSet("termmin") {
			termMin = abi.ChainEpoch(cctx.Int64("termmin"))
		}

		if cctx.IsSet("termmin") {
			termMax = abi.ChainEpoch(cctx.Int64("termmax"))
		}

		if cctx.IsSet("termmin") {
			exp = abi.ChainEpoch(cctx.Int64("expiration"))
		}

		if termMax < termMin {
			return fmt.Errorf("maximum duration %d cannot be smaller than minimum duration %d", termMax, termMin)
		}

		head, err := gapi.ChainHead(ctx)
		if err != nil {
			return err
		}

		if termMin < head.Height() || termMax < head.Height() {
			return fmt.Errorf("current chain head %d is greater than termMin %d or termMax %d", head.Height(), termMin, termMax)
		}

		// Create allocation requests
		var allocationRequests []verifregst.AllocationRequest
		for mid, minfo := range maddrs {
			for _, p := range pieceInfos {
				if uint64(minfo.SectorSize) < uint64(p.Size) {
					return fmt.Errorf("specified piece size %d is bigger than miner's sector size %s", uint64(p.Size), minfo.SectorSize.String())
				}
				allocationRequests = append(allocationRequests, verifregst.AllocationRequest{
					Provider:   mid,
					Data:       p.PieceCID,
					Size:       p.Size,
					TermMin:    termMin,
					TermMax:    termMax,
					Expiration: exp,
				})
			}
		}

		arequest := &verifregst.AllocationRequests{
			Allocations: allocationRequests,
		}

		receiverParams, err := actors.SerializeParams(arequest)
		if err != nil {
			return fmt.Errorf("failed to seralize the parameters: %w", err)
		}

		transferParams, err := actors.SerializeParams(&datacap2.TransferParams{
			To:           builtin.VerifiedRegistryActorAddr,
			Amount:       big.Mul(rDataCap, builtin.TokenPrecision),
			OperatorData: receiverParams,
		})

		if err != nil {
			return fmt.Errorf("failed to serialize transfer parameters: %w", err)
		}

		msg := &types.Message{
			To:     builtin.DatacapActorAddr,
			From:   walletAddr,
			Method: datacap.Methods.TransferExported,
			Params: transferParams,
			Value:  big.Zero(),
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

		newallocations, err := gapi.StateGetAllocations(ctx, walletAddr, types.EmptyTSK)
		if err != nil {
			return fmt.Errorf("failed to get allocations: %w", err)
		}

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

		for key, val := range newallocations {
			_, ok := oldallocations[key]
			if !ok {
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
		}

		if !cctx.Bool("quiet") {

			if cctx.Bool("json") {
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

		return nil
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

		if !cctx.Bool("quiet") {

			if cctx.Bool("json") {
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

		return nil
	},
}
