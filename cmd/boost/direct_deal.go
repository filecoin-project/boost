package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	bcli "github.com/filecoin-project/boost/cli"
	clinode "github.com/filecoin-project/boost/cli/node"
	"github.com/filecoin-project/boost/cmd"
	"github.com/filecoin-project/boost/cmd/boost/util"
	"github.com/filecoin-project/boost/cmd/lib"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	verifregst "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors/builtin/verifreg"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/lib/tablewriter"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
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
		type jalloc struct {
			Client     abi.ActorID         `json:"client"`
			Provider   abi.ActorID         `json:"provider"`
			Data       cid.Cid             `json:"data"`
			Size       abi.PaddedPieceSize `json:"size"`
			TermMin    abi.ChainEpoch      `json:"term_min"`
			TermMax    abi.ChainEpoch      `json:"term_max"`
			Expiration abi.ChainEpoch      `json:"expiration"`
		}
		allocMap := make(map[verifregst.AllocationId]jalloc, len(allocations))
		for id, allocation := range allocations {
			allocMap[id] = jalloc{
				Provider:   allocation.Provider,
				Client:     allocation.Client,
				Data:       allocation.Data,
				Size:       allocation.Size,
				TermMin:    allocation.TermMin,
				TermMax:    allocation.TermMax,
				Expiration: allocation.Expiration,
			}
		}
		return cmd.PrintJson(map[string]any{"allocations": allocations})
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

var clientExtendDealCmd = &cli.Command{
	Name:  "extend-claim",
	Usage: "extend claim expiration (TermMax)",
	Flags: []cli.Flag{
		&cli.Int64Flag{
			Name: "term-max",
			Usage: "The maximum period for which a provider can earn quality-adjusted power for the piece (epochs).\n" +
				"Default is 5 years.",
			Aliases: []string{"tmax"},
			Value:   verifregst.MaximumVerifiedAllocationTerm,
		},
		&cli.StringFlag{
			Name:  "wallet",
			Usage: "the wallet address that will used to send the message",
		},
		&cli.BoolFlag{
			Name:  "all",
			Usage: "automatically extend TermMax of all claims for specified miner[s] to --term-max (default: 5 years from claim start epoch)",
		},
		&cli.StringSliceFlag{
			Name:    "miner",
			Usage:   "storage provider address[es]",
			Aliases: []string{"m", "provider", "p"},
		},
		&cli.BoolFlag{
			Name:    "assume-yes",
			Usage:   "automatic yes to prompts; assume 'yes' as answer to all prompts and run non-interactively",
			Aliases: []string{"y", "yes"},
		},
		&cli.IntFlag{
			Name:  "confidence",
			Usage: "number of block confirmations to wait for",
			Value: int(build.MessageConfidence),
		},
		cmd.FlagRepo,
	},
	ArgsUsage: "<claim1> <claim2> ... or <miner1=claim1> <miner2=claims2> ...",
	Action: func(cctx *cli.Context) error {

		n, err := clinode.Setup(cctx.String(cmd.FlagRepo.Name))
		if err != nil {
			return err
		}

		miners := cctx.StringSlice("miner")
		all := cctx.Bool("all")
		wallet := cctx.String("wallet")
		tmax := cctx.Int64("term-max")

		// No miner IDs and no arguments
		if len(miners) == 0 && cctx.Args().Len() == 0 {
			return fmt.Errorf("must specify at least one miner ID or argument[s]")
		}

		// Single Miner with no claimID and no --all flag
		if len(miners) == 1 && cctx.Args().Len() == 0 && !all {
			return fmt.Errorf("must specify either --all flag or claim IDs to extend in argument")
		}

		// Multiple Miner with claimIDs
		if len(miners) > 1 && cctx.Args().Len() > 0 {
			return fmt.Errorf("either specify multiple miner IDs or multiple arguments")
		}

		// Multiple Miner with no claimID and no --all flag
		if len(miners) > 1 && cctx.Args().Len() == 0 && !all {
			return fmt.Errorf("must specify --all flag with multiple miner IDs")
		}

		// Tmax can't be more than policy max
		if tmax > verifregst.MaximumVerifiedAllocationTerm {
			return fmt.Errorf("specified term-max %d is larger than %d maximum allowed by verified regirty actor policy", tmax, verifregst.MaximumVerifiedAllocationTerm)
		}

		gapi, closer, err := lcli.GetGatewayAPI(cctx)
		if err != nil {
			return fmt.Errorf("can't setup gateway connection: %w", err)
		}
		defer closer()

		ctx := bcli.ReqContext(cctx)
		claimMap := make(map[verifregst.ClaimId]util.ProvInfo)

		// If no miners and arguments are present
		if len(miners) == 0 && cctx.Args().Len() > 0 {
			for _, arg := range cctx.Args().Slice() {
				detail := strings.Split(arg, "=")
				if len(detail) > 2 {
					return fmt.Errorf("incorrect argument format: %s", detail)
				}

				n, err := strconv.ParseInt(detail[1], 10, 64)
				if err != nil {
					return fmt.Errorf("failed to parse the claim ID for %s for argument %s: %w", detail[0], detail, err)
				}

				maddr, err := address.NewFromString(detail[0])
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

				pi := util.ProvInfo{
					Addr: maddr,
					ID:   abi.ActorID(mid),
				}

				claimMap[verifregst.ClaimId(n)] = pi
			}
		}

		// If 1 miner ID and multiple arguments
		if len(miners) == 1 && cctx.Args().Len() > 0 && !all {
			for _, arg := range cctx.Args().Slice() {
				detail := strings.Split(arg, "=")
				if len(detail) == 1 {
					return fmt.Errorf("incorrect argument format %s. Must provide only claim IDs with single miner ID", detail)
				}

				n, err := strconv.ParseInt(detail[0], 10, 64)
				if err != nil {
					return fmt.Errorf("failed to parse the claim ID for %s for argument %s: %w", detail[0], detail, err)
				}

				claimMap[verifregst.ClaimId(n)] = util.ProvInfo{}
			}
		}

		// Get wallet address from input
		walletAddr, err := n.GetProvidedOrDefaultWallet(ctx, wallet)
		if err != nil {
			return err
		}

		log.Debugw("selected wallet", "wallet", walletAddr)

		msgs, err := util.CreateExtendClaimMsg(ctx, gapi, claimMap, miners, walletAddr, abi.ChainEpoch(tmax), all, cctx.Bool("assume-yes"))
		if err != nil {
			return err
		}

		// If not msgs are found then no claims can be extended
		if msgs == nil {
			fmt.Println("No eligible claims found")
			return nil
		}

		var mcids []cid.Cid

		for _, msg := range msgs {
			mcid, sent, err := lib.SignAndPushToMpool(cctx, ctx, gapi, n, msg)
			if err != nil {
				return err
			}
			if !sent {
				fmt.Printf("message %s with method %s not send", msg.Cid(), msg.Method.String())
				continue
			}
			mcids = append(mcids, mcid)
		}

		// wait for msgs to get mined into a block
		var wg sync.WaitGroup
		wg.Add(len(mcids))
		results := make(chan error, len(mcids))
		for _, msg := range mcids {
			m := msg
			go func() {
				defer wg.Done()
				wait, err := gapi.StateWaitMsg(ctx, m, uint64(cctx.Int("confidence")), 2000, true)
				if err != nil {
					results <- xerrors.Errorf("Timeout waiting for message to land on chain %s", wait.Message)
					return
				}

				if wait.Receipt.ExitCode.IsError() {
					results <- fmt.Errorf("failed to execute message %s: %w", wait.Message, wait.Receipt.ExitCode)
					return
				}
				results <- nil
			}()
		}

		wg.Wait()
		close(results)

		for res := range results {
			if res != nil {
				fmt.Println("Failed to execute the message %w", res)
			}
		}
		return nil
	},
}

var listClaimsCmd = &cli.Command{
	Name:      "list-claims",
	Usage:     "List claims made by provider",
	ArgsUsage: "providerAddress",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "expired",
			Usage: "list only expired claims",
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.NArg() != 1 {
			return fmt.Errorf("must provide a miner ID")
		}

		gapi, closer, err := lcli.GetGatewayAPI(cctx)
		if err != nil {
			return fmt.Errorf("can't setup gateway connection: %w", err)
		}
		defer closer()

		ctx := bcli.ReqContext(cctx)

		providerAddr, err := address.NewFromString(cctx.Args().Get(0))
		if err != nil {
			return err
		}

		head, err := gapi.ChainHead(ctx)
		if err != nil {
			return err
		}

		claims, err := gapi.StateGetClaims(ctx, providerAddr, types.EmptyTSK)
		if err != nil {
			return err
		}

		if cctx.Bool("json") {
			type jclaim struct {
				Provider  abi.ActorID         `json:"provider"`
				Client    abi.ActorID         `json:"client"`
				Data      cid.Cid             `json:"data"`
				Size      abi.PaddedPieceSize `json:"size"`
				TermMin   abi.ChainEpoch      `json:"term_min"`
				TermMax   abi.ChainEpoch      `json:"term_max"`
				TermStart abi.ChainEpoch      `json:"term_start"`
				Sector    abi.SectorNumber    `json:"sector"`
			}
			claimMap := make(map[verifregst.ClaimId]jclaim, len(claims))
			for id, claim := range claims {
				claimMap[id] = jclaim{
					Provider:  claim.Provider,
					Client:    claim.Client,
					Data:      claim.Data,
					Size:      claim.Size,
					TermMin:   claim.TermMin,
					TermMax:   claim.TermMax,
					TermStart: claim.TermStart,
					Sector:    claim.Sector,
				}
			}
			return cmd.PrintJson(map[string]any{"claims": claimMap})
		}

		tw := tablewriter.New(
			tablewriter.Col("ID"),
			tablewriter.Col("Provider"),
			tablewriter.Col("Client"),
			tablewriter.Col("Data"),
			tablewriter.Col("Size"),
			tablewriter.Col("TermMin"),
			tablewriter.Col("TermMax"),
			tablewriter.Col("TermStart"),
			tablewriter.Col("Sector"),
		)

		for claimId, claim := range claims {
			if head.Height() > claim.TermMax+claim.TermStart || !cctx.IsSet("expired") {
				tw.Write(map[string]interface{}{
					"ID":        claimId,
					"Provider":  claim.Provider,
					"Client":    claim.Client,
					"Data":      claim.Data,
					"Size":      claim.Size,
					"TermMin":   claim.TermMin,
					"TermMax":   claim.TermMax,
					"TermStart": claim.TermStart,
					"Sector":    claim.Sector,
				})
			}
		}
		return tw.Flush(os.Stdout)
	},
}
