package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/ipfs/go-datastore"
	ds_sync "github.com/ipfs/go-datastore/sync"

	bcli "github.com/filecoin-project/boost/cli"
	clinode "github.com/filecoin-project/boost/cli/node"
	"github.com/filecoin-project/boost/cmd"
	"github.com/filecoin-project/boost/cmd/boost/util"
	"github.com/filecoin-project/boost/cmd/lib"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	verifreg13types "github.com/filecoin-project/go-state-types/builtin/v13/verifreg"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors/builtin/verifreg"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/lib/tablewriter"
	"github.com/ipfs/go-cid"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
)

var directDealAllocate = &cli.Command{
	Name:        "allocate",
	Usage:       "Create new allocation[s] for verified deals",
	Description: "The command can accept a CSV formatted file in the format 'pieceCid,pieceSize,miner,tmin,tmax,expiration'",
	Flags: []cli.Flag{
		&cli.StringSliceFlag{
			Name:    "miner",
			Usage:   "storage provider address[es]",
			Aliases: []string{"m", "provider", "p"},
		},
		&cli.StringSliceFlag{
			Name:    "piece-info",
			Usage:   "data piece-info[s] to create the allocation. The format must be --piece-info pieceCid1=pieceSize1 --piece-info pieceCid2=pieceSize2",
			Aliases: []string{"pi"},
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
			Value:   verifreg13types.MinimumVerifiedAllocationTerm,
		},
		&cli.Int64Flag{
			Name: "term-max",
			Usage: "The maximum period for which a provider can earn quality-adjusted power for the piece (epochs).\n" +
				"Default is 5 years.",
			Aliases: []string{"tmax"},
			Value:   verifreg13types.MaximumVerifiedAllocationTerm,
		},
		&cli.Int64Flag{
			Name: "expiration",
			Usage: "The latest epoch by which a provider must commit data before the allocation expires (epochs).\n" +
				"Default is 60 days.",
			Value: verifreg13types.MaximumVerifiedAllocationExpiration,
		},
		&cli.StringFlag{
			Name:    "piece-file",
			Usage:   "file containing piece-info[s] to create the allocation. Each line in the file should be in the format 'pieceCid,pieceSize,miner,tmin,tmax,expiration'",
			Aliases: []string{"pf"},
		},
		&cli.IntFlag{
			Name:  "batch-size",
			Usage: "number of extend requests per batch. If set incorrectly, this will lead to out of gas error",
			Value: 500,
		},
		&cli.IntFlag{
			Name:  "confidence",
			Usage: "number of block confirmations to wait for",
			Value: int(build.MessageConfidence),
		},
		&cli.BoolFlag{
			Name:    "assume-yes",
			Usage:   "automatic yes to prompts; assume 'yes' as answer to all prompts and run non-interactively",
			Aliases: []string{"y", "yes"},
		},
		&cli.StringFlag{
			Name:  "evm-client-contract",
			Usage: "f4 address of EVM contract to spend DataCap from",
		},
	},
	Before: before,
	Action: func(cctx *cli.Context) error {
		ctx := bcli.ReqContext(cctx)

		pieceFile := cctx.String("piece-file")
		miners := cctx.StringSlice("miner")
		pinfos := cctx.StringSlice("piece-info")

		if pieceFile == "" && len(pinfos) < 1 {
			return fmt.Errorf("must provide at least one --piece-info or use --piece-file")
		}

		if pieceFile == "" && len(miners) < 1 {
			return fmt.Errorf("must provide at least one miner address or use --piece-file")
		}

		if pieceFile != "" && len(pinfos) > 0 {
			return fmt.Errorf("cannot use both --piece-info and --piece-file flags at once")
		}

		var pieceInfos []util.PieceInfos

		if pieceFile != "" {
			// Read file line by line
			loc, err := homedir.Expand(pieceFile)
			if err != nil {
				return err
			}
			file, err := os.Open(loc)
			if err != nil {
				return err
			}
			defer func() {
				_ = file.Close()
			}()
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				line := scanner.Text()
				// Extract pieceCid, pieceSize and MinerAddr from line
				parts := strings.Split(line, ",")
				if len(parts) != 6 {
					return fmt.Errorf("invalid line format. Expected pieceCid, pieceSize, MinerAddr, TMin, TMax, Exp at %s", line)
				}
				if parts[0] == "" || parts[1] == "" || parts[2] == "" || parts[3] == "" || parts[4] == "" || parts[5] == "" {
					return fmt.Errorf("empty column value in the input file at %s", line)
				}

				pieceCid, err := cid.Parse(parts[0])
				if err != nil {
					return fmt.Errorf("failed to parse CID: %w", err)
				}
				pieceSize, err := strconv.ParseInt(parts[1], 10, 64)
				if err != nil {
					return fmt.Errorf("failed to parse size %w", err)
				}
				maddr, err := address.NewFromString(parts[2])
				if err != nil {
					return fmt.Errorf("failed to parse miner address %w", err)
				}

				mid, err := address.IDFromAddress(maddr)
				if err != nil {
					return fmt.Errorf("failed to convert miner address %w", err)
				}

				tmin, err := strconv.ParseUint(parts[3], 10, 64)
				if err != nil {
					return fmt.Errorf("failed to tmin %w", err)
				}

				tmax, err := strconv.ParseUint(parts[4], 10, 64)
				if err != nil {
					return fmt.Errorf("failed to tmax %w", err)
				}

				exp, err := strconv.ParseUint(parts[5], 10, 64)
				if err != nil {
					return fmt.Errorf("failed to expiration %w", err)
				}

				if tmax < tmin {
					return fmt.Errorf("maximum duration %d cannot be smaller than minimum duration %d", tmax, tmin)
				}

				pieceInfos = append(pieceInfos, util.PieceInfos{
					Cid:       pieceCid,
					Size:      pieceSize,
					Miner:     abi.ActorID(mid),
					MinerAddr: maddr,
					Tmin:      abi.ChainEpoch(tmin),
					Tmax:      abi.ChainEpoch(tmax),
					Exp:       abi.ChainEpoch(exp),
				})
				if err := scanner.Err(); err != nil {
					return err
				}
			}
		} else {
			for _, miner := range miners {
				maddr, err := address.NewFromString(miner)
				if err != nil {
					return fmt.Errorf("failed to parse miner address %w", err)
				}

				mid, err := address.IDFromAddress(maddr)
				if err != nil {
					return fmt.Errorf("failed to convert miner address %w", err)
				}
				for _, p := range cctx.StringSlice("piece-info") {
					pieceDetail := strings.Split(p, "=")
					if len(pieceDetail) != 2 {
						return fmt.Errorf("incorrect pieceInfo format: %s", pieceDetail)
					}

					size, err := strconv.ParseInt(pieceDetail[1], 10, 64)
					if err != nil {
						return fmt.Errorf("failed to parse the piece size for %s for pieceCid %s: %w", pieceDetail[0], pieceDetail[1], err)
					}
					pcid, err := cid.Parse(pieceDetail[0])
					if err != nil {
						return fmt.Errorf("failed to parse the pieceCid for %s: %w", pieceDetail[0], err)
					}

					tmin := abi.ChainEpoch(cctx.Int64("term-min"))

					tmax := abi.ChainEpoch(cctx.Int64("term-max"))

					exp := abi.ChainEpoch(cctx.Int64("expiration"))

					if tmax < tmin {
						return fmt.Errorf("maximum duration %d cannot be smaller than minimum duration %d", tmax, tmin)
					}

					pieceInfos = append(pieceInfos, util.PieceInfos{
						Cid:       pcid,
						Size:      size,
						Miner:     abi.ActorID(mid),
						MinerAddr: maddr,
						Tmin:      tmin,
						Tmax:      tmax,
						Exp:       exp,
					})
				}
			}
		}

		n, err := clinode.Setup(cctx.String(cmd.FlagRepo.Name))
		if err != nil {
			return err
		}

		gapi, closer, err := lcli.GetGatewayAPIV1(cctx)
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

		var msgs []*types.Message
		var allocationsAddr address.Address
		if cctx.IsSet("evm-client-contract") {
			evmContract := cctx.String("evm-client-contract")
			if evmContract == "" {
				return fmt.Errorf("evm-client-contract can't be empty")
			}
			evmContractAddr, err := address.NewFromString(evmContract)
			if err != nil {
				return err
			}
			allocationsAddr = evmContractAddr
			msgs, err = util.CreateAllocationViaEVMMsg(ctx, gapi, pieceInfos, walletAddr, evmContractAddr, cctx.Int("batch-size"))
			if err != nil {
				return err
			}
		} else {
			allocationsAddr = walletAddr
			msgs, err = util.CreateAllocationMsg(ctx, gapi, pieceInfos, walletAddr, cctx.Int("batch-size"))
			if err != nil {
				return err
			}
		}

		oldallocations, err := gapi.StateGetAllocations(ctx, allocationsAddr, types.EmptyTSK)
		if err != nil {
			return fmt.Errorf("failed to get allocations: %w", err)
		}

		var mcids []cid.Cid

		ds := ds_sync.MutexWrap(datastore.NewMapDatastore())
		for _, msg := range msgs {
			mcid, sent, err := lib.SignAndPushToMpool(cctx, ctx, gapi, n, ds, msg)
			if err != nil {
				return err
			}
			if !sent {
				fmt.Printf("message %s with method %s not sent\n", msg.Cid(), msg.Method.String())
				continue
			}
			mcids = append(mcids, mcid)
		}

		var mcidStr []string
		for _, c := range mcids {
			mcidStr = append(mcidStr, c.String())
		}

		log.Infow("submitted data cap allocation message[s]", "CID", mcidStr)
		log.Info("waiting for message to be included in a block")

		// wait for msgs to get mined into a block
		eg := errgroup.Group{}
		eg.SetLimit(10)
		for _, msg := range mcids {
			m := msg
			eg.Go(func() error {
				wait, err := gapi.StateWaitMsg(ctx, m, uint64(cctx.Int("confidence")), 2000, true)
				if err != nil {
					return fmt.Errorf("timeout waiting for message to land on chain %s", m.String())

				}

				if wait.Receipt.ExitCode.IsError() {
					return fmt.Errorf("failed to execute message %s: %w", m.String(), wait.Receipt.ExitCode)
				}
				return nil
			})
		}
		err = eg.Wait()
		if err != nil {
			return err
		}

		// Return early of quiet flag is set
		if cctx.Bool("quiet") {
			return nil
		}

		newallocations, err := gapi.StateGetAllocations(ctx, allocationsAddr, types.EmptyTSK)
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

		gapi, closer, err := lcli.GetGatewayAPIV1(cctx)
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
		allocMap := make(map[verifreg13types.AllocationId]jalloc, len(allocations))
		for id, allocation := range allocations {
			allocMap[verifreg13types.AllocationId(id)] = jalloc{
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
	UsageText: `Extends claim expiration (TermMax).
If the client is the original client, then the claim can be extended up to a maximum of 5 years, and no Datacap is required.
If the client id different then claim can be extended up to maximum 5 years from now and Datacap is required.
`,
	Flags: []cli.Flag{
		&cli.Int64Flag{
			Name:    "term-max",
			Usage:   "The maximum period for which a provider can earn quality-adjusted power for the piece (epochs). Default is 5 years.",
			Aliases: []string{"tmax"},
			Value:   verifreg13types.MaximumVerifiedAllocationTerm,
		},
		&cli.StringFlag{
			Name:  "wallet",
			Usage: "the wallet address that will used to send the message",
		},
		&cli.BoolFlag{
			Name:  "all",
			Usage: "automatically extend TermMax of all claims for specified miner[s] to --term-max (default: 5 years from claim start epoch)",
		},
		&cli.BoolFlag{
			Name:  "no-datacap",
			Usage: "will only extend the claim expiration without requiring a datacap i.e. made with --wallet address",
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
		&cli.IntFlag{
			Name:  "batch-size",
			Usage: "number of extend requests per batch. If set incorrectly, this will lead to out of gas error",
			Value: 500,
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
		noDatacap := cctx.Bool("no-datacap")

		if !all && noDatacap {
			return fmt.Errorf("can't use --no-datacap flag without --all flag")
		}

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
		if tmax > verifreg13types.MaximumVerifiedAllocationTerm {
			return fmt.Errorf("specified term-max %d is larger than %d maximum allowed by verified regirty actor policy", tmax, verifreg13types.MaximumVerifiedAllocationTerm)
		}

		gapi, closer, err := lcli.GetGatewayAPIV1(cctx)
		if err != nil {
			return fmt.Errorf("can't setup gateway connection: %w", err)
		}
		defer closer()

		ctx := bcli.ReqContext(cctx)
		claimMap := make(map[verifreg13types.ClaimId]util.ProvInfo)

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

				claimMap[verifreg13types.ClaimId(n)] = pi
			}
		}

		// If 1 miner ID and multiple arguments
		if len(miners) == 1 && cctx.Args().Len() > 0 && !all {
			for _, arg := range cctx.Args().Slice() {
				detail := strings.Split(arg, "=")
				if len(detail) > 1 {
					return fmt.Errorf("incorrect argument format %s. Must provide only claim IDs with single miner ID", detail)
				}

				n, err := strconv.ParseInt(detail[0], 10, 64)
				if err != nil {
					return fmt.Errorf("failed to parse the claim ID for %s for argument %s: %w", detail[0], detail, err)
				}

				claimMap[verifreg13types.ClaimId(n)] = util.ProvInfo{}
			}
		}

		// Get wallet address from input
		walletAddr, err := n.GetProvidedOrDefaultWallet(ctx, wallet)
		if err != nil {
			return err
		}

		log.Debugw("selected wallet", "wallet", walletAddr)

		msgs, err := util.CreateExtendClaimMsg(ctx, gapi, claimMap, miners, walletAddr, abi.ChainEpoch(tmax), all, cctx.Bool("assume-yes"), noDatacap, cctx.Int("batch-size"))
		if err != nil {
			return err
		}

		// If not msgs are found then no claims can be extended
		if msgs == nil {
			fmt.Println("No eligible claims found")
			return nil
		}

		var mcids []cid.Cid
		ds := ds_sync.MutexWrap(datastore.NewMapDatastore())
		for _, msg := range msgs {
			mcid, sent, err := lib.SignAndPushToMpool(cctx, ctx, gapi, n, ds, msg)
			if err != nil {
				return err
			}
			if !sent {
				fmt.Printf("message %s with method %s not sent\n", msg.Cid(), msg.Method.String())
				continue
			}
			mcids = append(mcids, mcid)
		}

		// wait for msgs to get mined into a block
		eg := errgroup.Group{}
		eg.SetLimit(10)
		for _, msg := range mcids {
			m := msg
			eg.Go(func() error {
				wait, err := gapi.StateWaitMsg(ctx, m, uint64(cctx.Int("confidence")), 2000, true)
				if err != nil {
					return fmt.Errorf("timeout waiting for message to land on chain %s", m.String())

				}

				if wait.Receipt.ExitCode.IsError() {
					return fmt.Errorf("failed to execute message %s: %w", m.String(), wait.Receipt.ExitCode)
				}
				return nil
			})
		}
		return eg.Wait()
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

		gapi, closer, err := lcli.GetGatewayAPIV1(cctx)
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
			claimMap := make(map[verifreg13types.ClaimId]jclaim, len(claims))
			for id, claim := range claims {
				claimMap[verifreg13types.ClaimId(id)] = jclaim{
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
