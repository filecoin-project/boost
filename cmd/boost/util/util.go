package util

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/builtin"
	"github.com/filecoin-project/go-state-types/builtin/v13/datacap"
	verifreg13 "github.com/filecoin-project/go-state-types/builtin/v13/verifreg"
	verifreg9 "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors"
	datacap2 "github.com/filecoin-project/lotus/chain/actors/builtin/datacap"
	"github.com/filecoin-project/lotus/chain/actors/builtin/verifreg"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	"github.com/manifoldco/promptui"
)

func CreateAllocationRequests(ctx context.Context, api api.Gateway, infos []PieceInfos) (*big.Int, []verifreg9.AllocationRequest, error) {
	var allocationRequests []verifreg9.AllocationRequest
	rDataCap := big.NewInt(0)
	head, err := api.ChainHead(ctx)
	if err != nil {
		return nil, nil, err
	}
	for _, info := range infos {
		minfo, err := api.StateMinerInfo(ctx, info.MinerAddr, types.EmptyTSK)
		if err != nil {
			return nil, nil, err
		}
		if uint64(minfo.SectorSize) < uint64(info.Size) {
			return nil, nil, fmt.Errorf("specified piece size %d is bigger than miner's sector size %s", info.Size, minfo.SectorSize.String())
		}
		allocationRequests = append(allocationRequests, verifreg9.AllocationRequest{
			Provider:   info.Miner,
			Data:       info.Cid,
			Size:       abi.PaddedPieceSize(info.Size),
			TermMin:    info.Tmin,
			TermMax:    info.Tmax,
			Expiration: head.Height() + info.Exp,
		})
		rDataCap.Add(big.NewInt(info.Size).Int, rDataCap.Int)
	}
	return &rDataCap, allocationRequests, nil
}

func CreateAllocationMsg(ctx context.Context, api api.Gateway, infos []PieceInfos, wallet address.Address, batchSize int) ([]*types.Message, error) {
	// Create allocation requests
	rDataCap, allocationRequests, err := CreateAllocationRequests(ctx, api, infos)
	if err != nil {
		return nil, err
	}

	// Get datacap balance
	aDataCap, err := api.StateVerifiedClientStatus(ctx, wallet, types.EmptyTSK)
	if err != nil {
		return nil, err
	}

	if aDataCap == nil {
		return nil, fmt.Errorf("wallet %s does not have any datacap", wallet)
	}

	// Check that we have enough data cap to make the allocation
	if rDataCap.GreaterThan(big.NewInt(aDataCap.Int64())) {
		return nil, fmt.Errorf("requested datacap %s is greater then the available datacap %s", rDataCap, aDataCap)
	}

	// Batch allocationRequests to create message
	var messages []*types.Message
	for i := 0; i < len(allocationRequests); i += batchSize {
		end := i + batchSize
		if end > len(allocationRequests) {
			end = len(allocationRequests)
		}
		batch := allocationRequests[i:end]
		arequest := &verifreg9.AllocationRequests{
			Allocations: batch,
		}
		bDataCap := big.NewInt(0)
		for _, bd := range batch {
			bDataCap.Add(big.NewInt(int64(bd.Size)).Int, bDataCap.Int)
		}

		receiverParams, err := actors.SerializeParams(arequest)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize the parameters: %w", err)
		}

		transferParams, err := actors.SerializeParams(&datacap.TransferParams{
			To:           builtin.VerifiedRegistryActorAddr,
			Amount:       big.Mul(bDataCap, builtin.TokenPrecision),
			OperatorData: receiverParams,
		})

		if err != nil {
			return nil, fmt.Errorf("failed to serialize transfer parameters: %w", err)
		}
		msg := &types.Message{
			To:     builtin.DatacapActorAddr,
			From:   wallet,
			Method: datacap2.Methods.TransferExported,
			Params: transferParams,
			Value:  big.Zero(),
		}
		messages = append(messages, msg)
	}
	return messages, nil
}

type PieceInfos struct {
	Cid       cid.Cid
	Size      int64
	MinerAddr address.Address
	Miner     abi.ActorID
	Tmin      abi.ChainEpoch
	Tmax      abi.ChainEpoch
	Exp       abi.ChainEpoch
}

// CreateExtendClaimMsg creates extend message[s] based on the following conditions
// 1. Extend all claims for a miner ID
// 2. Extend all claims for multiple miner IDs
// 3. Extend specified claims for a miner ID
// 4. Extend specific claims for specific miner ID
// 5. Extend all claims for a miner ID with different client address (2 messages)
// 6. Extend all claims for multiple miner IDs with different client address (2 messages)
// 7. Extend specified claims for a miner ID with different client address (2 messages)
// 8. Extend specific claims for specific miner ID with different client address (2 messages)
func CreateExtendClaimMsg(ctx context.Context, api api.Gateway, pcm map[verifreg13.ClaimId]ProvInfo, miners []string, wallet address.Address, tmax abi.ChainEpoch, all, assumeYes, noDatacap bool, batchSize int) ([]*types.Message, error) {
	ac, err := api.StateLookupID(ctx, wallet, types.EmptyTSK)
	if err != nil {
		return nil, err
	}
	w, err := address.IDFromAddress(ac)
	if err != nil {
		return nil, fmt.Errorf("converting wallet address to ID: %w", err)
	}

	wid := abi.ActorID(w)

	head, err := api.ChainHead(ctx)
	if err != nil {
		return nil, err
	}

	var terms []verifreg13.ClaimTerm
	newClaims := make(map[verifreg13.ClaimExtensionRequest]big.Int)
	rDataCap := big.NewInt(0)

	// If --all is set
	if all {
		for _, id := range miners {
			maddr, err := address.NewFromString(id)
			if err != nil {
				return nil, fmt.Errorf("parsing miner %s: %w", id, err)
			}
			mid, err := address.IDFromAddress(maddr)
			if err != nil {
				return nil, fmt.Errorf("converting miner address to miner ID: %w", err)
			}
			claims, err := api.StateGetClaims(ctx, maddr, types.EmptyTSK)
			if err != nil {
				return nil, fmt.Errorf("getting claims for miner %s: %w", maddr, err)
			}
			for cID, c := range claims {
				claimID := cID
				claim := c
				// If the client is not the original client - burn datacap
				if claim.Client != wid {
					if noDatacap {
						continue
					}
					// The new duration should be greater than the original deal duration and claim should not already be expired
					if head.Height()+tmax-claim.TermStart > claim.TermMax-claim.TermStart && claim.TermStart+claim.TermMax > head.Height() {
						req := verifreg13.ClaimExtensionRequest{
							Claim:    verifreg13.ClaimId(claimID),
							Provider: abi.ActorID(mid),
							TermMax:  head.Height() + tmax - claim.TermStart,
						}
						newClaims[req] = big.NewInt(int64(claim.Size))
						rDataCap.Add(big.NewInt(int64(claim.Size)).Int, rDataCap.Int)
					}
					// If new duration shorter than the original duration then do nothing
					continue
				}
				// For original client, compare duration(TermMax) and claim should not already be expired
				if claim.TermMax < tmax && claim.TermStart+claim.TermMax > head.Height() {
					terms = append(terms, verifreg13.ClaimTerm{
						ClaimId:  verifreg13.ClaimId(claimID),
						TermMax:  tmax,
						Provider: abi.ActorID(mid),
					})
				}
			}
		}
	}

	// Single miner and specific claims
	if len(miners) == 1 && len(pcm) > 0 {
		maddr, err := address.NewFromString(miners[0])
		if err != nil {
			return nil, fmt.Errorf("parsing miner %s: %w", miners[0], err)
		}
		mid, err := address.IDFromAddress(maddr)
		if err != nil {
			return nil, fmt.Errorf("converting miner address to miner ID: %w", err)
		}
		claims, err := api.StateGetClaims(ctx, maddr, types.EmptyTSK)
		if err != nil {
			return nil, fmt.Errorf("getting claims for miner %s: %w", maddr, err)
		}

		for cID := range pcm {
			claimID := cID
			claim, ok := claims[verifreg9.ClaimId(claimID)]
			if !ok {
				return nil, fmt.Errorf("claim %d not found for provider %s", claimID, miners[0])
			}
			// If the client is not the original client - burn datacap
			if claim.Client != wid {
				// The new duration should be greater than the original deal duration and claim should not already be expired
				if head.Height()+tmax-claim.TermStart > claim.TermMax-claim.TermStart && claim.TermStart+claim.TermMax > head.Height() {
					req := verifreg13.ClaimExtensionRequest{
						Claim:    claimID,
						Provider: abi.ActorID(mid),
						TermMax:  head.Height() + tmax - claim.TermStart,
					}
					newClaims[req] = big.NewInt(int64(claim.Size))
					rDataCap.Add(big.NewInt(int64(claim.Size)).Int, rDataCap.Int)
				}
				// If new duration shorter than the original duration then do nothing
				continue
			}
			// For original client, compare duration(TermMax) and claim should not already be expired
			if claim.TermMax < tmax && claim.TermStart+claim.TermMax > head.Height() {
				terms = append(terms, verifreg13.ClaimTerm{
					ClaimId:  claimID,
					TermMax:  tmax,
					Provider: abi.ActorID(mid),
				})
			}
		}
	}

	if len(miners) == 0 && len(pcm) > 0 {
		for claimID, prov := range pcm {
			prov := prov
			claimID := claimID
			claim, err := api.StateGetClaim(ctx, prov.Addr, verifreg9.ClaimId(claimID), types.EmptyTSK)
			if err != nil {
				return nil, fmt.Errorf("could not load the claim %d: %w", claimID, err)
			}
			if claim == nil {
				return nil, fmt.Errorf("claim %d not found for provider %s", claimID, prov.Addr)
			}
			// If the client is not the original client - burn datacap
			if claim.Client != wid {
				// The new duration should be greater than the original deal duration and claim should not already be expired
				if head.Height()+tmax-claim.TermStart > claim.TermMax-claim.TermStart && claim.TermStart+claim.TermMax > head.Height() {
					req := verifreg13.ClaimExtensionRequest{
						Claim:    claimID,
						Provider: prov.ID,
						TermMax:  head.Height() + tmax - claim.TermStart,
					}
					newClaims[req] = big.NewInt(int64(claim.Size))
					rDataCap.Add(big.NewInt(int64(claim.Size)).Int, rDataCap.Int)
				}
				// If new duration shorter than the original duration then do nothing
				continue
			}
			// For original client, compare duration(TermMax) and claim should not already be expired
			if claim.TermMax < tmax && claim.TermStart+claim.TermMax > head.Height() {
				terms = append(terms, verifreg13.ClaimTerm{
					ClaimId:  claimID,
					TermMax:  tmax,
					Provider: prov.ID,
				})
			}
		}
	}

	var msgs []*types.Message

	if len(terms) > 0 {
		for i := 0; i < len(terms); i += batchSize {
			batchEnd := i + batchSize
			if batchEnd > len(terms) {
				batchEnd = len(terms)
			}

			batch := terms[i:batchEnd]

			params, err := actors.SerializeParams(&verifreg13.ExtendClaimTermsParams{
				Terms: batch,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to searialise the parameters: %w", err)
			}
			oclaimMsg := &types.Message{
				To:     verifreg.Address,
				From:   wallet,
				Method: verifreg.Methods.ExtendClaimTerms,
				Params: params,
			}
			msgs = append(msgs, oclaimMsg)
		}
	}

	if len(newClaims) > 0 {
		// Get datacap balance
		aDataCap, err := api.StateVerifiedClientStatus(ctx, wallet, types.EmptyTSK)
		if err != nil {
			return nil, err
		}

		if aDataCap == nil {
			return nil, fmt.Errorf("wallet %s does not have any datacap", wallet)
		}

		// Check that we have enough data cap to make the allocation
		if rDataCap.GreaterThan(big.NewInt(aDataCap.Int64())) {
			return nil, fmt.Errorf("requested datacap %s is greater then the available datacap %s", rDataCap, aDataCap)
		}

		if !assumeYes {
			out := fmt.Sprintf("Some of the specified allocation have a different client address and will require %d Datacap to extend. Proceed? Yes [Y/y] / No [N/n], Ctrl+C (^C) to exit", rDataCap.Int)
			validate := func(input string) error {
				if strings.EqualFold(input, "y") || strings.EqualFold(input, "yes") {
					return nil
				}
				if strings.EqualFold(input, "n") || strings.EqualFold(input, "no") {
					return nil
				}
				return errors.New("incorrect input")
			}

			templates := &promptui.PromptTemplates{
				Prompt:  "{{ . }} ",
				Valid:   "{{ . | green }} ",
				Invalid: "{{ . | red }} ",
				Success: "{{ . | cyan | bold }} ",
			}

			prompt := promptui.Prompt{
				Label:     out,
				Templates: templates,
				Validate:  validate,
			}

			input, err := prompt.Run()
			if err != nil {
				return nil, err
			}
			if strings.Contains(strings.ToLower(input), "n") {
				fmt.Println("Dropping the extension for claims that require Datacap")
				return msgs, nil
			}
		}

		// Create a map of just keys, so we can easily batch based on the numeric keys
		keys := make([]verifreg13.ClaimExtensionRequest, 0, len(newClaims))
		for k := range newClaims {
			keys = append(keys, k)
		}

		// Batch in 500 to avoid running out of gas
		for i := 0; i < len(keys); i += batchSize {
			batchEnd := i + batchSize
			if batchEnd > len(keys) {
				batchEnd = len(keys)
			}

			batch := keys[i:batchEnd]

			// Calculate Datacap for this batch
			dcap := big.NewInt(0)
			for _, k := range batch {
				dc := newClaims[k]
				dcap.Add(dcap.Int, dc.Int)
			}

			ncparams, err := actors.SerializeParams(&verifreg13.AllocationRequests{
				Extensions: batch,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to searialise the parameters: %w", err)
			}

			transferParams, err := actors.SerializeParams(&datacap.TransferParams{
				To:           builtin.VerifiedRegistryActorAddr,
				Amount:       big.Mul(dcap, builtin.TokenPrecision),
				OperatorData: ncparams,
			})

			if err != nil {
				return nil, fmt.Errorf("failed to serialize transfer parameters: %w", err)
			}

			nclaimMsg := &types.Message{
				To:     builtin.DatacapActorAddr,
				From:   wallet,
				Method: datacap2.Methods.TransferExported,
				Params: transferParams,
				Value:  big.Zero(),
			}
			msgs = append(msgs, nclaimMsg)
		}
	}

	return msgs, nil
}

type ProvInfo struct {
	Addr address.Address
	ID   abi.ActorID
}
