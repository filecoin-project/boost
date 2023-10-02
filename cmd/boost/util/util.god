package util

import (
	"context"
	"fmt"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/builtin"
	"github.com/filecoin-project/go-state-types/builtin/v9/datacap"
	"github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	"github.com/filecoin-project/lotus/api"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors"
	datacap2 "github.com/filecoin-project/lotus/chain/actors/builtin/datacap"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	"strconv"
	"strings"
)

func CreateAllocationMsg(ctx context.Context, api api.Gateway, pInfos, miners []string, wallet address.Address, tmin, tmax, exp abi.ChainEpoch) (*types.Message, error) {
	// Get all minerIDs from input
	maddrs := make(map[abi.ActorID]lapi.MinerInfo)
	minerIds := miners
	for _, id := range minerIds {
		maddr, err := address.NewFromString(id)
		if err != nil {
			return nil, err
		}

		// Verify that minerID exists
		m, err := api.StateMinerInfo(ctx, maddr, types.EmptyTSK)
		if err != nil {
			return nil, err
		}

		mid, err := address.IDFromAddress(maddr)
		if err != nil {
			return nil, err
		}

		maddrs[abi.ActorID(mid)] = m
	}

	// Get all pieceCIDs from input
	rDataCap := big.NewInt(0)
	var pieceInfos []*abi.PieceInfo
	pieces := pInfos
	for _, p := range pieces {
		pieceDetail := strings.Split(p, "=")
		if len(pieceDetail) > 2 {
			return nil, fmt.Errorf("incorrect pieceInfo format: %s", pieceDetail)
		}

		n, err := strconv.ParseInt(pieceDetail[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse the piece size for %s for pieceCid %s: %w", pieceDetail[0], pieceDetail[1], err)
		}
		pcid, err := cid.Parse(pieceDetail[0])
		if err != nil {
			return nil, fmt.Errorf("failed to parse the pieceCid for %s: %w", pieceDetail[0], err)
		}

		pieceInfos = append(pieceInfos, &abi.PieceInfo{
			Size:     abi.PaddedPieceSize(n),
			PieceCID: pcid,
		})
		rDataCap.Add(big.NewInt(n).Int, rDataCap.Int)
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

	if tmax < tmin {
		return nil, fmt.Errorf("maximum duration %d cannot be smaller than minimum duration %d", tmax, tmin)
	}

	head, err := api.ChainHead(ctx)
	if err != nil {
		return nil, err
	}

	if tmax < head.Height() || tmin < head.Height() {
		return nil, fmt.Errorf("current chain head %d is greater than TermMin %d or TermMax %d", head.Height(), tmin, tmax)
	}

	// Create allocation requests
	var allocationRequests []verifreg.AllocationRequest
	for mid, minfo := range maddrs {
		for _, p := range pieceInfos {
			if uint64(minfo.SectorSize) < uint64(p.Size) {
				return nil, fmt.Errorf("specified piece size %d is bigger than miner's sector size %s", uint64(p.Size), minfo.SectorSize.String())
			}
			allocationRequests = append(allocationRequests, verifreg.AllocationRequest{
				Provider:   mid,
				Data:       p.PieceCID,
				Size:       p.Size,
				TermMin:    tmin,
				TermMax:    tmax,
				Expiration: exp,
			})
		}
	}

	arequest := &verifreg.AllocationRequests{
		Allocations: allocationRequests,
	}

	receiverParams, err := actors.SerializeParams(arequest)
	if err != nil {
		return nil, fmt.Errorf("failed to seralize the parameters: %w", err)
	}

	transferParams, err := actors.SerializeParams(&datacap.TransferParams{
		To:           builtin.VerifiedRegistryActorAddr,
		Amount:       big.Mul(rDataCap, builtin.TokenPrecision),
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

	return msg, nil
}
