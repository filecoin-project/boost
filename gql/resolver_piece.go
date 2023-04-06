package gql

import (
	"context"
	"errors"
	"fmt"

	"github.com/filecoin-project/boost-gfm/retrievalmarket"
	"github.com/filecoin-project/boost-gfm/storagemarket"
	gqltypes "github.com/filecoin-project/boost/gql/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/dagstore"
	"github.com/filecoin-project/dagstore/shard"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	"github.com/graph-gophers/graphql-go"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
)

type IndexStatus string

const (
	IndexStatusUnknown    IndexStatus = ""
	IndexStatusNotFound   IndexStatus = "NotFound"
	IndexStatusRegistered IndexStatus = "Registered"
	IndexStatusComplete   IndexStatus = "Complete"
	IndexStatusFailed     IndexStatus = "Failed"
)

type pieceDealResolver struct {
	Deal       *basicDealResolver
	Sector     *sectorResolver
	SealStatus *sealStatus
}

type sealStatus struct {
	IsUnsealed bool
	Error      string
}

type pieceInfoDeal struct {
	ChainDealID gqltypes.Uint64
	Sector      *sectorResolver
	SealStatus  *sealStatus
}

type indexStatus struct {
	Status string
	Error  string
}

type pieceResolver struct {
	PieceCid       string
	IndexStatus    *indexStatus
	Deals          []*pieceDealResolver
	PieceInfoDeals []*pieceInfoDeal
}

func (r *resolver) PiecesWithPayloadCid(ctx context.Context, args struct{ PayloadCid string }) ([]string, error) {
	payloadCid, err := cid.Parse(args.PayloadCid)
	if err != nil {
		return nil, fmt.Errorf("%s is not a valid payload cid", args.PayloadCid)
	}

	pieces, err := r.dagst.ShardsContainingMultihash(ctx, payloadCid.Hash())
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			return []string{}, nil
		}
		return nil, fmt.Errorf("getting shards containing cid %s: %w", payloadCid, err)
	}

	pieceCids := make([]string, 0, len(pieces))
	for _, piece := range pieces {
		pieceCids = append(pieceCids, piece.String())
	}
	return pieceCids, nil
}

func (r *resolver) PiecesWithRootPayloadCid(ctx context.Context, args struct{ PayloadCid string }) ([]string, error) {
	payloadCid, err := cid.Parse(args.PayloadCid)
	if err != nil {
		return nil, fmt.Errorf("%s is not a valid payload cid", args.PayloadCid)
	}

	var pieceCidSet = make(map[string]struct{})

	// Get boost deals by payload cid
	boostDeals, err := r.dealsDB.ByRootPayloadCID(ctx, payloadCid)
	if err != nil {
		return nil, err
	}
	for _, dl := range boostDeals {
		pieceCidSet[dl.ClientDealProposal.Proposal.PieceCID.String()] = struct{}{}
	}

	// Get legacy markets deals by payload cid
	// TODO: add method to markets to filter deals by payload CID
	allLegacyDeals, err := r.legacyProv.ListLocalDeals()
	if err != nil {
		return nil, err
	}
	for _, dl := range allLegacyDeals {
		if dl.Ref.Root == payloadCid {
			pieceCidSet[dl.ClientDealProposal.Proposal.PieceCID.String()] = struct{}{}
		}
	}

	pieceCids := make([]string, 0, len(pieceCidSet))
	for pieceCid := range pieceCidSet {
		pieceCids = append(pieceCids, pieceCid)
	}
	return pieceCids, nil
}

func (r *resolver) PieceStatus(ctx context.Context, args struct{ PieceCid string }) (*pieceResolver, error) {
	pieceCid, err := cid.Parse(args.PieceCid)
	if err != nil {
		return nil, fmt.Errorf("%s is not a valid piece cid", args.PieceCid)
	}

	// Get sector info from PieceStore
	pieceInfo, err := r.ps.GetPieceInfo(pieceCid)
	if err != nil && !errors.Is(err, retrievalmarket.ErrNotFound) {
		return nil, err
	}

	// Get boost deals by piece Cid
	boostDeals, err := r.dealsDB.ByPieceCID(ctx, pieceCid)
	if err != nil {
		return nil, err
	}

	// Get legacy markets deals by piece Cid
	// TODO: add method to markets to filter deals by piece CID
	allLegacyDeals, err := r.legacyProv.ListLocalDeals()
	if err != nil {
		return nil, err
	}
	var legacyDeals []storagemarket.MinerDeal
	for _, dl := range allLegacyDeals {
		if dl.Ref.PieceCid != nil && *dl.Ref.PieceCid == pieceCid {
			legacyDeals = append(legacyDeals, dl)
		}
	}

	// Convert piece info deals to graphQL format
	var pids []*pieceInfoDeal
	for _, dl := range pieceInfo.Deals {
		// Check the sealing status of each deal
		errMsg := ""
		isUnsealed, err := r.sa.IsUnsealed(ctx, dl.SectorID, dl.Offset.Unpadded(), dl.Length.Unpadded())
		if err != nil {
			errMsg = err.Error()
		}

		pids = append(pids, &pieceInfoDeal{
			SealStatus: &sealStatus{
				IsUnsealed: isUnsealed,
				Error:      errMsg,
			},
			ChainDealID: gqltypes.Uint64(dl.DealID),
			Sector: &sectorResolver{
				ID:     gqltypes.Uint64(dl.SectorID),
				Offset: gqltypes.Uint64(dl.Offset),
				Length: gqltypes.Uint64(dl.Length),
			},
		})
	}

	// Convert boost deals to graphQL format
	deals := make([]*pieceDealResolver, 0, len(boostDeals)+len(legacyDeals))
	for _, dl := range boostDeals {
		bd := propToBasicDeal(dl.ClientDealProposal.Proposal)
		bd.IsLegacy = false
		bd.ID = graphql.ID(dl.DealUuid.String())
		bd.CreatedAt = graphql.Time{Time: dl.CreatedAt}
		bd.ClientPeerID = dl.ClientPeerID.String()
		bd.DealDataRoot = dl.DealDataRoot.String()
		bd.PublishCid = cidToString(dl.PublishCID)
		bd.Transfer = dealTransfer{
			Type: dl.Transfer.Type,
			Size: gqltypes.Uint64(dl.Transfer.Size),
		}
		bd.Message = dl.Checkpoint.String()

		// Only check the unseal state if the deal has already been added to a sector
		st := &sealStatus{IsUnsealed: false}
		if dl.Checkpoint >= dealcheckpoints.AddedPiece {
			isUnsealed, err := r.sa.IsUnsealed(ctx, dl.SectorID, dl.Offset.Unpadded(), dl.Length.Unpadded())
			if err != nil {
				st.Error = err.Error()
			}
			st.IsUnsealed = isUnsealed
		}

		deals = append(deals, &pieceDealResolver{
			Deal:       &bd,
			SealStatus: st,
			Sector: &sectorResolver{
				ID:     gqltypes.Uint64(dl.SectorID),
				Offset: gqltypes.Uint64(dl.Offset),
				Length: gqltypes.Uint64(dl.Length),
			},
		})
	}

	// Convert legacy deals to graphQL format
	for _, dl := range legacyDeals {
		bd := propToBasicDeal(dl.Proposal)
		bd.IsLegacy = true
		bd.ID = graphql.ID(dl.ProposalCid.String())
		bd.CreatedAt = graphql.Time{Time: dl.CreationTime.Time()}
		bd.ClientPeerID = dl.Client.String()
		bd.DealDataRoot = dl.Ref.Root.String()
		bd.PublishCid = cidToString(dl.PublishCid)
		bd.Transfer = dealTransfer{
			Type: "graphsync",
			Size: gqltypes.Uint64(dl.Ref.RawBlockSize),
		}
		bd.Message = dl.Message

		// For legacy deals the sector information is stored in the piece store
		sector := r.getLegacyDealSector(ctx, pids, dl.DealID)

		st := &sealStatus{IsUnsealed: false}
		if sector == nil {
			sector = &sectorResolver{ID: gqltypes.Uint64(dl.SectorNumber)}
		} else {
			secID := abi.SectorNumber(sector.ID)
			offset := abi.PaddedPieceSize(sector.Offset).Unpadded()
			len := abi.PaddedPieceSize(sector.Length).Unpadded()
			isUnsealed, err := r.sa.IsUnsealed(ctx, secID, offset, len)
			st = &sealStatus{IsUnsealed: isUnsealed}
			if err != nil {
				st.Error = err.Error()
			}
		}

		deals = append(deals, &pieceDealResolver{
			Deal:       &bd,
			Sector:     sector,
			SealStatus: st,
		})
	}

	// Get the state of the piece in the DAG store
	idxStatus, err := r.getIndexStatus(ctx, pieceCid, deals)
	if err != nil {
		return nil, err
	}

	return &pieceResolver{
		PieceCid:       args.PieceCid,
		IndexStatus:    idxStatus,
		PieceInfoDeals: pids,
		Deals:          deals,
	}, nil
}

func (r *resolver) getIndexStatus(ctx context.Context, pieceCid cid.Cid, deals []*pieceDealResolver) (*indexStatus, error) {
	si, err := r.dagst.GetShardInfo(shard.KeyFromCID(pieceCid))
	if err != nil && !errors.Is(err, dagstore.ErrShardUnknown) {
		return nil, err
	}
	idxst := IndexStatusUnknown
	idxerr := ""
	switch {
	case err != nil && errors.Is(err, dagstore.ErrShardUnknown):
		idxst = IndexStatusNotFound
	case si.ShardState == dagstore.ShardStateNew, si.ShardState == dagstore.ShardStateInitializing:
		idxst = IndexStatusRegistered
	case si.ShardState == dagstore.ShardStateAvailable, si.ShardState == dagstore.ShardStateServing:
		idxst = IndexStatusComplete
	case si.ShardState == dagstore.ShardStateErrored, si.ShardState == dagstore.ShardStateRecovering:
		idxst = IndexStatusFailed
		if si.Error != nil {
			idxerr = si.Error.Error()
		}
	}

	// Try retrieving the piece payload cid as a means to check if the
	// payload cid => piece cid index has been created correctly
	if idxst == IndexStatusComplete && len(deals) > 0 {
		cidstr := deals[0].Deal.DealDataRoot
		c, err := cid.Parse(cidstr)
		if err != nil {
			// This should never happen, but check just in case
			return nil, fmt.Errorf("parsing retrieved deal data root cid %s: %w", cidstr, err)
		}
		ks, err := r.dagst.ShardsContainingMultihash(ctx, c.Hash())
		if err != nil || len(ks) == 0 {
			idxst = IndexStatusFailed
			idxerr = fmt.Sprintf("unable to resolve piece's root payload cid %s to piece cid", cidstr)
		}
	}

	return &indexStatus{Status: string(idxst), Error: idxerr}, nil
}

func cidToString(c *cid.Cid) string {
	cstr := ""
	if c != nil {
		cstr = c.String()
	}
	return cstr
}

func propToBasicDeal(prop market.DealProposal) basicDealResolver {
	return basicDealResolver{
		ClientAddress:      prop.Client.String(),
		ProviderAddress:    prop.Provider.String(),
		PieceCid:           prop.PieceCID.String(),
		PieceSize:          gqltypes.Uint64(prop.PieceSize),
		ProviderCollateral: gqltypes.Uint64(prop.ProviderCollateral.Uint64()),
		StartEpoch:         gqltypes.Uint64(prop.StartEpoch),
		EndEpoch:           gqltypes.Uint64(prop.EndEpoch),
	}
}

// Get the index status of the deal with the given on-chain ID from the piece info data
func (r *resolver) getLegacyDealSector(ctx context.Context, pids []*pieceInfoDeal, chainDealId abi.DealID) *sectorResolver {
	for _, pid := range pids {
		if abi.DealID(pid.ChainDealID) != chainDealId {
			continue
		}

		return pid.Sector
	}
	return nil
}
