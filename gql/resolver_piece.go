package gql

import (
	"context"
	"fmt"
	"time"

	gqltypes "github.com/filecoin-project/boost/gql/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/boostd-data/svc/types"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	"github.com/graph-gophers/graphql-go"
	"github.com/ipfs/go-cid"
)

type IndexStatus string

const (
	IndexStatusUnknown    IndexStatus = ""
	IndexStatusNotFound   IndexStatus = "NotFound"
	IndexStatusRegistered IndexStatus = "Registered"
	IndexStatusIndexing   IndexStatus = "Indexing"
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

type flaggedPieceResolver struct {
	Piece     *pieceResolver
	CreatedAt graphql.Time
}

type piecesFlaggedArgs struct {
	Cursor *gqltypes.BigInt // CreatedAt in milli-seconds
	Offset graphql.NullInt
	Limit  graphql.NullInt
}

type flaggedPieceListResolver struct {
	TotalCount int32
	Pieces     []*flaggedPieceResolver
	More       bool
}

func (r *resolver) PiecesFlagged(ctx context.Context, args piecesFlaggedArgs) (*flaggedPieceListResolver, error) {
	offset := 0
	if args.Offset.Set && args.Offset.Value != nil && *args.Offset.Value > 0 {
		offset = int(*args.Offset.Value)
	}

	limit := 10
	if args.Limit.Set && args.Limit.Value != nil && *args.Limit.Value > 0 {
		limit = int(*args.Limit.Value)
	}

	// Fetch one extra row so that we can check if there are more rows
	// beyond the limit
	cursor := bigIntToTime(args.Cursor)
	flaggedPieces, err := r.piecedirectory.FlaggedPiecesList(ctx, cursor, offset, limit+1)
	if err != nil {
		return nil, err
	}
	more := len(flaggedPieces) > limit
	if more {
		// Truncate list to limit
		flaggedPieces = flaggedPieces[:limit]
	}

	// Get the total row count
	count, err := r.piecedirectory.FlaggedPiecesCount(ctx)
	if err != nil {
		return nil, err
	}

	allLegacyDeals, err := r.legacyProv.ListLocalDeals()
	if err != nil {
		return nil, err
	}

	flaggedPieceResolvers := make([]*flaggedPieceResolver, 0, len(flaggedPieces))
	for _, flaggedPiece := range flaggedPieces {
		pieceResolver, err := r.pieceStatus(ctx, flaggedPiece.PieceCid, allLegacyDeals)
		if err != nil {
			return nil, err
		}
		flaggedPieceResolvers = append(flaggedPieceResolvers, &flaggedPieceResolver{
			Piece:     pieceResolver,
			CreatedAt: graphql.Time{Time: flaggedPiece.CreatedAt},
		})
	}

	return &flaggedPieceListResolver{
		TotalCount: int32(count),
		Pieces:     flaggedPieceResolvers,
		More:       more,
	}, nil
}

func (r *resolver) PiecesWithPayloadCid(ctx context.Context, args struct{ PayloadCid string }) ([]string, error) {
	payloadCid, err := cid.Parse(args.PayloadCid)
	if err != nil {
		return nil, fmt.Errorf("%s is not a valid payload cid", args.PayloadCid)
	}

	pieces, err := r.piecedirectory.PiecesContainingMultihash(ctx, payloadCid.Hash())
	if err != nil {
		if types.IsNotFound(err) {
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

func (r *resolver) PieceBuildIndex(args struct{ PieceCid string }) (bool, error) {
	pieceCid, err := cid.Parse(args.PieceCid)
	if err != nil {
		return false, fmt.Errorf("%s is not a valid piece cid", args.PieceCid)
	}

	// Use the global boost context for build piece, because if the user
	// navigates away from the page we don't want to cancel the build piece
	// operation
	err = r.piecedirectory.BuildIndexForPiece(r.ctx, pieceCid)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (r *resolver) PieceStatus(ctx context.Context, args struct{ PieceCid string }) (*pieceResolver, error) {
	pieceCid, err := cid.Parse(args.PieceCid)
	if err != nil {
		return nil, fmt.Errorf("%s is not a valid piece cid", args.PieceCid)
	}

	allLegacyDeals, err := r.legacyProv.ListLocalDeals()
	if err != nil {
		return nil, err
	}

	return r.pieceStatus(ctx, pieceCid, allLegacyDeals)
}

func (r *resolver) pieceStatus(ctx context.Context, pieceCid cid.Cid, allLegacyDeals []storagemarket.MinerDeal) (*pieceResolver, error) {
	// Get piece info from local index directory
	pieceInfo, err := r.piecedirectory.GetPieceMetadata(ctx, pieceCid)
	if err != nil && !types.IsNotFound(err) {
		return nil, err
	}

	// Get boost deals by piece Cid
	boostDeals, err := r.dealsDB.ByPieceCID(ctx, pieceCid)
	if err != nil {
		return nil, err
	}

	// Get legacy markets deals by piece Cid
	var legacyDeals []storagemarket.MinerDeal
	for _, dl := range allLegacyDeals {
		if dl.Ref.PieceCid != nil && *dl.Ref.PieceCid == pieceCid {
			legacyDeals = append(legacyDeals, dl)
		}
	}

	// Convert local index directory deals to graphQL format
	var pids []*pieceInfoDeal
	for _, dl := range pieceInfo.Deals {
		// Check the sealing status of each deal
		errMsg := ""
		isUnsealed, err := r.isUnsealed(ctx, dl.SectorID, dl.PieceOffset.Unpadded(), dl.PieceLength.Unpadded())
		if err != nil {
			errMsg = err.Error()
		}

		pids = append(pids, &pieceInfoDeal{
			SealStatus: &sealStatus{
				IsUnsealed: isUnsealed,
				Error:      errMsg,
			},
			ChainDealID: gqltypes.Uint64(dl.ChainDealID),
			Sector: &sectorResolver{
				ID:     gqltypes.Uint64(dl.SectorID),
				Offset: gqltypes.Uint64(dl.PieceOffset),
				Length: gqltypes.Uint64(dl.PieceLength),
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

		// Only check the unseal state if the deal has already been added to a piece
		st := &sealStatus{IsUnsealed: false}
		if dl.Checkpoint >= dealcheckpoints.AddedPiece {
			isUnsealed, err := r.isUnsealed(ctx, dl.SectorID, dl.Offset.Unpadded(), dl.Length.Unpadded())
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
			size := abi.PaddedPieceSize(sector.Length).Unpadded()
			isUnsealed, err := r.isUnsealed(ctx, secID, offset, size)
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

	// Get the state of the piece's index
	idxStatus, err := r.getIndexStatus(ctx, pieceCid, deals)
	if err != nil {
		return nil, err
	}

	return &pieceResolver{
		PieceCid:       pieceCid.String(),
		IndexStatus:    idxStatus,
		PieceInfoDeals: pids,
		Deals:          deals,
	}, nil
}

func (r *resolver) getIndexStatus(ctx context.Context, pieceCid cid.Cid, deals []*pieceDealResolver) (*indexStatus, error) {
	var idxst IndexStatus
	idxerr := ""

	md, err := r.piecedirectory.GetPieceMetadata(ctx, pieceCid)
	switch {
	case err != nil && types.IsNotFound(err):
		idxst = IndexStatusNotFound
	case err != nil:
		idxst = IndexStatusFailed
		idxerr = err.Error()
	case md.Indexing:
		idxst = IndexStatusIndexing
	case md.Error != "":
		idxst = IndexStatusFailed
		idxerr = md.Error
	case md.IndexedAt.IsZero():
		idxst = IndexStatusRegistered
	default:
		idxst = IndexStatusComplete
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
		pieces, err := r.piecedirectory.PiecesContainingMultihash(ctx, c.Hash())
		if err != nil || len(pieces) == 0 {
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

const isUnsealedTimeout = 5 * time.Second

func (r *resolver) isUnsealed(ctx context.Context, sectorID abi.SectorNumber, offset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (bool, error) {
	isUnsealedCtx, cancel := context.WithTimeout(ctx, isUnsealedTimeout)
	defer cancel()
	isUnsealed, err := r.sa.IsUnsealed(isUnsealedCtx, sectorID, offset, length)
	if err != nil && isUnsealedCtx.Err() != nil {
		e := fmt.Errorf("IsUnsealed: timed out after %s (IsUnsealed blocks if the sector is currently being unsealed)",
			isUnsealedTimeout)
		return isUnsealed, e
	}

	return isUnsealed, err
}
