package gql

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/filecoin-project/boost/cmd/lib"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/extern/boostd-data/svc/types"
	gqltypes "github.com/filecoin-project/boost/gql/types"
	pdtypes "github.com/filecoin-project/boost/piecedirectory/types"
	"github.com/filecoin-project/boost/sectorstatemgr"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	"github.com/graph-gophers/graphql-go"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"golang.org/x/sync/errgroup"
)

type IndexStatus string

const (
	IndexStatusNotFound   IndexStatus = "NotFound"
	IndexStatusRegistered IndexStatus = "Registered"
	IndexStatusIndexing   IndexStatus = "Indexing"
	IndexStatusComplete   IndexStatus = "Complete"
	IndexStatusFailed     IndexStatus = "Failed"
)

type SealedStatus string

const (
	SealedStatusUnknown         SealedStatus = "Unknown"
	SealedStatusError           SealedStatus = "Error"
	SealedStatusHasUnsealedCopy SealedStatus = "HasUnsealedCopy"
	SealedStatusNoUnsealedCopy  SealedStatus = "NoUnsealedCopy"
)

type sealStatusResolver struct {
	Status string
	Error  string
}

type pieceDealResolver struct {
	Deal   *basicDealResolver
	Sector *sectorResolver
	ss     *sealStatusReporter
}

type piecePayload struct {
	PayloadCid string
	Multihash  string
}

func (pdr *pieceDealResolver) SealStatus(ctx context.Context) *sealStatusResolver {
	return pdr.ss.sealStatus(ctx)
}

type pieceInfoDeal struct {
	MinerAddress string
	ChainDealID  gqltypes.Uint64
	Sector       *sectorResolver
	ss           *sealStatusReporter
}

func (pid *pieceInfoDeal) SealStatus(ctx context.Context) *sealStatusResolver {
	return pid.ss.sealStatus(ctx)
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
	MinerAddr   string
	PieceCid    string
	IndexStatus *indexStatus
	DealCount   int32
	CreatedAt   graphql.Time
}

type piecesFlaggedArgs struct {
	MinerAddr       graphql.NullString
	HasUnsealedCopy graphql.NullBool
	Cursor          *gqltypes.BigInt // CreatedAt in milli-seconds
	Offset          graphql.NullInt
	Limit           graphql.NullInt
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

	var filter *types.FlaggedPiecesListFilter
	if args.MinerAddr.Set && args.MinerAddr.Value != nil {
		maddr, err := address.NewFromString(*args.MinerAddr.Value)
		if err != nil {
			return nil, fmt.Errorf("parsing miner address '%s': %w", *args.MinerAddr.Value, err)
		}
		filter = &types.FlaggedPiecesListFilter{MinerAddr: maddr}
	}
	if args.HasUnsealedCopy.Set && args.HasUnsealedCopy.Value != nil {
		if filter == nil {
			filter = &types.FlaggedPiecesListFilter{}
		}
		filter.HasUnsealedCopy = *args.HasUnsealedCopy.Value
	}

	// Fetch one extra row so that we can check if there are more rows
	// beyond the limit
	cursor := bigIntToTime(args.Cursor)
	flaggedPieces, err := r.piecedirectory.FlaggedPiecesList(ctx, filter, cursor, offset, limit+1)
	if err != nil {
		return nil, err
	}
	more := len(flaggedPieces) > limit
	if more {
		// Truncate list to limit
		flaggedPieces = flaggedPieces[:limit]
	}

	// Get the total row count
	count, err := r.piecedirectory.FlaggedPiecesCount(ctx, filter)
	if err != nil {
		return nil, err
	}

	var eg errgroup.Group
	flaggedPieceResolvers := make([]*flaggedPieceResolver, 0, len(flaggedPieces))
	for _, flaggedPiece := range flaggedPieces {
		flaggedPiece := flaggedPiece
		eg.Go(func() error {
			// Get piece info from local index directory
			pieceInfo, pmErr := r.piecedirectory.GetPieceMetadata(ctx, flaggedPiece.PieceCid)
			if pmErr != nil && !types.IsNotFound(pmErr) {
				return pmErr
			}

			// Get the state of the piece's index
			idxStatus, err := r.getIndexStatus(pieceInfo, pmErr)
			if err != nil {
				return err
			}

			flaggedPieceResolvers = append(flaggedPieceResolvers, &flaggedPieceResolver{
				MinerAddr:   flaggedPiece.MinerAddr.String(),
				PieceCid:    flaggedPiece.PieceCid.String(),
				IndexStatus: idxStatus,
				DealCount:   int32(len(pieceInfo.Deals)),
				CreatedAt:   graphql.Time{Time: flaggedPiece.CreatedAt},
			})
			return nil
		})
	}
	err = eg.Wait()
	if err != nil {
		return nil, err
	}

	sort.Slice(flaggedPieceResolvers, func(i, j int) bool {
		return flaggedPieceResolvers[i].CreatedAt.After(flaggedPieces[j].CreatedAt)
	})

	return &flaggedPieceListResolver{
		TotalCount: int32(count),
		Pieces:     flaggedPieceResolvers,
		More:       more,
	}, nil
}

type piecesFlaggedCountArgs struct {
	HasUnsealedCopy graphql.NullBool
}

func (r *resolver) PiecesFlaggedCount(ctx context.Context, args piecesFlaggedCountArgs) (int32, error) {
	var filter *types.FlaggedPiecesListFilter
	if args.HasUnsealedCopy.Set && args.HasUnsealedCopy.Value != nil {
		filter = &types.FlaggedPiecesListFilter{HasUnsealedCopy: *args.HasUnsealedCopy.Value}
	}

	count, err := r.piecedirectory.FlaggedPiecesCount(ctx, filter)
	return int32(count), err
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

func (r *resolver) PieceIndexes(ctx context.Context, args struct{ PieceCid string }) ([]string, error) {
	var indexes []string
	pieceCid, err := cid.Parse(args.PieceCid)
	if err != nil {
		return nil, fmt.Errorf("%s is not a valid piece cid", args.PieceCid)
	}

	ii, err := r.piecedirectory.GetIterableIndex(ctx, pieceCid)
	if err != nil {
		return nil, fmt.Errorf("could not get indexes for %s: %w", pieceCid, err)
	}

	err = ii.ForEach(func(m multihash.Multihash, _ uint64) error {
		indexes = append(indexes, cid.NewCidV1(cid.DagProtobuf, m).String())
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("iterating index for piece %s: %w", pieceCid, err)
	}

	return indexes, nil
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

	// Get piece info from local index directory
	pieceInfo, pmErr := r.piecedirectory.GetPieceMetadata(ctx, pieceCid)
	if pmErr != nil && !types.IsNotFound(pmErr) {
		return nil, pmErr
	}

	// Get boost deals by piece Cid
	boostDeals, err := r.dealsDB.ByPieceCID(ctx, pieceCid)
	if err != nil {
		return nil, err
	}

	// Get legacy markets deals by piece Cid
	legacyDeals, err := r.legacyDeals.ByPieceCid(ctx, pieceCid)
	if err != nil {
		return nil, err
	}

	// Convert local index directory deals to graphQL format
	var pids []*pieceInfoDeal
	for _, dl := range pieceInfo.Deals {
		sector := &sectorResolver{
			ID:     gqltypes.Uint64(dl.SectorID),
			Offset: gqltypes.Uint64(dl.PieceOffset),
			Length: gqltypes.Uint64(dl.PieceLength),
		}

		actorId, err := address.IDFromAddress(dl.MinerAddr)
		if err != nil {
			return nil, fmt.Errorf("getting actor id from address %s", dl.MinerAddr)
		}

		pids = append(pids, &pieceInfoDeal{
			MinerAddress: dl.MinerAddr.String(),
			ChainDealID:  gqltypes.Uint64(dl.ChainDealID),
			Sector:       sector,
			ss: &sealStatusReporter{
				mma:     r.mma,
				ssm:     r.ssm,
				sector:  sector,
				minerID: abi.ActorID(actorId),
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

		provAddr, err := address.NewFromString(bd.ProviderAddress)
		if err != nil {
			return nil, fmt.Errorf("parsing actor address %s", bd.ProviderAddress)
		}
		minerId, err := address.IDFromAddress(provAddr)
		if err != nil {
			return nil, fmt.Errorf("getting actor id from address %s", bd.ProviderAddress)
		}
		sector := &sectorResolver{
			ID:     gqltypes.Uint64(dl.SectorID),
			Offset: gqltypes.Uint64(dl.Offset),
			Length: gqltypes.Uint64(dl.Length),
		}
		deals = append(deals, &pieceDealResolver{
			Deal:   &bd,
			Sector: sector,
			ss: &sealStatusReporter{
				mma:     r.mma,
				sector:  sector,
				ssm:     r.ssm,
				minerID: abi.ActorID(minerId),
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
		if sector == nil {
			sector = &sectorResolver{ID: gqltypes.Uint64(dl.SectorNumber)}
		}

		provAddr, err := address.NewFromString(bd.ProviderAddress)
		if err != nil {
			return nil, fmt.Errorf("parsing actor address %s", bd.ProviderAddress)
		}
		minerId, err := address.IDFromAddress(provAddr)
		if err != nil {
			return nil, fmt.Errorf("getting actor id from address %s", bd.ProviderAddress)
		}
		deals = append(deals, &pieceDealResolver{
			Deal:   &bd,
			Sector: sector,
			ss: &sealStatusReporter{
				sector:  sector,
				mma:     r.mma,
				ssm:     r.ssm,
				minerID: abi.ActorID(minerId),
			},
		})
	}

	// Get the state of the piece's index
	idxStatus, err := r.getIndexStatus(pieceInfo, pmErr)
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

func (r *resolver) getIndexStatus(md pdtypes.PieceDirMetadata, mdErr error) (*indexStatus, error) {
	var idxst IndexStatus
	idxerr := ""

	switch {
	case mdErr != nil && types.IsNotFound(mdErr):
		idxst = IndexStatusNotFound
	case mdErr != nil:
		idxst = IndexStatusFailed
		idxerr = mdErr.Error()
	case md.Indexing:
		idxst = IndexStatusIndexing
	case md.IndexedAt.IsZero():
		idxst = IndexStatusRegistered
	default:
		idxst = IndexStatusComplete
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

type sealStatusReporter struct {
	mma     *lib.MultiMinerAccessor
	sector  *sectorResolver
	ssm     *sectorstatemgr.SectorStateMgr
	minerID abi.ActorID
}

func (ss *sealStatusReporter) sealStatus(ctx context.Context) *sealStatusResolver {
	if ss.sector == nil || ss.sector.ID == 0 {
		return &sealStatusResolver{Error: "unable to find sector for deal"}
	}
	if ss.sector.Length == 0 {
		return &sealStatusResolver{Error: fmt.Sprintf("sector %d has zero length", ss.sector.ID)}
	}

	ssr := &sealStatusResolver{}

	// Get the sealing status as reported by the SectorsStatus API call
	isUnsealedCtx, cancel := context.WithTimeout(ctx, isUnsealedTimeout)
	defer cancel()

	maddr, err := address.NewIDAddress(uint64(ss.minerID))
	if err != nil {
		// There should never be an error but handle it just in case
		return &sealStatusResolver{Error: fmt.Sprintf("unable to convert miner ID %d into ID address: %s", ss.minerID, err)}
	}
	sectorsStatusApiIsUnsealed, err := ss.mma.IsUnsealed(
		isUnsealedCtx, maddr, abi.SectorNumber(ss.sector.ID),
		abi.PaddedPieceSize(ss.sector.Offset).Unpadded(),
		abi.PaddedPieceSize(ss.sector.Length).Unpadded(),
	)

	if err != nil {
		ssr.Status = string(SealedStatusError)
		ssr.Error = err.Error()
		if isUnsealedCtx.Err() != nil {
			ssr.Error = fmt.Sprintf("IsUnsealed: timed out after %s "+
				"(IsUnsealed blocks if the sector is currently being unsealed)",
				isUnsealedTimeout)
		}
		return ssr
	}

	if sectorsStatusApiIsUnsealed {
		ssr.Status = string(SealedStatusHasUnsealedCopy)
	} else {
		ssr.Status = string(SealedStatusNoUnsealedCopy)
	}

	// Get the sealing status as reported by the StorageList API call
	ss.ssm.LatestUpdateMu.Lock()
	lu := ss.ssm.LatestUpdate
	ss.ssm.LatestUpdateMu.Unlock()

	// Check if the StorageList API call has completed at least once.
	// If not, just return the sealing status reported by the SectorsStatus API call.
	if lu == nil {
		return ssr
	}

	sectorId := abi.SectorID{
		Miner:  ss.minerID,
		Number: abi.SectorNumber(ss.sector.ID),
	}
	sectorState, ok := lu.SectorStates[sectorId]
	if !ok {
		return ssr
	}

	// Check that the sealing status of the sector reported by both API calls
	// agrees
	storageListApiIsUnsealed := sectorState == db.SealStateUnsealed
	if storageListApiIsUnsealed != sectorsStatusApiIsUnsealed {
		// The sealing status reported by the two APIs disagrees, so report
		// the sealing status as unknown. It could be that the change happened
		// recently and the caches are out of sync, or it could be that the
		// sector is corrupted.
		ssr.Status = string(SealedStatusUnknown)
	}

	return ssr
}

func (r *resolver) PiecePayloadCids(ctx context.Context, args struct{ PieceCid string }) ([]*piecePayload, error) {
	var out []*piecePayload
	pieceCid, err := cid.Parse(args.PieceCid)
	if err != nil {
		return nil, fmt.Errorf("%s is not a valid piece cid", args.PieceCid)
	}

	// Additional check to return early if piece in not present in LID
	// This is to avoid a slow operation of reading all the indexes from the DB
	_, err = r.piecedirectory.GetPieceMetadata(ctx, pieceCid)
	if err != nil {
		return nil, err
	}

	ii, err := r.piecedirectory.GetIterableIndex(ctx, pieceCid)
	if err != nil {
		return nil, err
	}

	err = ii.ForEach(func(m multihash.Multihash, _ uint64) error {
		payload := piecePayload{
			PayloadCid: cid.NewCidV1(cid.Raw, m).String(),
			Multihash:  m.HexString(),
		}
		out = append(out, &payload)
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("iterating index for piece %s: %w", pieceCid, err)
	}

	return out, nil
}
