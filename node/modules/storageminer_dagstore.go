package modules

import (
	"context"
	"github.com/filecoin-project/boost-gfm/piecestore"
	"github.com/filecoin-project/boost-gfm/storagemarket"
	"github.com/filecoin-project/boost-gfm/stores"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/dagstore"
	lotus_gfm_piecestore "github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/filecoin-project/go-fil-markets/shared"
	lotus_gfm_storagemarket "github.com/filecoin-project/go-fil-markets/storagemarket"
	mdagstore "github.com/filecoin-project/lotus/markets/dagstore"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/index"
)

func NewBoostGFMDAGStoreWrapper(w *mdagstore.Wrapper) stores.DAGStoreWrapper {
	return &boostDagstoreWrapper{w: w}
}

type boostDagstoreWrapper struct {
	w *mdagstore.Wrapper
}

func (b *boostDagstoreWrapper) RegisterShard(ctx context.Context, pieceCid cid.Cid, carPath string, eagerInit bool, resch chan dagstore.ShardResult) error {
	return b.w.RegisterShard(ctx, pieceCid, carPath, eagerInit, resch)
}

func (b *boostDagstoreWrapper) LoadShard(ctx context.Context, pieceCid cid.Cid) (stores.ClosableBlockstore, error) {
	return b.w.LoadShard(ctx, pieceCid)
}

func (b *boostDagstoreWrapper) MigrateDeals(ctx context.Context, deals []storagemarket.MinerDeal) (bool, error) {
	dls := make([]lotus_gfm_storagemarket.MinerDeal, 0, len(deals))
	for _, d := range deals {
		dls = append(dls, toLotusGFMMinerDeal(d))
	}
	return b.w.MigrateDeals(ctx, dls)
}

func (b *boostDagstoreWrapper) GetPiecesContainingBlock(blockCID cid.Cid) ([]cid.Cid, error) {
	return b.w.GetPiecesContainingBlock(blockCID)
}

func (b *boostDagstoreWrapper) GetIterableIndexForPiece(pieceCid cid.Cid) (index.IterableIndex, error) {
	return b.w.GetIterableIndexForPiece(pieceCid)
}

func (b *boostDagstoreWrapper) DestroyShard(ctx context.Context, pieceCid cid.Cid, resch chan dagstore.ShardResult) error {
	return b.w.DestroyShard(ctx, pieceCid, resch)
}

func (b *boostDagstoreWrapper) Close() error {
	return b.w.Close()
}

func NewLotusGFMProviderPieceStore(ps dtypes.ProviderPieceStore) lotus_dtypes.ProviderPieceStore {
	return &lotusProviderPieceStore{ProviderPieceStore: ps}
}

type lotusProviderPieceStore struct {
	dtypes.ProviderPieceStore
}

var _ lotus_dtypes.ProviderPieceStore = (*lotusProviderPieceStore)(nil)

func (l *lotusProviderPieceStore) OnReady(ready shared.ReadyFunc) {
	if ready == nil {
		return
	}
	l.ProviderPieceStore.OnReady(func(err error) {
		ready(err)
	})
}

func (l *lotusProviderPieceStore) AddDealForPiece(pieceCID cid.Cid, payloadCid cid.Cid, dealInfo lotus_gfm_piecestore.DealInfo) error {
	return l.ProviderPieceStore.AddDealForPiece(pieceCID, payloadCid, piecestore.DealInfo{
		DealID:   dealInfo.DealID,
		SectorID: dealInfo.SectorID,
		Offset:   dealInfo.Offset,
		Length:   dealInfo.Length,
	})
}

func (l *lotusProviderPieceStore) AddPieceBlockLocations(pieceCID cid.Cid, blockLocations map[cid.Cid]lotus_gfm_piecestore.BlockLocation) error {
	bls := make(map[cid.Cid]piecestore.BlockLocation, len(blockLocations))
	for c, bl := range blockLocations {
		bls[c] = piecestore.BlockLocation{
			RelOffset: bl.RelOffset,
			BlockSize: bl.BlockSize,
		}
	}
	return l.ProviderPieceStore.AddPieceBlockLocations(pieceCID, bls)
}

func (l *lotusProviderPieceStore) GetPieceInfo(pieceCID cid.Cid) (lotus_gfm_piecestore.PieceInfo, error) {
	pi, err := l.ProviderPieceStore.GetPieceInfo(pieceCID)
	if err != nil {
		return lotus_gfm_piecestore.PieceInfo{}, err
	}
	dls := make([]lotus_gfm_piecestore.DealInfo, 0, len(pi.Deals))
	for _, d := range pi.Deals {
		dls = append(dls, lotus_gfm_piecestore.DealInfo{
			DealID:   d.DealID,
			SectorID: d.SectorID,
			Offset:   d.Offset,
			Length:   d.Length,
		})
	}
	return lotus_gfm_piecestore.PieceInfo{
		PieceCID: pi.PieceCID,
		Deals:    dls,
	}, nil
}

func (l *lotusProviderPieceStore) GetCIDInfo(payloadCID cid.Cid) (lotus_gfm_piecestore.CIDInfo, error) {
	ci, err := l.ProviderPieceStore.GetCIDInfo(payloadCID)
	if err != nil {
		return lotus_gfm_piecestore.CIDInfo{}, err
	}

	bls := make([]lotus_gfm_piecestore.PieceBlockLocation, 0, len(ci.PieceBlockLocations))
	for _, bl := range ci.PieceBlockLocations {
		bls = append(bls, lotus_gfm_piecestore.PieceBlockLocation{
			BlockLocation: lotus_gfm_piecestore.BlockLocation{
				RelOffset: bl.BlockLocation.RelOffset,
				BlockSize: bl.BlockLocation.BlockSize,
			},
			PieceCID: bl.PieceCID,
		})
	}
	return lotus_gfm_piecestore.CIDInfo{
		CID:                 ci.CID,
		PieceBlockLocations: bls,
	}, nil
}
