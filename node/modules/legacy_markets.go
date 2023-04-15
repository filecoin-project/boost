package modules

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	piecefilestore "github.com/filecoin-project/boost-gfm/filestore"
	"github.com/filecoin-project/boost-gfm/storagemarket"
	storageimpl "github.com/filecoin-project/boost-gfm/storagemarket/impl"
	"github.com/filecoin-project/boost-gfm/storagemarket/impl/storedask"
	smnet "github.com/filecoin-project/boost-gfm/storagemarket/network"
	"github.com/filecoin-project/boost-gfm/stores"
	"github.com/filecoin-project/boost/markets/idxprov"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/go-address"
	datatransferv2 "github.com/filecoin-project/go-data-transfer/v2"
	lotus_gfm_filestore "github.com/filecoin-project/go-fil-markets/filestore"
	lotus_gfm_storagemarket "github.com/filecoin-project/go-fil-markets/storagemarket"
	lotus_modules "github.com/filecoin-project/lotus/node/modules"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/node/repo"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	provider "github.com/ipni/index-provider"
	"github.com/libp2p/go-libp2p/core/host"
)

func StorageProvider(minerAddress lotus_dtypes.MinerAddress,
	storedAsk *storedask.StoredAsk,
	h host.Host, ds lotus_dtypes.MetadataDS,
	r repo.LockedRepo,
	pieceStore dtypes.ProviderPieceStore,
	indexer provider.Interface,
	dataTransfer dtypes.ProviderDataTransfer,
	spn storagemarket.StorageProviderNode,
	df storageimpl.DealDeciderFunc,
	dsw stores.DAGStoreWrapper,
	meshCreator idxprov.MeshCreator,
) (storagemarket.StorageProvider, error) {
	net := smnet.NewFromLibp2pHost(h)

	dir := filepath.Join(r.Path(), lotus_modules.StagingAreaDirName)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("creating directory for staging legacy markets deals %s: %w", dir, err)
	}

	store, err := piecefilestore.NewLocalFileStore(piecefilestore.OsPath(dir))
	if err != nil {
		return nil, err
	}

	opt := storageimpl.CustomDealDecisionLogic(df)

	return storageimpl.NewProvider(
		net,
		namespace.Wrap(ds, datastore.NewKey("/deals/provider")),
		store,
		dsw,
		indexer,
		pieceStore,
		dataTransfer,
		spn,
		address.Address(minerAddress),
		storedAsk,
		meshCreator,
		opt,
	)
}

func DealDeciderFn(df lotus_dtypes.StorageDealFilter) storageimpl.DealDeciderFunc {
	return func(ctx context.Context, deal storagemarket.MinerDeal) (bool, string, error) {
		return df(ctx, toLotusGFMMinerDeal(deal))
	}
}

func toLotusGFMMinerDeal(deal storagemarket.MinerDeal) lotus_gfm_storagemarket.MinerDeal {
	lotusGFMDeal := lotus_gfm_storagemarket.MinerDeal{
		ClientDealProposal:    deal.ClientDealProposal,
		ProposalCid:           deal.ProposalCid,
		AddFundsCid:           deal.AddFundsCid,
		PublishCid:            deal.PublishCid,
		Miner:                 deal.Miner,
		Client:                deal.Client,
		State:                 deal.State,
		PiecePath:             lotus_gfm_filestore.Path(deal.PiecePath),
		MetadataPath:          lotus_gfm_filestore.Path(deal.MetadataPath),
		SlashEpoch:            deal.SlashEpoch,
		FastRetrieval:         deal.FastRetrieval,
		Message:               deal.Message,
		FundsReserved:         deal.FundsReserved,
		AvailableForRetrieval: deal.AvailableForRetrieval,
		DealID:                deal.DealID,
		CreationTime:          deal.CreationTime,
		SectorNumber:          deal.SectorNumber,
		InboundCAR:            deal.InboundCAR,
	}
	if deal.Ref != nil {
		lotusGFMDeal.Ref = &lotus_gfm_storagemarket.DataRef{
			TransferType: deal.Ref.TransferType,
			Root:         deal.Ref.Root,
			PieceCid:     deal.Ref.PieceCid,
			PieceSize:    deal.Ref.PieceSize,
			RawBlockSize: deal.Ref.RawBlockSize,
		}
	}
	if deal.TransferChannelId != nil {
		lotusGFMDeal.TransferChannelId = &datatransferv2.ChannelID{
			Initiator: deal.TransferChannelId.Initiator,
			Responder: deal.TransferChannelId.Responder,
			ID:        datatransferv2.TransferID(deal.TransferChannelId.ID),
		}
	}
	return lotusGFMDeal
}
