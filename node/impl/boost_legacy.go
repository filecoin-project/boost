package impl

import (
	"context"
	"os"
	"time"

	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"golang.org/x/xerrors"
)

func (sm *BoostAPI) MarketListDataTransfers(ctx context.Context) ([]lapi.DataTransferChannel, error) {
	inProgressChannels, err := sm.DataTransfer.InProgressChannels(ctx)
	if err != nil {
		return nil, err
	}

	apiChannels := make([]lapi.DataTransferChannel, 0, len(inProgressChannels))
	for _, channelState := range inProgressChannels {
		apiChannels = append(apiChannels, lapi.NewDataTransferChannel(sm.Host.ID(), channelState))
	}

	return apiChannels, nil
}

func (sm *BoostAPI) MarketRestartDataTransfer(ctx context.Context, transferID datatransfer.TransferID, otherPeer peer.ID, isInitiator bool) error {
	selfPeer := sm.Host.ID()
	if isInitiator {
		return sm.DataTransfer.RestartDataTransferChannel(ctx, datatransfer.ChannelID{Initiator: selfPeer, Responder: otherPeer, ID: transferID})
	}
	return sm.DataTransfer.RestartDataTransferChannel(ctx, datatransfer.ChannelID{Initiator: otherPeer, Responder: selfPeer, ID: transferID})
}

func (sm *BoostAPI) MarketDataTransferUpdates(ctx context.Context) (<-chan lapi.DataTransferChannel, error) {
	channels := make(chan lapi.DataTransferChannel)

	unsub := sm.DataTransfer.SubscribeToEvents(func(evt datatransfer.Event, channelState datatransfer.ChannelState) {
		channel := lapi.NewDataTransferChannel(sm.Host.ID(), channelState)
		select {
		case <-ctx.Done():
		case channels <- channel:
		}
	})

	go func() {
		defer unsub()
		<-ctx.Done()
	}()

	return channels, nil
}

func (sm *BoostAPI) MarketListRetrievalDeals(ctx context.Context) ([]retrievalmarket.ProviderDealState, error) {
	var out []retrievalmarket.ProviderDealState
	deals := sm.RetrievalProvider.ListDeals()

	for _, deal := range deals {
		if deal.ChannelID != nil {
			if deal.ChannelID.Initiator == "" || deal.ChannelID.Responder == "" {
				deal.ChannelID = nil // don't try to push unparsable peer IDs over jsonrpc
			}
		}
		out = append(out, deal)
	}

	return out, nil
}

func (sm *BoostAPI) MarketImportDealData(ctx context.Context, propCid cid.Cid, path string) error {
	fi, err := os.Open(path)
	if err != nil {
		return xerrors.Errorf("failed to open file: %w", err)
	}
	defer fi.Close() //nolint:errcheck

	return sm.LegacyStorageProvider.ImportDataForDeal(ctx, propCid, fi)
}

func (sm *BoostAPI) MarketSetRetrievalAsk(ctx context.Context, rask *retrievalmarket.Ask) error {
	sm.RetrievalProvider.SetAsk(rask)
	return nil
}

func (sm *BoostAPI) MarketGetRetrievalAsk(ctx context.Context) (*retrievalmarket.Ask, error) {
	return sm.RetrievalProvider.GetAsk(), nil
}

func (sm *BoostAPI) DealsConsiderOnlineStorageDeals(ctx context.Context) (bool, error) {
	return sm.ConsiderOnlineStorageDealsConfigFunc()
}

func (sm *BoostAPI) DealsSetConsiderOnlineStorageDeals(ctx context.Context, b bool) error {
	return sm.SetConsiderOnlineStorageDealsConfigFunc(b)
}

func (sm *BoostAPI) DealsConsiderOnlineRetrievalDeals(ctx context.Context) (bool, error) {
	return sm.ConsiderOnlineRetrievalDealsConfigFunc()
}

func (sm *BoostAPI) DealsSetConsiderOnlineRetrievalDeals(ctx context.Context, b bool) error {
	return sm.SetConsiderOnlineRetrievalDealsConfigFunc(b)
}

func (sm *BoostAPI) DealsConsiderOfflineStorageDeals(ctx context.Context) (bool, error) {
	return sm.ConsiderOfflineStorageDealsConfigFunc()
}

func (sm *BoostAPI) DealsSetConsiderOfflineStorageDeals(ctx context.Context, b bool) error {
	return sm.SetConsiderOfflineStorageDealsConfigFunc(b)
}

func (sm *BoostAPI) DealsConsiderOfflineRetrievalDeals(ctx context.Context) (bool, error) {
	return sm.ConsiderOfflineRetrievalDealsConfigFunc()
}

func (sm *BoostAPI) DealsSetConsiderOfflineRetrievalDeals(ctx context.Context, b bool) error {
	return sm.SetConsiderOfflineRetrievalDealsConfigFunc(b)
}

func (sm *BoostAPI) DealsConsiderVerifiedStorageDeals(ctx context.Context) (bool, error) {
	return sm.ConsiderVerifiedStorageDealsConfigFunc()
}

func (sm *BoostAPI) DealsSetConsiderVerifiedStorageDeals(ctx context.Context, b bool) error {
	return sm.SetConsiderVerifiedStorageDealsConfigFunc(b)
}

func (sm *BoostAPI) DealsConsiderUnverifiedStorageDeals(ctx context.Context) (bool, error) {
	return sm.ConsiderUnverifiedStorageDealsConfigFunc()
}

func (sm *BoostAPI) DealsSetConsiderUnverifiedStorageDeals(ctx context.Context, b bool) error {
	return sm.SetConsiderUnverifiedStorageDealsConfigFunc(b)
}

func (sm *BoostAPI) DealsGetExpectedSealDurationFunc(ctx context.Context) (time.Duration, error) {
	return sm.GetExpectedSealDurationFunc()
}

func (sm *BoostAPI) DealsSetExpectedSealDurationFunc(ctx context.Context, d time.Duration) error {
	return sm.SetExpectedSealDurationFunc(d)
}

func (sm *BoostAPI) DealsPieceCidBlocklist(ctx context.Context) ([]cid.Cid, error) {
	return sm.StorageDealPieceCidBlocklistConfigFunc()
}

func (sm *BoostAPI) DealsSetPieceCidBlocklist(ctx context.Context, cids []cid.Cid) error {
	return sm.SetStorageDealPieceCidBlocklistConfigFunc(cids)
}

func (sm *BoostAPI) MarketSetAsk(ctx context.Context, price types.BigInt, verifiedPrice types.BigInt, duration abi.ChainEpoch, minPieceSize abi.PaddedPieceSize, maxPieceSize abi.PaddedPieceSize) error {
	options := []storagemarket.StorageAskOption{
		storagemarket.MinPieceSize(minPieceSize),
		storagemarket.MaxPieceSize(maxPieceSize),
	}

	return sm.LegacyStorageProvider.SetAsk(price, verifiedPrice, duration, options...)
}

func (sm *BoostAPI) MarketGetAsk(ctx context.Context) (*storagemarket.SignedStorageAsk, error) {
	return sm.LegacyStorageProvider.GetAsk(), nil
}

func (sm *BoostAPI) ActorSectorSize(ctx context.Context, addr address.Address) (abi.SectorSize, error) {
	mi, err := sm.Full.StateMinerInfo(ctx, addr, types.EmptyTSK)
	if err != nil {
		return 0, err
	}
	return mi.SectorSize, nil
}
