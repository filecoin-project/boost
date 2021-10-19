package impl

import (
	"encoding/json"
	"net/http"

	"github.com/filecoin-project/go-jsonrpc/auth"

	"go.uber.org/fx"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/node/modules/dtypes"
)

type BoostAPI struct {
	fx.In

	api.Common
	api.Net

	//Full        api.FullNode
	//LocalStore  *stores.Local
	//RemoteStore *stores.Remote

	//// Markets
	//PieceStore        dtypes.ProviderPieceStore         `optional:"true"`
	//StorageProvider   storagemarket.StorageProvider     `optional:"true"`
	//RetrievalProvider retrievalmarket.RetrievalProvider `optional:"true"`
	//SectorAccessor    retrievalmarket.SectorAccessor    `optional:"true"`
	//DataTransfer      dtypes.ProviderDataTransfer       `optional:"true"`
	//DealPublisher     *storageadapter.DealPublisher     `optional:"true"`
	//SectorBlocks      *sectorblocks.SectorBlocks        `optional:"true"`
	//Host              host.Host                         `optional:"true"`
	//DAGStore          *dagstore.DAGStore                `optional:"true"`

	DS dtypes.MetadataDS
}

var _ api.Boost = &BoostAPI{}

func (sm *BoostAPI) ServeRemote(perm bool) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if perm == true {
			if !auth.HasPerm(r.Context(), nil, api.PermAdmin) {
				w.WriteHeader(401)
				_ = json.NewEncoder(w).Encode(struct{ Error string }{"unauthorized: missing write permission"})
				return
			}
		}

		//sm.StorageMgr.ServeHTTP(w, r)
	}
}

//func (sm *BoostAPI) MarketImportDealData(ctx context.Context, propCid cid.Cid, path string) error {
//fi, err := os.Open(path)
//if err != nil {
//return xerrors.Errorf("failed to open file: %w", err)
//}
//defer fi.Close() //nolint:errcheck

//return sm.StorageProvider.ImportDataForDeal(ctx, propCid, fi)
//}

//func (sm *BoostAPI) listDeals(ctx context.Context) ([]api.MarketDeal, error) {
//ts, err := sm.Full.ChainHead(ctx)
//if err != nil {
//return nil, err
//}
//tsk := ts.Key()
//allDeals, err := sm.Full.StateMarketDeals(ctx, tsk)
//if err != nil {
//return nil, err
//}

//var out []api.MarketDeal

//for _, deal := range allDeals {
//if deal.Proposal.Provider == sm.Miner.Address() {
//out = append(out, deal)
//}
//}

//return out, nil
//}

//func (sm *BoostAPI) MarketListDeals(ctx context.Context) ([]api.MarketDeal, error) {
//return sm.listDeals(ctx)
//}

//func (sm *BoostAPI) MarketListRetrievalDeals(ctx context.Context) ([]retrievalmarket.ProviderDealState, error) {
//var out []retrievalmarket.ProviderDealState
//deals := sm.RetrievalProvider.ListDeals()

//for _, deal := range deals {
//if deal.ChannelID != nil {
//if deal.ChannelID.Initiator == "" || deal.ChannelID.Responder == "" {
//deal.ChannelID = nil // don't try to push unparsable peer IDs over jsonrpc
//}
//}
//out = append(out, deal)
//}

//return out, nil
//}

//func (sm *BoostAPI) MarketGetDealUpdates(ctx context.Context) (<-chan storagemarket.MinerDeal, error) {
//results := make(chan storagemarket.MinerDeal)
//unsub := sm.StorageProvider.SubscribeToEvents(func(evt storagemarket.ProviderEvent, deal storagemarket.MinerDeal) {
//select {
//case results <- deal:
//case <-ctx.Done():
//}
//})
//go func() {
//<-ctx.Done()
//unsub()
//close(results)
//}()
//return results, nil
//}

//func (sm *BoostAPI) MarketListIncompleteDeals(ctx context.Context) ([]storagemarket.MinerDeal, error) {
//return sm.StorageProvider.ListLocalDeals()
//}

//func (sm *BoostAPI) MarketSetAsk(ctx context.Context, price types.BigInt, verifiedPrice types.BigInt, duration abi.ChainEpoch, minPieceSize abi.PaddedPieceSize, maxPieceSize abi.PaddedPieceSize) error {
//options := []storagemarket.StorageAskOption{
//storagemarket.MinPieceSize(minPieceSize),
//storagemarket.MaxPieceSize(maxPieceSize),
//}

//return sm.StorageProvider.SetAsk(price, verifiedPrice, duration, options...)
//}

//func (sm *BoostAPI) MarketGetAsk(ctx context.Context) (*storagemarket.SignedStorageAsk, error) {
//return sm.StorageProvider.GetAsk(), nil
//}

//func (sm *BoostAPI) MarketSetRetrievalAsk(ctx context.Context, rask *retrievalmarket.Ask) error {
//sm.RetrievalProvider.SetAsk(rask)
//return nil
//}

//func (sm *BoostAPI) MarketGetRetrievalAsk(ctx context.Context) (*retrievalmarket.Ask, error) {
//return sm.RetrievalProvider.GetAsk(), nil
//}

//func (sm *BoostAPI) MarketListDataTransfers(ctx context.Context) ([]api.DataTransferChannel, error) {
//inProgressChannels, err := sm.DataTransfer.InProgressChannels(ctx)
//if err != nil {
//return nil, err
//}

//apiChannels := make([]api.DataTransferChannel, 0, len(inProgressChannels))
//for _, channelState := range inProgressChannels {
//apiChannels = append(apiChannels, api.NewDataTransferChannel(sm.Host.ID(), channelState))
//}

//return apiChannels, nil
//}

//func (sm *BoostAPI) MarketRestartDataTransfer(ctx context.Context, transferID datatransfer.TransferID, otherPeer peer.ID, isInitiator bool) error {
//selfPeer := sm.Host.ID()
//if isInitiator {
//return sm.DataTransfer.RestartDataTransferChannel(ctx, datatransfer.ChannelID{Initiator: selfPeer, Responder: otherPeer, ID: transferID})
//}
//return sm.DataTransfer.RestartDataTransferChannel(ctx, datatransfer.ChannelID{Initiator: otherPeer, Responder: selfPeer, ID: transferID})
//}

//func (sm *BoostAPI) MarketCancelDataTransfer(ctx context.Context, transferID datatransfer.TransferID, otherPeer peer.ID, isInitiator bool) error {
//selfPeer := sm.Host.ID()
//if isInitiator {
//return sm.DataTransfer.CloseDataTransferChannel(ctx, datatransfer.ChannelID{Initiator: selfPeer, Responder: otherPeer, ID: transferID})
//}
//return sm.DataTransfer.CloseDataTransferChannel(ctx, datatransfer.ChannelID{Initiator: otherPeer, Responder: selfPeer, ID: transferID})
//}

//func (sm *BoostAPI) MarketDataTransferUpdates(ctx context.Context) (<-chan api.DataTransferChannel, error) {
//channels := make(chan api.DataTransferChannel)

//unsub := sm.DataTransfer.SubscribeToEvents(func(evt datatransfer.Event, channelState datatransfer.ChannelState) {
//channel := api.NewDataTransferChannel(sm.Host.ID(), channelState)
//select {
//case <-ctx.Done():
//case channels <- channel:
//}
//})

//go func() {
//defer unsub()
//<-ctx.Done()
//}()

//return channels, nil
//}

//func (sm *BoostAPI) MarketPendingDeals(ctx context.Context) (api.PendingDealInfo, error) {
//return api.PendingDealInfo{}, nil
////return sm.DealPublisher.PendingDeals(), nil
//}

//func (sm *BoostAPI) MarketRetryPublishDeal(ctx context.Context, propcid cid.Cid) error {
//return nil
////return sm.StorageProvider.RetryDealPublishing(propcid)
//}

//func (sm *BoostAPI) MarketPublishPendingDeals(ctx context.Context) error {
//sm.DealPublisher.ForcePublishPendingDeals()
//return nil
//}

//func (sm *BoostAPI) DagstoreListShards(ctx context.Context) ([]api.DagstoreShardInfo, error) {
//if sm.DAGStore == nil {
//return nil, fmt.Errorf("dagstore not available on this node")
//}

//info := sm.DAGStore.AllShardsInfo()
//ret := make([]api.DagstoreShardInfo, 0, len(info))
//for k, i := range info {
//ret = append(ret, api.DagstoreShardInfo{
//Key:   k.String(),
//State: i.ShardState.String(),
//Error: func() string {
//if i.Error == nil {
//return ""
//}
//return i.Error.Error()
//}(),
//})
//}

//// order by key.
//sort.SliceStable(ret, func(i, j int) bool {
//return ret[i].Key < ret[j].Key
//})

//return ret, nil
//}

//func (sm *BoostAPI) DagstoreInitializeShard(ctx context.Context, key string) error {
//if sm.DAGStore == nil {
//return fmt.Errorf("dagstore not available on this node")
//}

//k := shard.KeyFromString(key)

//info, err := sm.DAGStore.GetShardInfo(k)
//if err != nil {
//return fmt.Errorf("failed to get shard info: %w", err)
//}
//if st := info.ShardState; st != dagstore.ShardStateNew {
//return fmt.Errorf("cannot initialize shard; expected state ShardStateNew, was: %s", st.String())
//}

//ch := make(chan dagstore.ShardResult, 1)
//if err = sm.DAGStore.AcquireShard(ctx, k, ch, dagstore.AcquireOpts{}); err != nil {
//return fmt.Errorf("failed to acquire shard: %w", err)
//}

//var res dagstore.ShardResult
//select {
//case res = <-ch:
//case <-ctx.Done():
//return ctx.Err()
//}

//if err := res.Error; err != nil {
//return fmt.Errorf("failed to acquire shard: %w", err)
//}

//if res.Accessor != nil {
//err = res.Accessor.Close()
//if err != nil {
//log.Warnw("failed to close shard accessor; continuing", "shard_key", k, "error", err)
//}
//}

//return nil
//}

//func (sm *BoostAPI) DagstoreInitializeAll(ctx context.Context, params api.DagstoreInitializeAllParams) (<-chan api.DagstoreInitializeAllEvent, error) {
//if sm.DAGStore == nil {
//return nil, fmt.Errorf("dagstore not available on this node")
//}

//if sm.SectorAccessor == nil {
//return nil, fmt.Errorf("sector accessor not available on this node")
//}

//// prepare the thottler tokens.
//var throttle chan struct{}
//if c := params.MaxConcurrency; c > 0 {
//throttle = make(chan struct{}, c)
//for i := 0; i < c; i++ {
//throttle <- struct{}{}
//}
//}

//// are we initializing only unsealed pieces?
//onlyUnsealed := !params.IncludeSealed

//info := sm.DAGStore.AllShardsInfo()
//var toInitialize []string
//for k, i := range info {
//if i.ShardState != dagstore.ShardStateNew {
//continue
//}

//// if we're initializing only unsealed pieces, check if there's an
//// unsealed deal for this piece available.
//if onlyUnsealed {
//pieceCid, err := cid.Decode(k.String())
//if err != nil {
//log.Warnw("DagstoreInitializeAll: failed to decode shard key as piece CID; skipping", "shard_key", k.String(), "error", err)
//continue
//}

//pi, err := sm.PieceStore.GetPieceInfo(pieceCid)
//if err != nil {
//log.Warnw("DagstoreInitializeAll: failed to get piece info; skipping", "piece_cid", pieceCid, "error", err)
//continue
//}

//var isUnsealed bool
//for _, d := range pi.Deals {
//isUnsealed, err = sm.SectorAccessor.IsUnsealed(ctx, d.SectorID, d.Offset.Unpadded(), d.Length.Unpadded())
//if err != nil {
//log.Warnw("DagstoreInitializeAll: failed to get unsealed status; skipping deal", "deal_id", d.DealID, "error", err)
//continue
//}
//if isUnsealed {
//break
//}
//}

//if !isUnsealed {
//log.Infow("DagstoreInitializeAll: skipping piece because it's sealed", "piece_cid", pieceCid, "error", err)
//continue
//}
//}

//// yes, we're initializing this shard.
//toInitialize = append(toInitialize, k.String())
//}

//total := len(toInitialize)
//if total == 0 {
//out := make(chan api.DagstoreInitializeAllEvent)
//close(out)
//return out, nil
//}

//// response channel must be closed when we're done, or the context is cancelled.
//// this buffering is necessary to prevent inflight children goroutines from
//// publishing to a closed channel (res) when the context is cancelled.
//out := make(chan api.DagstoreInitializeAllEvent, 32) // internal buffer.
//res := make(chan api.DagstoreInitializeAllEvent, 32) // returned to caller.

//// pump events back to caller.
//// two events per shard.
//go func() {
//defer close(res)

//for i := 0; i < total*2; i++ {
//select {
//case res <- <-out:
//case <-ctx.Done():
//return
//}
//}
//}()

//go func() {
//for i, k := range toInitialize {
//if throttle != nil {
//select {
//case <-throttle:
//// acquired a throttle token, proceed.
//case <-ctx.Done():
//return
//}
//}

//go func(k string, i int) {
//r := api.DagstoreInitializeAllEvent{
//Key:     k,
//Event:   "start",
//Total:   total,
//Current: i + 1, // start with 1
//}
//select {
//case out <- r:
//case <-ctx.Done():
//return
//}

//err := sm.DagstoreInitializeShard(ctx, k)

//if throttle != nil {
//throttle <- struct{}{}
//}

//r.Event = "end"
//if err == nil {
//r.Success = true
//} else {
//r.Success = false
//r.Error = err.Error()
//}

//select {
//case out <- r:
//case <-ctx.Done():
//}
//}(k, i)
//}
//}()

//return res, nil

//}

//func (sm *BoostAPI) DagstoreRecoverShard(ctx context.Context, key string) error {
//if sm.DAGStore == nil {
//return fmt.Errorf("dagstore not available on this node")
//}

//k := shard.KeyFromString(key)

//info, err := sm.DAGStore.GetShardInfo(k)
//if err != nil {
//return fmt.Errorf("failed to get shard info: %w", err)
//}
//if st := info.ShardState; st != dagstore.ShardStateErrored {
//return fmt.Errorf("cannot recover shard; expected state ShardStateErrored, was: %s", st.String())
//}

//ch := make(chan dagstore.ShardResult, 1)
//if err = sm.DAGStore.RecoverShard(ctx, k, ch, dagstore.RecoverOpts{}); err != nil {
//return fmt.Errorf("failed to recover shard: %w", err)
//}

//var res dagstore.ShardResult
//select {
//case res = <-ch:
//case <-ctx.Done():
//return ctx.Err()
//}

//return res.Error
//}

//func (sm *BoostAPI) DagstoreGC(ctx context.Context) ([]api.DagstoreShardResult, error) {
//if sm.DAGStore == nil {
//return nil, fmt.Errorf("dagstore not available on this node")
//}

//res, err := sm.DAGStore.GC(ctx)
//if err != nil {
//return nil, fmt.Errorf("failed to gc: %w", err)
//}

//ret := make([]api.DagstoreShardResult, 0, len(res.Shards))
//for k, err := range res.Shards {
//r := api.DagstoreShardResult{Key: k.String()}
//if err == nil {
//r.Success = true
//} else {
//r.Success = false
//r.Error = err.Error()
//}
//ret = append(ret, r)
//}

//return ret, nil
//}

//func (sm *BoostAPI) DealsList(ctx context.Context) ([]api.MarketDeal, error) {
//return sm.listDeals(ctx)
//}

//func (sm *BoostAPI) RetrievalDealsList(ctx context.Context) (map[retrievalmarket.ProviderDealIdentifier]retrievalmarket.ProviderDealState, error) {
//return sm.RetrievalProvider.ListDeals(), nil
//}

//func (sm *BoostAPI) DealsImportData(ctx context.Context, deal cid.Cid, fname string) error {
//fi, err := os.Open(fname)
//if err != nil {
//return xerrors.Errorf("failed to open given file: %w", err)
//}
//defer fi.Close() //nolint:errcheck

//return sm.StorageProvider.ImportDataForDeal(ctx, deal, fi)
//}

//func (sm *BoostAPI) DealsPieceCidBlocklist(ctx context.Context) ([]cid.Cid, error) {
//return sm.StorageDealPieceCidBlocklistConfigFunc()
//}

//func (sm *BoostAPI) DealsSetPieceCidBlocklist(ctx context.Context, cids []cid.Cid) error {
//return sm.SetStorageDealPieceCidBlocklistConfigFunc(cids)
//}
