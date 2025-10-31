package gql

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/boost/cmd/lib"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/fundmanager"
	gqltypes "github.com/filecoin-project/boost/gql/types"
	"github.com/filecoin-project/boost/indexprovider"
	"github.com/filecoin-project/boost/lib/legacy"
	"github.com/filecoin-project/boost/lib/mpoolmonitor"
	"github.com/filecoin-project/boost/markets/storageadapter"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/boost/retrievalmarket/rtvllog"
	"github.com/filecoin-project/boost/sectorstatemgr"
	"github.com/filecoin-project/boost/storagemanager"
	"github.com/filecoin-project/boost/storagemarket"
	"github.com/filecoin-project/boost/storagemarket/sealingpipeline"
	"github.com/filecoin-project/boost/storagemarket/storedask"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/boost/transport"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/build"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	"github.com/google/uuid"
	"github.com/graph-gophers/graphql-go"
	"github.com/ipfs/go-cid"
	provider "github.com/ipni/index-provider"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
)

type dealListResolver struct {
	TotalCount int32
	Deals      []*dealResolver
	More       bool
}

// resolver translates from a request for a graphql field to the data for
// that field
type resolver struct {
	// This context is closed when boost shuts down
	ctx context.Context

	cfg            *config.Boost
	repo           lotus_repo.LockedRepo
	h              host.Host
	dealsDB        *db.DealsDB
	directDealsDB  *db.DirectDealsDB
	logsDB         *db.LogsDB
	retDB          *rtvllog.RetrievalLogDB
	plDB           *db.ProposalLogsDB
	fundsDB        *db.FundsDB
	fundMgr        *fundmanager.FundManager
	storageMgr     *storagemanager.StorageManager
	provider       *storagemarket.Provider
	legacyDeals    legacy.LegacyDealManager
	ddProvider     *storagemarket.DirectDealsProvider
	ssm            *sectorstatemgr.SectorStateMgr
	piecedirectory *piecedirectory.PieceDirectory
	publisher      *storageadapter.DealPublisher
	idxProv        provider.Interface
	idxProvWrapper *indexprovider.Wrapper
	spApi          sealingpipeline.API
	fullNode       v1api.FullNode
	mpool          *mpoolmonitor.MpoolMonitor
	mma            *lib.MultiMinerAccessor
	askProv        storedask.StoredAsk
	curio          bool
}

func NewResolver(ctx context.Context, cfg *config.Boost, r lotus_repo.LockedRepo, h host.Host, dealsDB *db.DealsDB, directDealsDB *db.DirectDealsDB, logsDB *db.LogsDB, retDB *rtvllog.RetrievalLogDB, plDB *db.ProposalLogsDB, fundsDB *db.FundsDB, fundMgr *fundmanager.FundManager, storageMgr *storagemanager.StorageManager, spApi sealingpipeline.API, provider *storagemarket.Provider, ddProvider *storagemarket.DirectDealsProvider, legacyDeals legacy.LegacyDealManager, piecedirectory *piecedirectory.PieceDirectory, publisher *storageadapter.DealPublisher, indexProv provider.Interface, idxProvWrapper *indexprovider.Wrapper, fullNode v1api.FullNode, ssm *sectorstatemgr.SectorStateMgr, mpool *mpoolmonitor.MpoolMonitor, mma *lib.MultiMinerAccessor, assk storedask.StoredAsk) (*resolver, error) {

	ret := &resolver{
		ctx:            ctx,
		cfg:            cfg,
		repo:           r,
		h:              h,
		dealsDB:        dealsDB,
		directDealsDB:  directDealsDB,
		logsDB:         logsDB,
		retDB:          retDB,
		plDB:           plDB,
		fundsDB:        fundsDB,
		fundMgr:        fundMgr,
		storageMgr:     storageMgr,
		provider:       provider,
		ddProvider:     ddProvider,
		legacyDeals:    legacyDeals,
		piecedirectory: piecedirectory,
		publisher:      publisher,
		spApi:          spApi,
		idxProv:        indexProv,
		idxProvWrapper: idxProvWrapper,
		fullNode:       fullNode,
		ssm:            ssm,
		mpool:          mpool,
		mma:            mma,
		askProv:        assk,
	}

	v, err := spApi.Version(context.Background())
	if err != nil {
		return nil, err
	}

	if strings.Contains(v.String(), "curio") {
		ret.curio = true
	}

	return ret, nil
}

// query: deal(id) Deal
func (r *resolver) Deal(ctx context.Context, args struct{ ID graphql.ID }) (*dealResolver, error) {
	id, err := toUuid(args.ID)
	if err != nil {
		return nil, err
	}

	deal, err := r.dealByID(ctx, id)
	if err != nil {
		return nil, err
	}

	return newDealResolver(r.mpool, deal, r.provider, r.dealsDB, r.logsDB, r.spApi, r.curio), nil
}

type filterArgs struct {
	Checkpoint   gqltypes.Checkpoint
	IsOffline    graphql.NullBool
	TransferType graphql.NullString
	IsVerified   graphql.NullBool
}

type dealsArgs struct {
	Query  graphql.NullString
	Filter *filterArgs
	Cursor *graphql.ID
	Offset graphql.NullInt
	Limit  graphql.NullInt
}

// query: deals(query, filter, cursor, offset, limit) DealList
func (r *resolver) Deals(ctx context.Context, args dealsArgs) (*dealListResolver, error) {
	offset := 0
	if args.Offset.Set && args.Offset.Value != nil && *args.Offset.Value > 0 {
		offset = int(*args.Offset.Value)
	}

	limit := 10
	if args.Limit.Set && args.Limit.Value != nil && *args.Limit.Value > 0 {
		limit = int(*args.Limit.Value)
	}

	query := ""
	if args.Query.Set && args.Query.Value != nil {
		query = *args.Query.Value
	}

	var filter *db.FilterOptions
	if args.Filter != nil {
		filter = &db.FilterOptions{
			Checkpoint:   args.Filter.Checkpoint.Value,
			IsOffline:    args.Filter.IsOffline.Value,
			TransferType: args.Filter.TransferType.Value,
			IsVerified:   args.Filter.IsVerified.Value,
		}
	}

	deals, count, more, err := r.dealList(ctx, query, filter, args.Cursor, offset, limit)
	if err != nil {
		return nil, err
	}

	resolvers := make([]*dealResolver, 0, len(deals))
	for _, deal := range deals {
		deal.NBytesReceived = int64(r.provider.NBytesReceived(deal.DealUuid))
		resolvers = append(resolvers, newDealResolver(r.mpool, &deal, r.provider, r.dealsDB, r.logsDB, r.spApi, r.curio))
	}

	return &dealListResolver{
		TotalCount: int32(count),
		Deals:      resolvers,
		More:       more,
	}, nil
}

func (r *resolver) DealsCount(ctx context.Context) (int32, error) {
	count, err := r.dealsDB.Count(ctx, "", nil)
	if err != nil {
		return 0, err
	}

	return int32(count), nil
}

// subscription: dealUpdate(id) <-chan Deal
func (r *resolver) DealUpdate(ctx context.Context, args struct{ ID graphql.ID }) (<-chan *dealResolver, error) {
	dealUuid, err := toUuid(args.ID)
	if err != nil {
		return nil, err
	}

	// Send an update to the client with the initial state
	deal, err := r.dealByID(ctx, dealUuid)
	if err != nil {
		return nil, err
	}

	net := make(chan *dealResolver, 1)
	net <- newDealResolver(r.mpool, deal, r.provider, r.dealsDB, r.logsDB, r.spApi, r.curio)

	// Updates to deal state are broadcast on pubsub. Pipe these updates to the
	// client
	dealUpdatesSub, err := r.provider.SubscribeDealUpdates(dealUuid)
	if err != nil {
		if errors.Is(err, storagemarket.ErrDealHandlerNotFound) {
			close(net)
			return net, nil
		}
		return nil, fmt.Errorf("%s: subscribing to deal updates: %w", args.ID, err)
	}
	sub := &subLastUpdate{sub: dealUpdatesSub, provider: r.provider, dealsDB: r.dealsDB, logsDB: r.logsDB, spApi: r.spApi, mpool: r.mpool, curio: r.curio}
	go func() {
		sub.Pipe(ctx, net) // blocks until connection is closed
		close(net)
	}()

	return net, nil
}

type dealNewResolver struct {
	TotalCount int32
	Deal       *dealResolver
}

// subscription: dealNew() <-chan DealNew
func (r *resolver) DealNew(ctx context.Context) (<-chan *dealNewResolver, error) {
	c := make(chan *dealNewResolver, 1)

	sub, err := r.provider.SubscribeNewDeals()
	if err != nil {
		return nil, fmt.Errorf("subscribing to new deal events: %w", err)
	}

	// New deals are broadcast on pubsub. Pipe these deals to the
	// new deal subscription channel returned by this method.
	go func() {
		// When the connection ends, unsubscribe
		defer func() {
			_ = sub.Close()
		}()

		for {
			select {
			case <-ctx.Done():
				// Connection closed
				return

			// New deal
			case evti := <-sub.Out():
				// Pipe the deal to the new deal channel
				di := evti.(types.ProviderDealState)
				rsv := newDealResolver(r.mpool, &di, r.provider, r.dealsDB, r.logsDB, r.spApi, r.curio)
				totalCount, err := r.dealsDB.Count(ctx, "", nil)
				if err != nil {
					log.Errorf("getting total deal count: %w", err)
				}
				dealNew := &dealNewResolver{
					TotalCount: int32(totalCount),
					Deal:       rsv,
				}

				select {
				case <-ctx.Done():
					return

				case c <- dealNew:
				}
			}
		}
	}()

	return c, nil
}

func (r *resolver) isDirectDeal(ctx context.Context, dealUuid uuid.UUID) bool {
	_, err := r.directDealsDB.ByID(ctx, dealUuid)
	return err == nil
}

// mutation: dealCancel(id): ID
func (r *resolver) DealCancel(ctx context.Context, args struct{ ID graphql.ID }) (graphql.ID, error) {
	dealUuid, err := toUuid(args.ID)
	if err != nil {
		return args.ID, err
	}

	deal, err := r.dealsDB.ByID(ctx, dealUuid)
	if err != nil {
		return args.ID, err
	}

	if deal.IsOffline {
		err = r.provider.CancelOfflineDealAwaitingImport(dealUuid)
		return args.ID, err
	}

	err = r.provider.CancelDealDataTransfer(dealUuid)
	return args.ID, err
}

// mutation: dealRetryPaused(id): ID
func (r *resolver) DealRetryPaused(ctx context.Context, args struct{ ID graphql.ID }) (graphql.ID, error) {
	dealUuid, err := toUuid(args.ID)
	if err != nil {
		return args.ID, err
	}

	// Check whether this is a direct deal
	if r.isDirectDeal(ctx, dealUuid) {
		err = r.ddProvider.RetryPausedDeal(ctx, dealUuid)
		return args.ID, err
	}

	err = r.provider.RetryPausedDeal(dealUuid)
	return args.ID, err
}

// mutation: dealFailPaused(id): ID
func (r *resolver) DealFailPaused(ctx context.Context, args struct{ ID graphql.ID }) (graphql.ID, error) {
	dealUuid, err := toUuid(args.ID)
	if err != nil {
		return args.ID, err
	}

	// Check whether this is a direct deal
	if r.isDirectDeal(ctx, dealUuid) {
		err = r.ddProvider.FailPausedDeal(ctx, dealUuid)
		return args.ID, err
	}

	err = r.provider.FailPausedDeal(dealUuid)
	return args.ID, err
}

func (r *resolver) dealByID(ctx context.Context, dealUuid uuid.UUID) (*types.ProviderDealState, error) {
	deal, err := r.dealsDB.ByID(ctx, dealUuid)
	if err != nil {
		return nil, err
	}

	deal.NBytesReceived = int64(r.provider.NBytesReceived(deal.DealUuid))

	return deal, nil
}

func (r *resolver) dealsByPublishCID(ctx context.Context, publishCid cid.Cid) ([]*types.ProviderDealState, error) {
	deals, err := r.dealsDB.ByPublishCID(ctx, publishCid.String())
	if err != nil {
		return nil, err
	}

	for _, d := range deals {
		d.NBytesReceived = int64(r.provider.NBytesReceived(d.DealUuid))
	}

	return deals, nil
}

func (r *resolver) dealList(ctx context.Context, query string, filter *db.FilterOptions, cursor *graphql.ID, offset int, limit int) ([]types.ProviderDealState, int, bool, error) {
	// Fetch one extra deal so that we can check if there are more deals
	// beyond the limit
	deals, err := r.dealsDB.List(ctx, query, filter, cursor, offset, limit+1)
	if err != nil {
		return nil, 0, false, err
	}
	more := len(deals) > limit
	if more {
		// Truncate deal list to limit
		deals = deals[:limit]
	}

	// Get the total deal count
	count, err := r.dealsDB.Count(ctx, query, filter)
	if err != nil {
		return nil, 0, false, err
	}

	// Include data transfer information with the deal
	dis := make([]types.ProviderDealState, 0, len(deals))
	for _, deal := range deals {
		deal.NBytesReceived = int64(r.provider.NBytesReceived(deal.DealUuid))
		dis = append(dis, *deal)
	}

	return dis, count, more, nil
}

type dealResolver struct {
	mpool *mpoolmonitor.MpoolMonitor
	types.ProviderDealState
	provider    *storagemarket.Provider
	transferred uint64
	dealsDB     *db.DealsDB
	logsDB      *db.LogsDB
	spApi       sealingpipeline.API
	curio       bool
}

func newDealResolver(mpool *mpoolmonitor.MpoolMonitor, deal *types.ProviderDealState,
	provider *storagemarket.Provider, dealsDB *db.DealsDB, logsDB *db.LogsDB,
	spApi sealingpipeline.API, curio bool) *dealResolver {
	return &dealResolver{
		mpool:             mpool,
		ProviderDealState: *deal,
		provider:          provider,
		transferred:       uint64(deal.NBytesReceived),
		dealsDB:           dealsDB,
		logsDB:            logsDB,
		spApi:             spApi,
		curio:             curio,
	}
}

func (dr *dealResolver) ID() graphql.ID {
	return graphql.ID(dr.DealUuid.String())
}

func (dr *dealResolver) CreatedAt() graphql.Time {
	return graphql.Time{Time: dr.ProviderDealState.CreatedAt}
}

func (dr *dealResolver) ClientAddress() string {
	return dr.ClientDealProposal.Proposal.Client.String()
}

func (dr *dealResolver) ProviderAddress() string {
	return dr.ClientDealProposal.Proposal.Provider.String()
}

func (dr *dealResolver) IsVerified() bool {
	return dr.ClientDealProposal.Proposal.VerifiedDeal
}

func (dr *dealResolver) KeepUnsealedCopy() bool {
	return dr.FastRetrieval
}

func (dr *dealResolver) AnnounceToIPNI() bool {
	return dr.ProviderDealState.AnnounceToIPNI
}

func (dr *dealResolver) ProposalLabel() (string, error) {
	l := dr.ClientDealProposal.Proposal.Label
	if l.IsString() {
		return l.ToString()
	}
	bz, err := l.ToBytes()
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(bz), nil
}

func (dr *dealResolver) ClientPeerID() string {
	return dr.ProviderDealState.ClientPeerID.String()
}

func (dr *dealResolver) DealDataRoot() string {
	return dr.ProviderDealState.DealDataRoot.String()
}

func (dr *dealResolver) SignedProposalCid() (string, error) {
	cid, err := dr.ProviderDealState.SignedProposalCid()
	if err != nil {
		return "", err
	}
	return cid.String(), err
}

func (dr *dealResolver) PublishCid() string {
	if dr.PublishCID == nil {
		return ""
	}
	return dr.PublishCID.String()
}

func (dr *dealResolver) PieceSize() gqltypes.Uint64 {
	return gqltypes.Uint64(dr.ClientDealProposal.Proposal.PieceSize)
}

func (dr *dealResolver) ChainDealID() gqltypes.Uint64 {
	return gqltypes.Uint64(dr.ProviderDealState.ChainDealID)
}

func (dr *dealResolver) Transferred() gqltypes.Uint64 {
	return gqltypes.Uint64(dr.NBytesReceived)
}

type sectorResolver struct {
	ID     gqltypes.Uint64
	Offset gqltypes.Uint64
	Length gqltypes.Uint64
}

func (dr *dealResolver) Sector() *sectorResolver {
	return &sectorResolver{
		ID:     gqltypes.Uint64(dr.SectorID),
		Offset: gqltypes.Uint64(dr.Offset),
		Length: gqltypes.Uint64(dr.Length),
	}
}

type dealTransfer struct {
	Type     string
	Size     gqltypes.Uint64
	Params   string
	ClientID string
}

func (dr *dealResolver) Transfer() dealTransfer {
	transfer := dr.ProviderDealState.Transfer
	params := "{}"
	if !dr.IsOffline {
		var err error
		params, err = transport.TransferParamsAsJson(transfer)
		if err != nil {
			params = fmt.Sprintf(`{"url": "could not extract url from params: %s"}`, err)
		}
	}
	return dealTransfer{
		Type:     transfer.Type,
		Size:     gqltypes.Uint64(transfer.Size),
		Params:   params,
		ClientID: transfer.ClientID,
	}
}

func (dr *dealResolver) ProviderCollateral() gqltypes.Uint64 {
	return gqltypes.Uint64(dr.ClientDealProposal.Proposal.ProviderCollateral.Int64())
}

func (dr *dealResolver) ClientCollateral() gqltypes.Uint64 {
	return gqltypes.Uint64(dr.ClientDealProposal.Proposal.ClientCollateral.Uint64())
}

func (dr *dealResolver) StoragePricePerEpoch() gqltypes.Uint64 {
	return gqltypes.Uint64(dr.ClientDealProposal.Proposal.StoragePricePerEpoch.Uint64())
}

func (dr *dealResolver) StartEpoch() gqltypes.Uint64 {
	return gqltypes.Uint64(dr.ClientDealProposal.Proposal.StartEpoch)
}

func (dr *dealResolver) EndEpoch() gqltypes.Uint64 {
	return gqltypes.Uint64(dr.ClientDealProposal.Proposal.EndEpoch)
}

func (dr *dealResolver) PieceCid() string {
	return dr.ClientDealProposal.Proposal.PieceCID.String()
}

func (dr *dealResolver) Checkpoint() string {
	return dr.ProviderDealState.Checkpoint.String()
}

func (dr *dealResolver) CheckpointAt() graphql.Time {
	return graphql.Time{Time: dr.ProviderDealState.CheckpointAt}
}

func (dr *dealResolver) Retry() string {
	return string(dr.ProviderDealState.Retry)
}

func (dr *dealResolver) Message(ctx context.Context) string {
	msg := dr.message(ctx, dr.ProviderDealState.Checkpoint, dr.ProviderDealState.CheckpointAt)
	if dr.ProviderDealState.Retry != types.DealRetryFatal && dr.Err != "" {
		msg = "Paused at '" + msg + "': " + dr.Err
	}
	return msg
}

func (dr *dealResolver) message(ctx context.Context, checkpoint dealcheckpoints.Checkpoint, checkpointAt time.Time) string {
	switch checkpoint {
	case dealcheckpoints.Accepted:
		if dr.IsOffline {
			if dr.InboundFilePath != "" {
				return "Verifying Commp"
			}
			return "Awaiting Offline Data Import"
		}

		var pct uint64 = math.MaxUint64
		if dr.ProviderDealState.Transfer.Size > 0 {
			pct = (100 * dr.transferred) / dr.ProviderDealState.Transfer.Size
		}

		switch {
		case dr.transferred == 0 && !dr.provider.IsTransferStalled(dr.DealUuid):
			return "Transfer Queued"
		case pct == 100:
			return "Verifying Commp"
		default:
			isStalled := dr.provider.IsTransferStalled(dr.DealUuid)
			if isStalled {
				if pct == math.MaxUint64 {
					return fmt.Sprintf("Transfer stalled at %s", humanize.Bytes(dr.transferred))
				}
				return fmt.Sprintf("Transfer stalled at %d%%", pct)
			}
			if pct == math.MaxUint64 {
				return fmt.Sprintf("Transferring %s", humanize.Bytes(dr.transferred))
			}
			return fmt.Sprintf("Transferring %d%%", pct)
		}
	case dealcheckpoints.Transferred:
		return "Ready to Publish"
	case dealcheckpoints.Published:
		if *dr.PublishCID == cid.Undef {
			return "Awaiting Message CID"
		}
		found, elapsedEpochs, err := dr.mpool.MsgExecElapsedEpochs(ctx, *dr.PublishCID)
		if found {
			return "Awaiting Message Execution"
		}
		if err != nil {
			return fmt.Sprint(err)
		}
		confidenceEpochs := build.MessageConfidence * 2
		return fmt.Sprintf("Awaiting Publish Confirmation (%d/%d epochs)", elapsedEpochs, confidenceEpochs)
	case dealcheckpoints.PublishConfirmed:
		return "Adding to Sector"
	case dealcheckpoints.AddedPiece:
		if dr.curio {
			return "Waiting for sector to seal"
		}
		return "Indexing"
	case dealcheckpoints.IndexedAndAnnounced:
		return "Indexed and Announced"
	case dealcheckpoints.Complete:
		switch dr.Err {
		case "":
			return "Complete"
		case "Cancelled":
			return "Cancelled"
		}
		return "Error: " + dr.Err
	}
	return checkpoint.String()
}

func (dr *dealResolver) SealingState(ctx context.Context) string {
	if dr.ProviderDealState.Checkpoint < dealcheckpoints.AddedPiece {
		return "To be sealed"
	}
	if dr.ProviderDealState.Checkpoint == dealcheckpoints.Complete {
		return "Complete"
	}
	return dr.sealingState(ctx)
}

func (dr *dealResolver) TransferSamples() []*transferPoint {
	points := dr.provider.Transfer(dr.DealUuid)
	pts := make([]*transferPoint, 0, len(points))
	for _, pt := range points {
		pts = append(pts, &transferPoint{
			At:    graphql.Time{Time: pt.At},
			Bytes: gqltypes.Uint64(pt.Bytes),
		})
	}
	return pts
}

func (dr *dealResolver) IsTransferStalled() bool {
	return dr.provider.IsTransferStalled(dr.DealUuid)
}

func (dr *dealResolver) sealingState(ctx context.Context) string {
	si, err := dr.spApi.SectorsStatus(ctx, dr.SectorID, false)
	if err != nil {
		log.Warnw("error getting sealing status for sector", "sector", dr.SectorID, "error", err)
		return "Sealer: Sealing"
	}
	for _, d := range si.Deals {
		if d == dr.ProviderDealState.ChainDealID {
			return "Sealer: " + string(si.State)
		}
	}
	return fmt.Sprintf("Sealer: failed - deal not found in sector %d", si.SectorID)
}

func (dr *dealResolver) Logs(ctx context.Context) ([]*logsResolver, error) {
	logs, err := dr.logsDB.Logs(ctx, dr.DealUuid)
	if err != nil {
		return nil, err
	}

	logResolvers := make([]*logsResolver, 0, len(logs))
	for _, l := range logs {
		logResolvers = append(logResolvers, &logsResolver{l})
	}
	return logResolvers, nil
}

type logsResolver struct {
	db.DealLog
}

func (lr *logsResolver) DealUUID() graphql.ID {
	return graphql.ID(lr.DealLog.DealUUID.String())
}

func (lr *logsResolver) CreatedAt() graphql.Time {
	return graphql.Time{Time: lr.DealLog.CreatedAt}
}

func (lr *logsResolver) LogLevel() string {
	return lr.DealLog.LogLevel
}

func (lr *logsResolver) LogMsg() string {
	return lr.DealLog.LogMsg
}

func (lr *logsResolver) LogParams() string {
	return lr.DealLog.LogParams
}

func (lr *logsResolver) Subsystem() string {
	return lr.DealLog.Subsystem
}

func toUuid(id graphql.ID) (uuid.UUID, error) {
	var dealUuid uuid.UUID
	err := dealUuid.UnmarshalText([]byte(id))
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("parsing graphql ID '%s' as UUID: %w", id, err)
	}
	return dealUuid, nil
}

type subLastUpdate struct {
	sub      event.Subscription
	provider *storagemarket.Provider
	dealsDB  *db.DealsDB
	logsDB   *db.LogsDB
	spApi    sealingpipeline.API
	mpool    *mpoolmonitor.MpoolMonitor
	curio    bool
}

func (s *subLastUpdate) Pipe(ctx context.Context, net chan *dealResolver) {
	// When the connection ends, unsubscribe from deal update events
	defer func() {
		_ = s.sub.Close()
	}()

	var lastUpdate interface{}
	for {
		// Wait for an update
		select {
		case <-ctx.Done():
			return
		case update, ok := <-s.sub.Out():
			if !ok {
				// Stop listening for updates when the subscription is closed
				return
			}
			lastUpdate = update
		}

		// Each update supersedes the one before it, so read all pending
		// updates that are queued up behind the first one, and only save
		// the very last
		select {
		case update, ok := <-s.sub.Out():
			if ok {
				lastUpdate = update
			}
		default:
		}

		// Attempt to send the update to the network. If the network is
		// blocked, and another update arrives on the subscription,
		// override the latest update.
		updates := s.sub.Out()
	loop:
		for {
			di := lastUpdate.(types.ProviderDealState)
			rsv := newDealResolver(s.mpool, &di, s.provider, s.dealsDB, s.logsDB, s.spApi, s.curio)

			select {
			case <-ctx.Done():
				return
			case net <- rsv:
				break loop
			case update, ok := <-updates:
				if ok {
					lastUpdate = update
				} else {
					updates = nil
				}
			}
		}
	}
}
