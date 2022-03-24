package gql

import (
	"context"
	"fmt"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/fundmanager"
	gqltypes "github.com/filecoin-project/boost/gql/types"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/sealingpipeline"
	"github.com/filecoin-project/boost/storagemanager"
	"github.com/filecoin-project/boost/storagemarket"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	lotus_storagemarket "github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/markets/storageadapter"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	"github.com/google/uuid"
	"github.com/graph-gophers/graphql-go"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/host"
	"golang.org/x/xerrors"
)

type dealListResolver struct {
	TotalCount int32
	Next       *graphql.ID
	Deals      []*dealResolver
}

// resolver translates from a request for a graphql field to the data for
// that field
type resolver struct {
	cfg        *config.Boost
	repo       lotus_repo.LockedRepo
	h          host.Host
	dealsDB    *db.DealsDB
	logsDB     *db.LogsDB
	fundMgr    *fundmanager.FundManager
	storageMgr *storagemanager.StorageManager
	provider   *storagemarket.Provider
	legacyProv lotus_storagemarket.StorageProvider
	legacyDT   lotus_dtypes.ProviderDataTransfer
	publisher  *storageadapter.DealPublisher
	spApi      sealingpipeline.API
	fullNode   v1api.FullNode
}

func NewResolver(cfg *config.Boost, r lotus_repo.LockedRepo, h host.Host, dealsDB *db.DealsDB, logsDB *db.LogsDB, fundMgr *fundmanager.FundManager, storageMgr *storagemanager.StorageManager, spApi sealingpipeline.API, provider *storagemarket.Provider, legacyProv lotus_storagemarket.StorageProvider, legacyDT lotus_dtypes.ProviderDataTransfer, publisher *storageadapter.DealPublisher, fullNode v1api.FullNode) *resolver {
	return &resolver{
		cfg:        cfg,
		repo:       r,
		h:          h,
		dealsDB:    dealsDB,
		logsDB:     logsDB,
		fundMgr:    fundMgr,
		storageMgr: storageMgr,
		provider:   provider,
		legacyProv: legacyProv,
		legacyDT:   legacyDT,
		publisher:  publisher,
		spApi:      spApi,
		fullNode:   fullNode,
	}
}

type storageResolver struct {
	Staged      gqltypes.Uint64
	Transferred gqltypes.Uint64
	Pending     gqltypes.Uint64
	Free        gqltypes.Uint64
	MountPoint  string
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

	return newDealResolver(deal, r.dealsDB, r.logsDB), nil
}

type dealsArgs struct {
	First *graphql.ID
	Limit graphql.NullInt
}

// query: deals(first, limit) DealList
func (r *resolver) Deals(ctx context.Context, args dealsArgs) (*dealListResolver, error) {
	limit := 10
	if args.Limit.Set && args.Limit.Value != nil && *args.Limit.Value > 0 {
		limit = int(*args.Limit.Value)
	}

	deals, count, next, err := r.dealList(ctx, args.First, limit)
	if err != nil {
		return nil, err
	}

	resolvers := make([]*dealResolver, 0, len(deals))
	for _, deal := range deals {
		resolvers = append(resolvers, newDealResolver(&deal, r.dealsDB, r.logsDB))
	}

	var nextID *graphql.ID
	if next != nil {
		gqlid := graphql.ID(next.String())
		nextID = &gqlid
	}
	return &dealListResolver{
		TotalCount: int32(count),
		Next:       nextID,
		Deals:      resolvers,
	}, nil
}

func (r *resolver) DealsCount(ctx context.Context) (int32, error) {
	count, err := r.dealsDB.Count(ctx)
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

	net := make(chan *dealResolver, 1)

	// Send an update to the client with the initial state
	deal, err := r.dealByID(ctx, dealUuid)
	if err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case net <- newDealResolver(deal, r.dealsDB, r.logsDB):
	}

	// Updates to deal state are broadcast on pubsub. Pipe these updates to the
	// client
	dealUpdatesSub, err := r.provider.SubscribeDealUpdates(dealUuid)
	if err != nil {
		if xerrors.Is(err, storagemarket.ErrDealHandlerNotFound) {
			close(net)
			return net, nil
		}
		return nil, xerrors.Errorf("%s: subscribing to deal updates: %w", args.ID, err)
	}
	sub := &subLastUpdate{sub: dealUpdatesSub, dealsDB: r.dealsDB, logsDB: r.logsDB}
	go sub.Pipe(ctx, net)

	return net, nil
}

// subscription: dealNew() <-chan Deal
func (r *resolver) DealNew(ctx context.Context) (<-chan *dealResolver, error) {
	c := make(chan *dealResolver, 1)

	sub, err := r.provider.SubscribeNewDeals()
	if err != nil {
		return nil, xerrors.Errorf("subscribing to new deal events: %w", err)
	}

	// New deals are broadcast on pubsub. Pipe these deals to the
	// new deal subscription channel returned by this method.
	go func() {
		// When the connection ends, unsubscribe
		defer sub.Close()

		for {
			select {
			case <-ctx.Done():
				// Connection closed
				return

			// New deal
			case evti := <-sub.Out():
				// Pipe the deal to the new deal channel
				di := evti.(types.ProviderDealState)
				rsv := newDealResolver(&di, r.dealsDB, r.logsDB)

				select {
				case <-ctx.Done():
					return

				case c <- rsv:
				}
			}
		}
	}()

	return c, nil
}

// mutation: dealCancel(id): ID
func (r *resolver) DealCancel(_ context.Context, args struct{ ID graphql.ID }) (graphql.ID, error) {
	dealUuid, err := toUuid(args.ID)
	if err != nil {
		return args.ID, err
	}

	err = r.provider.CancelDealDataTransfer(dealUuid)
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

func (r *resolver) dealByPublishCID(ctx context.Context, publishCid *cid.Cid) (*types.ProviderDealState, error) {
	deal, err := r.dealsDB.ByPublishCID(ctx, publishCid.String())
	if err != nil {
		return nil, err
	}

	deal.NBytesReceived = int64(r.provider.NBytesReceived(deal.DealUuid))

	return deal, nil
}

func (r *resolver) dealList(ctx context.Context, first *graphql.ID, limit int) ([]types.ProviderDealState, int, *uuid.UUID, error) {
	// Get one extra deal so we can get the first deal UUID of the next page
	allDeals, err := r.dealsDB.List(ctx, first, limit+1)
	if err != nil {
		return nil, 0, nil, err
	}

	deals := allDeals
	var nextDealUuid *uuid.UUID
	// If there was more than one page of deals available
	if len(allDeals) > limit {
		// Get the first deal UUID of the next page
		nextDealUuid = &allDeals[len(allDeals)-1].DealUuid
		// Filter for deals on this page
		deals = allDeals[:limit]
	}

	// Get the total deal count
	count, err := r.dealsDB.Count(ctx)
	if err != nil {
		return nil, 0, nil, err
	}

	// Include data transfer information with the deal
	dis := make([]types.ProviderDealState, 0, len(deals))
	for _, deal := range deals {
		deal.NBytesReceived = int64(r.provider.NBytesReceived(deal.DealUuid))
		dis = append(dis, *deal)
	}

	return dis, count, nextDealUuid, nil
}

type dealResolver struct {
	types.ProviderDealState
	transferred uint64
	dealsDB     *db.DealsDB
	logsDB      *db.LogsDB
}

func newDealResolver(deal *types.ProviderDealState, dealsDB *db.DealsDB, logsDB *db.LogsDB) *dealResolver {
	return &dealResolver{
		ProviderDealState: *deal,
		transferred:       uint64(deal.NBytesReceived),
		dealsDB:           dealsDB,
		logsDB:            logsDB,
	}
}

func (dr *dealResolver) ID() graphql.ID {
	return graphql.ID(dr.ProviderDealState.DealUuid.String())
}

func (dr *dealResolver) CreatedAt() graphql.Time {
	return graphql.Time{Time: dr.ProviderDealState.CreatedAt}
}

func (dr *dealResolver) ClientAddress() string {
	return dr.ProviderDealState.ClientDealProposal.Proposal.Client.String()
}

func (dr *dealResolver) ProviderAddress() string {
	return dr.ProviderDealState.ClientDealProposal.Proposal.Provider.String()
}

func (dr *dealResolver) IsVerified() bool {
	return dr.ProviderDealState.ClientDealProposal.Proposal.VerifiedDeal
}

func (dr *dealResolver) ProposalLabel() string {
	return dr.ProviderDealState.ClientDealProposal.Proposal.Label
}

func (dr *dealResolver) ClientPeerID() string {
	return dr.ProviderDealState.ClientPeerID.String()
}

func (dr *dealResolver) DealDataRoot() string {
	return dr.ProviderDealState.DealDataRoot.String()
}

func (dr *dealResolver) PublishCid() string {
	if dr.ProviderDealState.PublishCID == nil {
		return ""
	}
	return dr.ProviderDealState.PublishCID.String()
}

func (dr *dealResolver) PieceSize() gqltypes.Uint64 {
	return gqltypes.Uint64(dr.ProviderDealState.ClientDealProposal.Proposal.PieceSize)
}

func (dr *dealResolver) ChainDealID() gqltypes.Uint64 {
	return gqltypes.Uint64(dr.ProviderDealState.ChainDealID)
}

func (dr *dealResolver) Transferred() gqltypes.Uint64 {
	return gqltypes.Uint64(dr.ProviderDealState.NBytesReceived)
}

type sectorResolver struct {
	ID     gqltypes.Uint64
	Offset gqltypes.Uint64
	Length gqltypes.Uint64
}

func (dr *dealResolver) Sector() *sectorResolver {
	return &sectorResolver{
		ID:     gqltypes.Uint64(dr.ProviderDealState.SectorID),
		Offset: gqltypes.Uint64(dr.ProviderDealState.Offset),
		Length: gqltypes.Uint64(dr.ProviderDealState.Length),
	}
}

type dealTransfer struct {
	Type   string
	Size   gqltypes.Uint64
	Params string
}

func (dr *dealResolver) Transfer() dealTransfer {
	return dealTransfer{
		Type:   dr.ProviderDealState.Transfer.Type,
		Size:   gqltypes.Uint64(dr.ProviderDealState.Transfer.Size),
		Params: "TODO",
	}
}

func (dr *dealResolver) ProviderCollateral() gqltypes.Uint64 {
	return gqltypes.Uint64(dr.ProviderDealState.ClientDealProposal.Proposal.ProviderCollateral.Int64())
}

func (dr *dealResolver) ClientCollateral() gqltypes.Uint64 {
	return gqltypes.Uint64(dr.ProviderDealState.ClientDealProposal.Proposal.ClientCollateral.Uint64())
}

func (dr *dealResolver) StoragePricePerEpoch() gqltypes.Uint64 {
	return gqltypes.Uint64(dr.ProviderDealState.ClientDealProposal.Proposal.StoragePricePerEpoch.Uint64())
}

func (dr *dealResolver) StartEpoch() gqltypes.Uint64 {
	return gqltypes.Uint64(dr.ProviderDealState.ClientDealProposal.Proposal.StartEpoch)
}

func (dr *dealResolver) EndEpoch() gqltypes.Uint64 {
	return gqltypes.Uint64(dr.ProviderDealState.ClientDealProposal.Proposal.EndEpoch)
}

func (dr *dealResolver) PieceCid() string {
	return dr.ProviderDealState.ClientDealProposal.Proposal.PieceCID.String()
}

func (dr *dealResolver) Stage() string {
	return dr.ProviderDealState.Checkpoint.String()
}

func (dr *dealResolver) Message() string {
	switch dr.Checkpoint {
	case dealcheckpoints.Accepted:
		switch dr.transferred {
		case 0:
			return "Transfer queued"
		case 100:
			return "Transfer Complete"
		default:
			pct := (100 * dr.transferred) / dr.ProviderDealState.Transfer.Size
			return fmt.Sprintf("Transferring %d%%", pct)
		}
	case dealcheckpoints.Transferred:
		return "Ready to Publish"
	case dealcheckpoints.Published:
		return "Awaiting Publish Confirmation"
	case dealcheckpoints.PublishConfirmed:
		return "Adding to Sector"
	case dealcheckpoints.AddedPiece:
		return "Announcing"
	case dealcheckpoints.IndexedAndAnnounced:
		return "Sealing"
	case dealcheckpoints.Complete:
		switch dr.Err {
		case "":
			return "Complete"
		case "Cancelled":
			return "Cancelled"
		}
		return "Error: " + dr.Err
	}
	return dr.ProviderDealState.Checkpoint.String()
}

func (dr *dealResolver) Logs(ctx context.Context) ([]*logsResolver, error) {
	logs, err := dr.logsDB.Logs(ctx, dr.ProviderDealState.DealUuid)
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
		return uuid.UUID{}, xerrors.Errorf("parsing graphql ID '%s' as UUID: %w", id, err)
	}
	return dealUuid, nil
}

type subLastUpdate struct {
	sub     event.Subscription
	dealsDB *db.DealsDB
	logsDB  *db.LogsDB
}

func (s *subLastUpdate) Pipe(ctx context.Context, net chan *dealResolver) {
	// When the connection ends, unsubscribe
	defer s.sub.Close()

	var lastUpdate interface{}
	for {
		// Wait for an update
		select {
		case <-ctx.Done():
			return
		case update := <-s.sub.Out():
			lastUpdate = update
		}

		// Each update supersedes the one before it, so read all pending
		// updates that are queued up behind the first one, and only save
		// the very last
		select {
		case update := <-s.sub.Out():
			lastUpdate = update
		default:
		}

		// Attempt to send the update to the network. If the network is
		// blocked, and another update arrives on the subscription,
		// override the latest update.
	loop:
		for {
			di := lastUpdate.(types.ProviderDealState)
			rsv := newDealResolver(&di, s.dealsDB, s.logsDB)

			select {
			case <-ctx.Done():
				return
			case net <- rsv:
				break loop
			case update := <-s.sub.Out():
				lastUpdate = update
			}
		}
	}
}
