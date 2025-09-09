package gql

import (
	"context"
	"fmt"

	"github.com/dustin/go-humanize"
	gqltypes "github.com/filecoin-project/boost/gql/types"
	"github.com/filecoin-project/boost/storagemarket/types/legacytypes"
	"github.com/graph-gophers/graphql-go"
	"github.com/ipfs/go-cid"
)

type legacyDealResolver struct {
	legacytypes.MinerDeal
	transferred uint64
}

type legacyDealListResolver struct {
	TotalCount int32
	More       bool
	Deals      []*legacyDealResolver
}

func (r *resolver) LegacyDeal(ctx context.Context, args struct{ ID graphql.ID }) (*legacyDealResolver, error) {
	signedPropCid, err := cid.Parse(string(args.ID))
	if err != nil {
		return nil, fmt.Errorf("parsing deal signed proposal cid %s: %w", args.ID, err)
	}

	dl, err := r.legacyDeals.ByPropCid(signedPropCid)
	if err != nil {
		return nil, fmt.Errorf("getting deal with signed proposal cid %s: %w", args.ID, err)
	}

	return r.withTransferState(ctx, dl), nil
}

func (r *resolver) withTransferState(ctx context.Context, dl legacytypes.MinerDeal) *legacyDealResolver {
	dr := &legacyDealResolver{MinerDeal: dl}
	return dr
}

// query: legacyDeals(query, cursor, offset, limit) DealList
func (r *resolver) LegacyDeals(ctx context.Context, args dealsArgs) (*legacyDealListResolver, error) {
	offset := 0
	if args.Offset.Set && args.Offset.Value != nil && *args.Offset.Value > 0 {
		offset = int(*args.Offset.Value)
	}

	limit := 10
	if args.Limit.Set && args.Limit.Value != nil && *args.Limit.Value > 0 {
		limit = int(*args.Limit.Value)
	}

	var startPropCid *cid.Cid
	if args.Cursor != nil {
		signedPropCid, err := cid.Parse(string(*args.Cursor))
		if err != nil {
			return nil, fmt.Errorf("parsing offset signed proposal cid %s: %w", *args.Cursor, err)
		}
		startPropCid = &signedPropCid
	}

	// Get the total number of deals
	dealCount, err := r.legacyDeals.DealCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting deal count: %w", err)
	}

	var more bool
	var pageDeals []legacytypes.MinerDeal
	if args.Query.Value != nil && *args.Query.Value != "" {
		// If there is a search query, assume the query is the deal
		// proposal cid and try to fetch the corresponding deal
		propCidQuery, err := cid.Parse(*args.Query.Value)
		if err == nil {
			dl, err := r.legacyDeals.ByPropCid(propCidQuery)
			if err == nil {
				pageDeals = []legacytypes.MinerDeal{dl}
			}
		}
	} else {
		// Get a page worth of deals, plus one extra so we can see if there are more deals
		pageDeals, err = r.legacyDeals.ListLocalDealsPage(startPropCid, offset, limit+1)
		if err != nil {
			return nil, fmt.Errorf("getting page of deals: %w", err)
		}

		// If there is another page of deals available
		more = len(pageDeals) > limit
		if more {
			// Filter for deals on the current page
			pageDeals = pageDeals[:limit]
		}
	}

	resolvers := make([]*legacyDealResolver, 0, len(pageDeals))
	for _, deal := range pageDeals {
		resolvers = append(resolvers, r.withTransferState(ctx, deal))
	}

	return &legacyDealListResolver{
		TotalCount: int32(dealCount),
		More:       more,
		Deals:      resolvers,
	}, nil
}

func (r *resolver) LegacyDealsCount(ctx context.Context) (int32, error) {
	dealCount, err := r.legacyDeals.DealCount(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting deal count: %w", err)
	}
	return int32(dealCount), nil
}

func (r *legacyDealResolver) ID() (graphql.ID, error) {
	return graphql.ID(r.ProposalCid.String()), nil
}

func (r *legacyDealResolver) CreatedAt() (graphql.Time, error) {
	return graphql.Time{Time: r.CreationTime.Time()}, nil
}

func (r *legacyDealResolver) ClientAddress() (string, error) {
	return r.Proposal.Client.String(), nil
}

func (r *legacyDealResolver) ProviderAddress() (string, error) {
	return r.Proposal.Provider.String(), nil
}

func (r *legacyDealResolver) ClientPeerID() string {
	return r.Client.String()
}

func (r *legacyDealResolver) DealDataRoot() string {
	return r.Ref.Root.String()
}

func (r *legacyDealResolver) PublishCid() string {
	if r.MinerDeal.PublishCid == nil {
		return ""
	}
	return r.MinerDeal.PublishCid.String()
}

func (r *legacyDealResolver) PieceSize() gqltypes.Uint64 {
	return gqltypes.Uint64(r.Proposal.PieceSize)
}

func (r *legacyDealResolver) PiecePath() string {
	return string(r.MinerDeal.PiecePath)
}

func (r *legacyDealResolver) SectorNumber() gqltypes.Uint64 {
	return gqltypes.Uint64(r.MinerDeal.SectorNumber)
}

func (r *legacyDealResolver) ProviderCollateral() gqltypes.Uint64 {
	return gqltypes.Uint64(r.Proposal.ProviderCollateral.Uint64())
}

func (r *legacyDealResolver) StartEpoch() gqltypes.Uint64 {
	return gqltypes.Uint64(r.Proposal.StartEpoch)
}

func (r *legacyDealResolver) EndEpoch() gqltypes.Uint64 {
	return gqltypes.Uint64(r.Proposal.EndEpoch)
}

func (r *legacyDealResolver) FundsReserved() gqltypes.Uint64 {
	return gqltypes.Uint64(r.MinerDeal.FundsReserved.Uint64())
}

func (r *legacyDealResolver) PieceCid() string {
	return r.Proposal.PieceCID.String()
}

func (r *legacyDealResolver) TransferType() string {
	return r.Ref.TransferType
}

func (r *legacyDealResolver) TransferSize() gqltypes.Uint64 {
	return gqltypes.Uint64(r.Ref.RawBlockSize)
}

func (r *legacyDealResolver) TransferChannelID() *string {
	if r.TransferChannelId == nil {
		return nil
	}
	chid := r.TransferChannelId.String()
	return &chid
}

func (r *legacyDealResolver) Transferred() gqltypes.Uint64 {
	return gqltypes.Uint64(r.transferred)
}

func (r *legacyDealResolver) ChainDealID() gqltypes.Uint64 {
	return gqltypes.Uint64(r.DealID)
}

func (r *legacyDealResolver) InboundCARPath() string {
	return r.InboundCAR
}

func (r *legacyDealResolver) Status() string {
	return legacytypes.DealStates[r.State]
}

func (r *legacyDealResolver) Message() string {
	if r.MinerDeal.Message == "" && r.State == legacytypes.StorageDealTransferring {
		switch r.transferred {
		case 0:
			return "Transferring"
		case 100:
			return "Transfer Complete"
		default:
			if r.Ref.RawBlockSize > 0 {
				pct := (100 * r.transferred) / r.Ref.RawBlockSize
				return fmt.Sprintf("Transferring: %s (%d%%)", humanize.IBytes(r.transferred), pct)
			} else {
				return fmt.Sprintf("Transferring: %s", humanize.IBytes(r.transferred))
			}
		}
	}
	return r.MinerDeal.Message
}

func (r *legacyDealResolver) IsVerified() bool {
	return r.Proposal.VerifiedDeal
}
