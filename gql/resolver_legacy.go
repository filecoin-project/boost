package gql

import (
	"context"
	"fmt"

	"github.com/dustin/go-humanize"
	gqltypes "github.com/filecoin-project/boost/gql/types"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/graph-gophers/graphql-go"
	"github.com/ipfs/go-cid"
)

type legacyDealResolver struct {
	storagemarket.MinerDeal
	transferred uint64
}

type legacyDealListResolver struct {
	TotalCount int32
	Next       *graphql.ID
	Deals      []*legacyDealResolver
}

func (r *resolver) LegacyDeal(ctx context.Context, args struct{ ID graphql.ID }) (*legacyDealResolver, error) {
	signedPropCid, err := cid.Parse(string(args.ID))
	if err != nil {
		return nil, fmt.Errorf("parsing deal signed proposal cid %s: %w", args.ID, err)
	}

	dl, err := r.legacyProv.GetLocalDeal(signedPropCid)
	if err != nil {
		return nil, fmt.Errorf("getting deal with signed proposal cid %s: %w", args.ID, err)
	}

	return r.withTransferState(ctx, dl), nil
}

func (r *resolver) withTransferState(ctx context.Context, dl storagemarket.MinerDeal) *legacyDealResolver {
	dr := &legacyDealResolver{MinerDeal: dl}
	if dl.State == storagemarket.StorageDealTransferring && dl.TransferChannelId != nil {
		st, err := r.legacyDT.ChannelState(ctx, *dl.TransferChannelId)
		if err != nil {
			log.Warnw("getting transfer channel id %s: %s", *dl.TransferChannelId, err)
		} else {
			dr.transferred = st.Received()
		}
	}
	return dr
}

// query: legacyDeals(first, limit) DealList
func (r *resolver) LegacyDeals(ctx context.Context, args dealsArgs) (*legacyDealListResolver, error) {
	limit := 10
	if args.Limit.Set && args.Limit.Value != nil && *args.Limit.Value > 0 {
		limit = int(*args.Limit.Value)
	}

	var offsetPropCid *cid.Cid
	if args.First != nil {
		signedPropCid, err := cid.Parse(string(*args.First))
		if err != nil {
			return nil, fmt.Errorf("parsing offset signed proposal cid %s: %w", *args.First, err)
		}
		offsetPropCid = &signedPropCid
	}

	// Get the total number of deals
	dealCount, err := r.legacyProv.LocalDealCount()
	if err != nil {
		return nil, fmt.Errorf("getting deal count: %w", err)
	}

	// Get a page worth of deals, plus one extra so we can get a "next" cursor
	pageDeals, err := r.legacyProv.ListLocalDealsPage(offsetPropCid, limit+1)
	if err != nil {
		return nil, fmt.Errorf("getting page of deals: %w", err)
	}

	// If there is another page of deals available
	var next *string
	if len(pageDeals) > limit {
		// Get the cursor at the start of the next page
		propCid := pageDeals[limit].ProposalCid.String()
		next = &propCid
		// Filter for deals on the current page
		pageDeals = pageDeals[:limit]
	}

	resolvers := make([]*legacyDealResolver, 0, len(pageDeals))
	for _, deal := range pageDeals {
		resolvers = append(resolvers, r.withTransferState(ctx, deal))
	}

	var nextID *graphql.ID
	if next != nil {
		gqlid := graphql.ID(*next)
		nextID = &gqlid
	}
	return &legacyDealListResolver{
		TotalCount: int32(dealCount),
		Next:       nextID,
		Deals:      resolvers,
	}, nil
}

func (r *resolver) LegacyDealsCount() (int32, error) {
	dealCount, err := r.legacyProv.LocalDealCount()
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
	return gqltypes.Uint64(r.ClientDealProposal.Proposal.PieceSize)
}

func (r *legacyDealResolver) SectorNumber() gqltypes.Uint64 {
	return gqltypes.Uint64(r.MinerDeal.SectorNumber)
}

func (r *legacyDealResolver) ProviderCollateral() gqltypes.Uint64 {
	return gqltypes.Uint64(r.ClientDealProposal.Proposal.ProviderCollateral.Int64())
}

func (r *legacyDealResolver) StartEpoch() gqltypes.Uint64 {
	return gqltypes.Uint64(r.ClientDealProposal.Proposal.StartEpoch)
}

func (r *legacyDealResolver) EndEpoch() gqltypes.Uint64 {
	return gqltypes.Uint64(r.ClientDealProposal.Proposal.EndEpoch)
}

func (r *legacyDealResolver) PieceCid() string {
	return r.ClientDealProposal.Proposal.PieceCID.String()
}

func (r *legacyDealResolver) TransferType() string {
	return r.Ref.TransferType
}

func (r *legacyDealResolver) TransferSize() gqltypes.Uint64 {
	return gqltypes.Uint64(r.Ref.RawBlockSize)
}

func (r *legacyDealResolver) Status() string {
	return storagemarket.DealStates[r.State]
}

func (r *legacyDealResolver) Message() string {
	if r.MinerDeal.Message == "" && r.State == storagemarket.StorageDealTransferring {
		switch r.transferred {
		case 0:
			return "Transferring"
		case 100:
			return "Transfer Complete"
		default:
			if r.Ref.RawBlockSize > 0 {
				pct := (100 * r.transferred) / r.Ref.RawBlockSize
				return fmt.Sprintf("Transferring: %s (%d%%)", humanize.Bytes(r.transferred), pct)
			} else {
				return fmt.Sprintf("Transferring: %s", humanize.Bytes(r.transferred))
			}
		}
	}
	return r.MinerDeal.Message
}
