package gql

import (
	"fmt"
	"sort"

	gqltypes "github.com/filecoin-project/boost/gql/types"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/graph-gophers/graphql-go"
)

type legacyDealResolver struct {
	storagemarket.MinerDeal
}

type legacyDealListResolver struct {
	TotalCount int32
	Next       *graphql.ID
	Deals      []*legacyDealResolver
}

func (r *resolver) LegacyDeal(args struct{ ID graphql.ID }) (*legacyDealResolver, error) {
	allDeals, err := r.legacyProv.ListLocalDeals()
	if err != nil {
		return nil, fmt.Errorf("getting legacy deals: %w", err)
	}

	for _, dl := range allDeals {
		if dl.ProposalCid.String() == string(args.ID) {
			return &legacyDealResolver{MinerDeal: dl}, nil
		}
	}

	return nil, fmt.Errorf("deal with id '%s' not found", args.ID)
}

// query: legacyDeals(first, limit) DealList
func (r *resolver) LegacyDeals(args dealsArgs) (*legacyDealListResolver, error) {
	limit := 10
	if args.Limit.Set && args.Limit.Value != nil && *args.Limit.Value > 0 {
		limit = int(*args.Limit.Value)
	}

	// Get all deals in descending order by creation time
	allDeals, err := r.legacyProv.ListLocalDeals()
	if err != nil {
		return nil, fmt.Errorf("getting legacy deals: %w", err)
	}
	sort.Slice(allDeals, func(i, j int) bool {
		return allDeals[j].CreationTime.Time().Before(allDeals[i].CreationTime.Time())
	})

	// Skip over deals until the first deal in the given page
	pageDeals := allDeals
	if args.First != nil {
		propcid := string(*args.First)
		first := -1
		for i, dl := range allDeals {
			if dl.ProposalCid.String() == propcid {
				first = i
				break
			}
		}

		if first >= 0 {
			pageDeals = allDeals[first:]
		}
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
		resolvers = append(resolvers, &legacyDealResolver{
			MinerDeal: deal,
		})
	}

	var nextID *graphql.ID
	if next != nil {
		gqlid := graphql.ID(*next)
		nextID = &gqlid
	}
	return &legacyDealListResolver{
		TotalCount: int32(len(allDeals)),
		Next:       nextID,
		Deals:      resolvers,
	}, nil
}

func (r *resolver) LegacyDealsCount() (int32, error) {
	allDeals, err := r.legacyProv.ListLocalDeals()
	if err != nil {
		return 0, fmt.Errorf("getting legacy deals: %w", err)
	}
	return int32(len(allDeals)), nil
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

func (r *legacyDealResolver) ProviderCollateral() float64 {
	return float64(r.ClientDealProposal.Proposal.ProviderCollateral.Int64())
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
	return r.Status()
}
