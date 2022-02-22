package gql

import (
	"fmt"
	"math/rand"
	"time"

	gqltypes "github.com/filecoin-project/boost/gql/types"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/graph-gophers/graphql-go"
	cbg "github.com/whyrusleeping/cbor-gen"
)

var localDeals []storagemarket.MinerDeal

func init() {
	var err error
	localDeals, err = mockListLocalDeals()
	if err != nil {
		panic(err)
	}
}

type legacyDealResolver struct {
	storagemarket.MinerDeal
}

type legacyDealListResolver struct {
	TotalCount int32
	Next       *graphql.ID
	Deals      []*legacyDealResolver
}

func (r *resolver) LegacyDeal(args struct{ ID graphql.ID }) (*legacyDealResolver, error) {
	for _, dl := range localDeals {
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
	//allDeals, err := r.legacyProv.ListLocalDeals()
	//if err != nil {
	//	return nil, err
	//}

	allDeals := localDeals
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
	//allDeals, err := r.legacyProv.ListLocalDeals()
	//allDeals, err := mockListLocalDeals()
	//if err != nil {
	//	return 0, err
	//}
	allDeals := localDeals

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

func mockListLocalDeals() ([]storagemarket.MinerDeal, error) {
	clientAddr, err := address.NewFromString("t01001")
	if err != nil {
		return nil, err
	}
	provAddr, err := address.NewFromString("t01000")
	if err != nil {
		return nil, err
	}
	pieceCid := testutil.GenerateCid()

	deals := []storagemarket.MinerDeal{}
	for i := 0; i < 30; i++ {
		deal := storagemarket.MinerDeal{
			ClientDealProposal: market.ClientDealProposal{
				Proposal: market.DealProposal{
					PieceCID:             testutil.GenerateCid(),
					PieceSize:            abi.PaddedPieceSize(8 * 1024 * 1024 * 1024),
					VerifiedDeal:         false,
					Client:               clientAddr,
					Provider:             provAddr,
					Label:                "",
					StartEpoch:           31231,
					EndEpoch:             38132,
					StoragePricePerEpoch: abi.NewTokenAmount(1024),
					ProviderCollateral:   abi.NewTokenAmount(1024 * 8),
					ClientCollateral:     abi.NewTokenAmount(0),
				},
			},
			ProposalCid: testutil.GenerateCid(),
			Miner:       "miner1",
			Client:      "client1",
			State:       storagemarket.StorageDealActive,
			Message:     "Storage deal is active",
			Ref: &storagemarket.DataRef{
				TransferType: "graphsync",
				PieceCid:     &pieceCid,
				PieceSize:    abi.UnpaddedPieceSize(8 * 1024 * 1024 * 1024),
				RawBlockSize: 1024 * 1024 * 1024,
			},
			DealID:       abi.DealID(rand.Intn(1024)),
			SectorNumber: abi.SectorNumber(rand.Intn(1024)),
			CreationTime: cbg.CborTime(time.Now().Add(-time.Hour + time.Duration(i)*time.Minute)),
		}
		deals = append(deals, deal)
	}
	return deals, nil
}
