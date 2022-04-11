package gql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/filecoin-project/boost/gql/types"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/graph-gophers/graphql-go"
	"golang.org/x/xerrors"
)

// basicDealResolver just has simple types (as opposed to dealResolver which
// has methods with logic)
type basicDealResolver struct {
	ID                 graphql.ID
	IsLegacy           bool
	ClientAddress      string
	ProviderAddress    string
	CreatedAt          graphql.Time
	PieceCid           string
	PieceSize          types.Uint64
	ProviderCollateral types.Uint64
	StartEpoch         types.Uint64
	EndEpoch           types.Uint64
	ClientPeerID       string
	DealDataRoot       string
	PublishCid         string
	Transfer           dealTransfer
	Message            string
}

type dealPublishResolver struct {
	Start          graphql.Time
	Period         int32
	MaxDealsPerMsg int32
	Deals          []*basicDealResolver
}

// query: dealPublish: DealPublish
func (r *resolver) DealPublish(ctx context.Context) (*dealPublishResolver, error) {
	// Get deals pending publish from deal publisher
	pending := r.publisher.PendingDeals()

	legacyDealIDs := make(map[string]struct{}, len(pending.Deals))
	basicDeals := make([]*basicDealResolver, 0, len(pending.Deals))
	for _, dp := range pending.Deals {
		signedProp, err := cborutil.AsIpld(&dp)
		if err != nil {
			return nil, xerrors.Errorf("failed to compute signed deal proposal ipld node: %w", err)
		}

		// Look up the deal by signed proposal CID in the Boost database
		signedPropCid := signedProp.Cid()
		deal, err := r.dealsDB.BySignedProposalCID(ctx, signedPropCid)
		if err == nil {
			prop := deal.ClientDealProposal.Proposal
			pubCid := ""
			if deal.PublishCID != nil {
				pubCid = deal.PublishCID.String()
			}
			basicDeals = append(basicDeals, &basicDealResolver{
				IsLegacy:           false,
				ID:                 graphql.ID(deal.DealUuid.String()),
				ClientAddress:      prop.Client.String(),
				ProviderAddress:    prop.Provider.String(),
				CreatedAt:          graphql.Time{Time: deal.CreatedAt},
				PieceCid:           prop.PieceCID.String(),
				PieceSize:          types.Uint64(prop.PieceSize),
				ProviderCollateral: types.Uint64(prop.ProviderCollateral.Uint64()),
				StartEpoch:         types.Uint64(prop.StartEpoch),
				EndEpoch:           types.Uint64(prop.EndEpoch),
				ClientPeerID:       deal.ClientPeerID.String(),
				DealDataRoot:       deal.DealDataRoot.String(),
				PublishCid:         pubCid,
				Transfer: dealTransfer{
					Type:   deal.Transfer.Type,
					Size:   types.Uint64(deal.Transfer.Size),
					Params: "TODO",
				},
				Message: deal.Checkpoint.String(),
			})

			continue
		}

		if !xerrors.Is(err, sql.ErrNoRows) {
			return nil, xerrors.Errorf("getting deal from DB by signed proposal cid: %w", err)
		}

		// The deal is not in the Boost database so look it up in the legacy
		// database
		propCid, err := dp.Proposal.Cid()
		if err != nil {
			return nil, xerrors.Errorf("getting proposal cid: %w", err)
		}
		legacyDealIDs[propCid.String()+string(dp.ClientSignature.Data)] = struct{}{}
	}

	// If there are any legacy deals to look up
	if len(legacyDealIDs) > 0 {
		// Get all deals from the legacy provider
		legacyDeals, err := r.legacyProv.ListLocalDeals()
		if err != nil {
			return nil, fmt.Errorf("getting legacy deals: %w", err)
		}

		// For each legacy deal, check if it matches a deal we're looking for
		for _, ld := range legacyDeals {
			propCid, err := ld.Proposal.Cid()
			if err != nil {
				return nil, xerrors.Errorf("getting proposal cid: %w", err)
			}

			// Match
			if _, ok := legacyDealIDs[propCid.String()+string(ld.ClientSignature.Data)]; ok {
				signedProp, err := cborutil.AsIpld(&ld.ClientDealProposal)
				if err != nil {
					return nil, xerrors.Errorf("failed to compute signed deal proposal ipld node: %w", err)
				}

				prop := ld.Proposal
				pubCid := ""
				if ld.PublishCid != nil {
					pubCid = ld.PublishCid.String()
				}
				basicDeals = append(basicDeals, &basicDealResolver{
					IsLegacy:           true,
					ID:                 graphql.ID(signedProp.Cid().String()),
					ClientAddress:      prop.Client.String(),
					ProviderAddress:    prop.Provider.String(),
					CreatedAt:          graphql.Time{Time: ld.CreationTime.Time()},
					PieceCid:           prop.PieceCID.String(),
					PieceSize:          types.Uint64(prop.PieceSize),
					ProviderCollateral: types.Uint64(prop.ProviderCollateral.Uint64()),
					StartEpoch:         types.Uint64(prop.StartEpoch),
					EndEpoch:           types.Uint64(prop.EndEpoch),
					ClientPeerID:       ld.Client.String(),
					DealDataRoot:       ld.Ref.Root.String(),
					PublishCid:         pubCid,
					Transfer: dealTransfer{
						Type:   "graphsync",
						Size:   types.Uint64(ld.Ref.RawBlockSize),
						Params: "TODO",
					},
					Message: ld.Message,
				})
			}
		}
	}

	return &dealPublishResolver{
		Deals:          basicDeals,
		Period:         int32(pending.PublishPeriod.Seconds()),
		Start:          graphql.Time{Time: pending.PublishPeriodStart},
		MaxDealsPerMsg: int32(r.cfg.LotusDealmaking.MaxDealsPerPublishMsg),
	}, nil
}

// mutation: dealPublishNow(): bool
func (r *resolver) DealPublishNow(ctx context.Context) (bool, error) {
	r.publisher.ForcePublishPendingDeals()
	return true, nil
}
