package pdcleaner

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/lib/legacy"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	verifregtypes "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/actors/adt"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	cbor "github.com/ipfs/go-ipld-cbor"
	logging "github.com/ipfs/go-log/v2"
	"go.uber.org/fx"
	"golang.org/x/net/context"
)

var log = logging.Logger("pdcleaner")

type PieceDirectoryCleanup interface {
	Start(ctx context.Context)
	CleanOnce() error
}

type pdcleaner struct {
	ctx             context.Context
	miner           address.Address
	dealsDB         *db.DealsDB
	directDealsDB   *db.DirectDealsDB
	legacyDeals     legacy.LegacyDealManager
	pd              *piecedirectory.PieceDirectory
	full            v1api.FullNode
	startOnce       sync.Once
	lk              sync.Mutex
	cleanupInterval time.Duration
}

func NewPieceDirectoryCleaner(cfg *config.Boost) func(lc fx.Lifecycle, dealsDB *db.DealsDB, directDealsDB *db.DirectDealsDB, legacyDeals legacy.LegacyDealManager, pd *piecedirectory.PieceDirectory, full v1api.FullNode) PieceDirectoryCleanup {
	return func(lc fx.Lifecycle, dealsDB *db.DealsDB, directDealsDB *db.DirectDealsDB, legacyDeals legacy.LegacyDealManager, pd *piecedirectory.PieceDirectory, full v1api.FullNode) PieceDirectoryCleanup {

		// Don't start cleanup loop if duration is '0s'
		if time.Duration(cfg.LocalIndexDirectory.LidCleanupInterval).Seconds() == 0 {
			return nil
		}

		pdc := newPDC(dealsDB, directDealsDB, legacyDeals, pd, full, time.Duration(cfg.LocalIndexDirectory.LidCleanupInterval))

		cctx, cancel := context.WithCancel(context.Background())

		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				mid, err := address.NewFromString(cfg.Wallets.Miner)
				if err != nil {
					return fmt.Errorf("failed to parse the miner ID %s: %w", cfg.Wallets.Miner, err)
				}
				pdc.miner = mid
				go pdc.Start(cctx)
				return nil
			},
			OnStop: func(ctx context.Context) error {
				cancel()
				return nil
			},
		})

		return pdc

	}
}

func newPDC(dealsDB *db.DealsDB, directDealsDB *db.DirectDealsDB, legacyDeals legacy.LegacyDealManager, pd *piecedirectory.PieceDirectory, full v1api.FullNode, cleanupInterval time.Duration) *pdcleaner {
	return &pdcleaner{
		dealsDB:         dealsDB,
		directDealsDB:   directDealsDB,
		legacyDeals:     legacyDeals,
		pd:              pd,
		full:            full,
		cleanupInterval: cleanupInterval,
	}
}

func (p *pdcleaner) Start(ctx context.Context) {
	p.startOnce.Do(func() {
		p.ctx = ctx
		go p.clean()
	})

}

func (p *pdcleaner) clean() {
	// Create a ticker with an hour tick
	ticker := time.NewTicker(p.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Infof("Starting LID clean up")
			err := p.CleanOnce()
			if err != nil {
				log.Errorf("Failed to cleanup LID: %s", err)
				continue
			}
			log.Debugf("Finished cleaning up LID")
		case <-p.ctx.Done():
			return
		}
	}
}

// CleanOnce generates a list of all Expired-Boost, Legacy and Direct deals. It then attempts to clean up these deals.
// It also generated a list of all pieces in LID and tries to find any pieceMetadata with no deals in Boost, Direct or Legacy DB.
// If such a deal is found, it is cleaned up as well
func (p *pdcleaner) CleanOnce() error {
	p.lk.Lock()
	defer p.lk.Unlock()

	head, err := p.full.ChainHead(p.ctx)
	if err != nil {
		return fmt.Errorf("getting chain head: %w", err)
	}
	tskey := head.Key()
	deals, err := p.full.StateMarketDeals(p.ctx, tskey)
	if err != nil {
		return fmt.Errorf("getting market deals: %w", err)
	}

	boostCompleteDeals, err := p.dealsDB.ListCompleted(p.ctx)
	if err != nil {
		return fmt.Errorf("getting complete boost deals: %w", err)
	}
	boostActiveDeals, err := p.dealsDB.ListActive(p.ctx)
	if err != nil {
		return fmt.Errorf("getting active boost deals: %w", err)
	}

	boostDeals := make([]*types.ProviderDealState, 0, len(boostActiveDeals)+len(boostCompleteDeals))

	boostDeals = append(boostDeals, boostCompleteDeals...)
	boostDeals = append(boostDeals, boostActiveDeals...)

	legacyDeals, err := p.legacyDeals.ListDeals()
	if err != nil {
		return fmt.Errorf("getting legacy deals: %w", err)
	}
	completeDirectDeals, err := p.directDealsDB.ListCompleted(p.ctx)
	if err != nil {
		return fmt.Errorf("getting complete direct deals: %w", err)
	}

	// Clean up completed/slashed Boost deals
	for _, d := range boostDeals {
		// Confirm deal did not reach termination before Publishing. Otherwise, no need to clean up
		if d.ChainDealID > abi.DealID(0) {
			// If deal exists online
			md, ok := deals[strconv.FormatInt(int64(d.ChainDealID), 10)]
			if ok {
				// If deal is slashed or end epoch has passed. No other reason for deal to reach termination
				// Same is true for verified deals. We rely on EndEpoch/SlashEpoch for verified deals created by f05
				toCheck := termOrSlash(md.Proposal.EndEpoch, md.State.SlashEpoch)
				if toCheck < head.Height() {
					err = p.pd.RemoveDealForPiece(p.ctx, d.ClientDealProposal.Proposal.PieceCID, d.DealUuid.String())
					if err != nil {
						// Don't return if cleaning up a deal results in error. Try them all.
						log.Errorf("cleaning up boost deal %s for piece %s: %s", d.DealUuid.String(), d.ClientDealProposal.Proposal.PieceCID.String(), err.Error())
					}
				}
			}
		}
	}

	// Clean up completed/slashed legacy deals
	for _, d := range legacyDeals {
		// Confirm deal did not reach termination before Publishing. Otherwise, no need to clean up
		if d.DealID > abi.DealID(0) {
			// If deal exists online
			md, ok := deals[strconv.FormatInt(int64(d.DealID), 10)]
			if ok {
				// If deal is slashed or end epoch has passed. No other reason for deal to reach termination
				toCheck := termOrSlash(md.Proposal.EndEpoch, md.State.SlashEpoch)
				if toCheck < head.Height() {
					err = p.pd.RemoveDealForPiece(p.ctx, d.ClientDealProposal.Proposal.PieceCID, d.ProposalCid.String())
					if err != nil {
						// Don't return if cleaning up a deal results in error. Try them all.
						log.Errorf("cleaning up legacy deal %s for piece %s: %s", d.ProposalCid.String(), d.ClientDealProposal.Proposal.PieceCID.String(), err.Error())
					}
				}
			}
		}
	}

	// Clean up direct deals if there are any otherwise skip this step
	if len(completeDirectDeals) > 0 {
		claims, err := p.full.StateGetClaims(p.ctx, p.miner, tskey)
		if err != nil {
			return fmt.Errorf("getting claims for the miner %s: %w", p.miner, err)
		}
		// Loading miner actor locally is preferred to avoid getting unnecessary data from full.StateMinerActiveSectors()
		mActor, err := p.full.StateGetActor(p.ctx, p.miner, tskey)
		if err != nil {
			return fmt.Errorf("getting actor for the miner %s: %w", p.miner, err)
		}
		store := adt.WrapStore(p.ctx, cbor.NewCborStore(blockstore.NewAPIBlockstore(p.full)))
		mas, err := miner.Load(store, mActor)
		if err != nil {
			return fmt.Errorf("loading miner actor state %s: %w", p.miner, err)
		}
		activeSectors, err := miner.AllPartSectors(mas, miner.Partition.ActiveSectors)
		if err != nil {
			return fmt.Errorf("getting active sector sets for miner %s: %w", p.miner, err)
		}

		for _, d := range completeDirectDeals {
			// AllocationID and ClaimID should match
			cID := verifregtypes.ClaimId(d.AllocationID)
			c, ok := claims[cID]
			if ok {
				present, err := activeSectors.IsSet(uint64(c.Sector))
				if err != nil {
					return fmt.Errorf("checking if bitfield is set: %w", err)
				}
				// Each claim is created with ProveCommit message. So, a sector in claim cannot be unproven.
				// it must be either Active(Proving, Faulty, Recovering) or terminated. If bitfield is not set
				// then sector must have been terminated. This method will also account for future change in sector numbers
				// of a claim. Even if the sector is changed then it must be Active as this change will require a
				// ProveCommit message
				if !present {
					err = p.pd.RemoveDealForPiece(p.ctx, d.PieceCID, d.ID.String())
					if err != nil {
						// Don't return if cleaning up a deal results in error. Try them all.
						log.Errorf("cleaning up legacy deal %s for piece %s: %s", d.ID.String(), d.PieceCID, err.Error())
					}
				}
			}
			// TODO: Account for a final sealing state other than proving (Depends on v1.26.0)
			// 1. We can either check allocation list for all client (Lotus v1.26.0) and cleanup LID if found
			// 2. If not found in allocation list then either claim expired or allocation. We should clean up in this case
			// 3. Account for Curio as it will have redundant sealing until proven
		}
	}

	// Clean up dangling LID deals with no Boost, Direct or Legacy deals attached to them
	plist, err := p.pd.ListPieces(p.ctx)
	if err != nil {
		return fmt.Errorf("getting piece list from LID: %w", err)
	}

	for _, piece := range plist {
		pdeals, err := p.pd.GetPieceDeals(p.ctx, piece)
		if err != nil {
			return fmt.Errorf("getting piece deals from LID: %w", err)
		}
		for _, deal := range pdeals {
			// Remove only if the miner ID matches to avoid removing for other miners in case of shared LID
			if deal.MinerAddr == p.miner {

				bd, err := p.dealsDB.ByPieceCID(p.ctx, piece)
				if err != nil {
					return err
				}
				if len(bd) > 0 {
					continue
				}

				ld, err := p.legacyDeals.ByPieceCid(p.ctx, piece)
				if err != nil {
					return err
				}
				if len(ld) > 0 {
					continue
				}

				dd, err := p.directDealsDB.ByPieceCID(p.ctx, piece)
				if err != nil {
					return err
				}
				if len(dd) > 0 {
					continue
				}

				err = p.pd.RemoveDealForPiece(p.ctx, piece, deal.DealUuid)
				if err != nil {
					// Don't return if cleaning up a deal results in error. Try them all.
					log.Errorf("cleaning up dangling deal %s for piece %s: %s", deal.DealUuid, piece, err.Error())
				}
			}
		}
	}

	return nil
}

func termOrSlash(term, slash abi.ChainEpoch) abi.ChainEpoch {
	if term > slash && slash > 0 {
		return slash
	}

	return term
}
