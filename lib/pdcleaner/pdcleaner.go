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
	cleanupInterval int
}

func NewPieceDirectoryCleaner(cfg *config.Boost) func(lc fx.Lifecycle, dealsDB *db.DealsDB, directDealsDB *db.DirectDealsDB, legacyDeals legacy.LegacyDealManager, pd *piecedirectory.PieceDirectory, full v1api.FullNode) PieceDirectoryCleanup {
	return func(lc fx.Lifecycle, dealsDB *db.DealsDB, directDealsDB *db.DirectDealsDB, legacyDeals legacy.LegacyDealManager, pd *piecedirectory.PieceDirectory, full v1api.FullNode) PieceDirectoryCleanup {

		pdc := newPDC(dealsDB, directDealsDB, legacyDeals, pd, full, cfg.LocalIndexDirectory.LidCleanupInterval)

		ctx, cancel := context.WithCancel(context.Background())

		lc.Append(fx.Hook{
			OnStart: func(_ context.Context) error {
				// Don't start cleanup loop if duration is 0
				if cfg.LocalIndexDirectory.LidCleanupInterval == 0 {
					return nil
				}
				mid, err := address.NewFromString(cfg.Wallets.Miner)
				if err != nil {
					return fmt.Errorf("failed to parse the miner ID %s: %w", cfg.Wallets.Miner, err)
				}
				pdc.miner = mid
				go pdc.Start(ctx)
				return nil
			},
			OnStop: func(_ context.Context) error {
				cancel()
				return nil
			},
		})

		return pdc

	}
}

func newPDC(dealsDB *db.DealsDB, directDealsDB *db.DirectDealsDB, legacyDeals legacy.LegacyDealManager, pd *piecedirectory.PieceDirectory, full v1api.FullNode, cleanupInterval int) *pdcleaner {
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
	ticker := time.NewTicker(time.Hour * time.Duration(p.cleanupInterval))
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
				if md.Proposal.EndEpoch < head.Height() || md.State.SlashEpoch > 0 {
					err = p.pd.RemoveDealForPiece(p.ctx, d.ClientDealProposal.Proposal.PieceCID, d.DealUuid.String())
					if err != nil {
						// Don't return if cleaning up a deal results in error. Try them all.
						log.Errorf("cleaning up boost deal %s for piece %s: %s", d.DealUuid.String(), d.ClientDealProposal.Proposal.PieceCID.String(), err.Error())
					}
				}
			}
		}
	}

	// Clean up completed Boost deals
	for _, d := range legacyDeals {
		// Confirm deal did not reach termination before Publishing. Otherwise, no need to clean up
		if d.DealID > abi.DealID(0) {
			// If deal exists online
			md, ok := deals[strconv.FormatInt(int64(d.DealID), 10)]
			if ok {
				// If deal is slashed or end epoch has passed. No other reason for deal to reach termination
				if md.Proposal.EndEpoch < head.Height() || md.State.SlashEpoch > 0 {
					err = p.pd.RemoveDealForPiece(p.ctx, d.ClientDealProposal.Proposal.PieceCID, d.ProposalCid.String())
					if err != nil {
						// Don't return if cleaning up a deal results in error. Try them all.
						log.Errorf("cleaning up legacy deal %s for piece %s: %s", d.ProposalCid.String(), d.ClientDealProposal.Proposal.PieceCID.String(), err.Error())
					}
				}
			}
		}
	}

	// Clean up direct deals
	claims, err := p.full.StateGetClaims(p.ctx, p.miner, tskey)
	if err != nil {
		return fmt.Errorf("getting claims for the miner %s: %w", p.miner.String(), err)
	}
	for _, d := range completeDirectDeals {
		// AllocationID and ClaimID should match
		cID := verifregtypes.ClaimId(d.AllocationID)
		c, ok := claims[cID]
		if ok {
			// TODO: Figure out slashing mechanism in Direct Deals and add that condition here
			if c.TermMax < head.Height() {
				err = p.pd.RemoveDealForPiece(p.ctx, d.PieceCID, d.ID.String())
				if err != nil {
					// Don't return if cleaning up a deal results in error. Try them all.
					log.Errorf("cleaning up legacy deal %s for piece %s: %s", d.ID.String(), d.PieceCID, err.Error())
				}
			}
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
