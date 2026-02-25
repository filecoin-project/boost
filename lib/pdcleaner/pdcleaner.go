package pdcleaner

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/lib/legacy"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/actors/adt"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	chaintypes "github.com/filecoin-project/lotus/chain/types"
	cbor "github.com/ipfs/go-ipld-cbor"
	logging "github.com/ipfs/go-log/v2"
	"go.uber.org/fx"
	"golang.org/x/sync/errgroup"
)

var log = logging.Logger("pdcleaner")

type PieceDirectoryCleanup interface {
	Start(ctx context.Context)
	CleanOnce(ctx context.Context) error
	getActiveUnprovenSectors(ctx context.Context, tskey chaintypes.TipSetKey) (bitfield.BitField, error)
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
	// Run at start up
	log.Infof("Starting LID clean up")
	serr := p.CleanOnce(p.ctx)
	if serr != nil {
		log.Errorf("Failed to cleanup LID: %s", serr)
	}
	log.Debugf("Finished cleaning up LID")

	// Create a ticker with an hour tick
	ticker := time.NewTicker(p.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Infof("Starting LID clean up")
			err := p.CleanOnce(p.ctx)
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
func (p *pdcleaner) CleanOnce(ctx context.Context) error {
	p.lk.Lock()
	defer p.lk.Unlock()

	head, err := p.full.ChainHead(ctx)
	if err != nil {
		return fmt.Errorf("getting chain head: %w", err)
	}
	tskey := head.Key()

	boostCompleteDeals, err := p.dealsDB.ListCompleted(ctx)
	if err != nil {
		return fmt.Errorf("getting complete boost deals: %w", err)
	}
	boostActiveDeals, err := p.dealsDB.ListActive(ctx)
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
	completeDirectDeals, err := p.directDealsDB.ListCompleted(ctx)
	if err != nil {
		return fmt.Errorf("getting complete direct deals: %w", err)
	}

	activeSectors, err := p.getActiveUnprovenSectors(ctx, tskey)
	if err != nil {
		return err
	}

	// Clean up Boost deals where sector does not exist anymore
	boosteg := errgroup.Group{}
	boosteg.SetLimit(20)
	for _, d := range boostDeals {
		deal := d
		boosteg.Go(func() error {
			present, err := activeSectors.IsSet(uint64(deal.SectorID))
			if err != nil {
				return fmt.Errorf("checking if bitfield is set: %w", err)
			}
			// If not present and start epoch has already passed (to cover any unproven sector in actor state)
			if !present && deal.ClientDealProposal.Proposal.StartEpoch < head.Height() {
				err = p.pd.RemoveDealForPiece(ctx, deal.ClientDealProposal.Proposal.PieceCID, deal.DealUuid.String())
				if err != nil {
					if strings.Contains(err.Error(), "not found") {
						return nil
					}
					return fmt.Errorf("cleaning up boost deal %s for piece %s: %s", deal.DealUuid.String(), deal.ClientDealProposal.Proposal.PieceCID.String(), err.Error())
				}
				log.Infof("removed deal for %s and deal ID %s", deal.ClientDealProposal.Proposal.PieceCID.String(), deal.DealUuid.String())
			}
			return nil
		})
	}
	err = boosteg.Wait()
	if err != nil {
		return err
	}

	// Clean up legacy deals where sector does not exist anymore
	legacyeg := errgroup.Group{}
	legacyeg.SetLimit(20)
	for _, d := range legacyDeals {
		deal := d
		legacyeg.Go(func() error {
			present, err := activeSectors.IsSet(uint64(deal.SectorNumber))
			if err != nil {
				return fmt.Errorf("checking if bitfield is set: %w", err)
			}
			if !present {
				err = p.pd.RemoveDealForPiece(ctx, deal.Proposal.PieceCID, deal.ProposalCid.String())
				if err != nil {
					if strings.Contains(err.Error(), "not found") {
						return nil
					}
					return fmt.Errorf("cleaning up legacy deal %s for piece %s: %s", deal.ProposalCid.String(), deal.Proposal.PieceCID.String(), err.Error())
				}
				log.Infof("removed legacy deal for %s and deal ID %s", deal.Proposal.PieceCID.String(), deal.ProposalCid.String())
			}
			return nil
		})
	}
	err = legacyeg.Wait()
	if err != nil {
		return err
	}

	// Clean up Direct deals where sector does not exist anymore
	// TODO: Refactor for Curio sealing redundancy
	ddoeg := errgroup.Group{}
	ddoeg.SetLimit(20)
	for _, d := range completeDirectDeals {
		deal := d
		ddoeg.Go(func() error {
			present, err := activeSectors.IsSet(uint64(deal.SectorID))
			if err != nil {
				return fmt.Errorf("checking if bitfield is set: %w", err)
			}
			if !present {
				err = p.pd.RemoveDealForPiece(ctx, deal.PieceCID, deal.ID.String())
				if err != nil {
					if strings.Contains(err.Error(), "not found") {
						return nil
					}
					return fmt.Errorf("cleaning up direct deal %s for piece %s: %s", deal.ID.String(), deal.PieceCID, err.Error())
				}
				log.Infof("removed direct deal for %s and deal ID %s", deal.PieceCID.String(), deal.ID.String())
			}
			return nil
		})
	}
	err = ddoeg.Wait()
	if err != nil {
		return err
	}

	// Clean up dangling LID deals with no Boost, Direct or Legacy deals attached to them
	plist, err := p.pd.ListPieces(ctx)
	if err != nil {
		return fmt.Errorf("getting piece list from LID: %w", err)
	}

	lideg := errgroup.Group{}
	lideg.SetLimit(50)
	for _, pi := range plist {
		piece := pi
		lideg.Go(func() error {
			pdeals, err := p.pd.GetPieceDeals(ctx, piece)
			if err != nil {
				return fmt.Errorf("getting piece deals from LID: %w", err)
			}
			for _, deal := range pdeals {
				// Remove only if the miner ID matches to avoid removing for other miners in case of shared LID
				if deal.MinerAddr == p.miner {

					bd, err := p.dealsDB.ByPieceCID(ctx, piece)
					if err != nil {
						return err
					}
					if len(bd) > 0 {
						continue
					}

					ld, err := p.legacyDeals.ByPieceCid(ctx, piece)
					if err != nil {
						return err
					}
					if len(ld) > 0 {
						continue
					}

					dd, err := p.directDealsDB.ByPieceCID(ctx, piece)
					if err != nil {
						return err
					}
					if len(dd) > 0 {
						continue
					}

					err = p.pd.RemoveDealForPiece(ctx, piece, deal.DealUuid)
					if err != nil {
						if strings.Contains(err.Error(), "not found") {
							return nil
						}
						log.Errorf("cleaning up dangling deal %s for piece %s: %s", deal.DealUuid, piece, err.Error())
					}
					log.Infof("removed dangling deal for %s and deal ID %s", piece.String(), deal.DealUuid)
				}
			}
			return nil
		})
	}

	return lideg.Wait()
}

func (p *pdcleaner) getActiveUnprovenSectors(ctx context.Context, tskey chaintypes.TipSetKey) (bitfield.BitField, error) {
	mActor, err := p.full.StateGetActor(ctx, p.miner, tskey)
	if err != nil {
		return bitfield.BitField{}, fmt.Errorf("getting actor for the miner %s: %w", p.miner, err)
	}

	store := adt.WrapStore(ctx, cbor.NewCborStore(blockstore.NewAPIBlockstore(p.full)))
	mas, err := miner.Load(store, mActor)
	if err != nil {
		return bitfield.BitField{}, fmt.Errorf("loading miner actor state %s: %w", p.miner, err)
	}
	liveSectors, err := miner.AllPartSectors(mas, miner.Partition.LiveSectors)
	if err != nil {
		return bitfield.BitField{}, fmt.Errorf("getting live sector sets for miner %s: %w", p.miner, err)
	}
	unProvenSectors, err := miner.AllPartSectors(mas, miner.Partition.UnprovenSectors)
	if err != nil {
		return bitfield.BitField{}, fmt.Errorf("getting unproven sector sets for miner %s: %w", p.miner, err)
	}
	activeSectors, err := bitfield.MergeBitFields(liveSectors, unProvenSectors)
	if err != nil {
		return bitfield.BitField{}, fmt.Errorf("merging bitfields to generate all sealed sectors on miner %s: %w", p.miner, err)
	}
	return activeSectors, nil
}
