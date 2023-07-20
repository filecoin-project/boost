package legacy

import (
	"context"
	"errors"
	gfm_storagemarket "github.com/filecoin-project/boost-gfm/storagemarket"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"sync"
	"time"
)

var log = logging.Logger("legacydeals")

type LegacyDealsManager struct {
	legacyProv gfm_storagemarket.StorageProvider

	startedOnce sync.Once
	started     chan struct{}

	lk            sync.RWMutex
	dealCount     int
	pieceCidIdx   map[cid.Cid][]cid.Cid
	payloadCidIdx map[cid.Cid][]cid.Cid
	publishCidIdx map[cid.Cid][]cid.Cid
}

func NewLegacyDealsManager(legacyProv gfm_storagemarket.StorageProvider) *LegacyDealsManager {
	return &LegacyDealsManager{
		legacyProv:    legacyProv,
		started:       make(chan struct{}),
		pieceCidIdx:   make(map[cid.Cid][]cid.Cid),
		payloadCidIdx: make(map[cid.Cid][]cid.Cid),
		publishCidIdx: make(map[cid.Cid][]cid.Cid),
	}
}

func (m *LegacyDealsManager) Run(ctx context.Context) {
	refresh := func() {
		err := m.refresh()
		if err != nil {
			log.Warnf("refreshing list of legacy deals: %s", err)
		}
	}

	// Refresh list on startup
	refresh()

	// Refresh list every 10 minutes
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refresh()
		}
	}
}

func (m *LegacyDealsManager) refresh() error {
	start := time.Now()
	log.Infow("refreshing legacy deals list")
	dls, err := m.legacyProv.ListLocalDeals()
	if err != nil {
		return err
	}

	log.Infow("refreshed legacy deals list",
		"elapsed", time.Since(start).String(), "count", len(dls))

	m.lk.Lock()
	{
		m.dealCount = len(dls)

		for _, dl := range dls {
			// piece cid -> signed proposal cid
			if dl.Ref != nil && dl.Ref.PieceCid != nil && dl.Ref.PieceCid.Defined() {
				m.pieceCidIdx[*dl.Ref.PieceCid] = append(m.pieceCidIdx[*dl.Ref.PieceCid], dl.ProposalCid)
			}

			// payload cid -> signed proposal cid
			if dl.Ref != nil && dl.Ref.Root.Defined() {
				m.payloadCidIdx[dl.Ref.Root] = append(m.payloadCidIdx[dl.Ref.Root], dl.ProposalCid)
			}

			// publish cid -> signed proposal cid
			if dl.PublishCid != nil && dl.PublishCid.Defined() {
				m.publishCidIdx[*dl.PublishCid] = append(m.publishCidIdx[*dl.PublishCid], dl.ProposalCid)
			}
		}
	}
	m.lk.Unlock()

	m.startedOnce.Do(func() {
		close(m.started)
	})

	return nil
}

func (m *LegacyDealsManager) waitStarted(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.started:
		return nil
	}
}

func (m *LegacyDealsManager) DealCount(ctx context.Context) (int, error) {
	if err := m.waitStarted(ctx); err != nil {
		return 0, err
	}

	m.lk.RLock()
	defer m.lk.RUnlock()

	return m.dealCount, nil
}

func (m *LegacyDealsManager) ByPieceCid(ctx context.Context, pieceCid cid.Cid) ([]gfm_storagemarket.MinerDeal, error) {
	if err := m.waitStarted(ctx); err != nil {
		return nil, err
	}

	// The index stores a mapping of piece cid -> deal proposal cids
	m.lk.RLock()
	propCids, ok := m.pieceCidIdx[pieceCid]
	m.lk.RUnlock()
	if !ok {
		return nil, nil
	}

	return m.byPropCids(propCids)
}

func (m *LegacyDealsManager) ByPayloadCid(ctx context.Context, payloadCid cid.Cid) ([]gfm_storagemarket.MinerDeal, error) {
	if err := m.waitStarted(ctx); err != nil {
		return nil, err
	}

	// The index stores a mapping of payload cid -> deal proposal cids
	m.lk.RLock()
	propCids, ok := m.payloadCidIdx[payloadCid]
	m.lk.RUnlock()
	if !ok {
		return nil, nil
	}

	return m.byPropCids(propCids)
}

func (m *LegacyDealsManager) ByPublishCid(ctx context.Context, publishCid cid.Cid) ([]gfm_storagemarket.MinerDeal, error) {
	if err := m.waitStarted(ctx); err != nil {
		return nil, err
	}

	// The index stores a mapping of publish cid -> deal proposal cids
	m.lk.RLock()
	propCids, ok := m.publishCidIdx[publishCid]
	m.lk.RUnlock()
	if !ok {
		return nil, nil
	}

	return m.byPropCids(propCids)
}

// Get deals by deal signed proposal cid
func (m *LegacyDealsManager) byPropCids(propCids []cid.Cid) ([]gfm_storagemarket.MinerDeal, error) {
	dls := make([]gfm_storagemarket.MinerDeal, 0, len(propCids))
	for _, propCid := range propCids {
		dl, err := m.legacyProv.GetLocalDeal(propCid)
		if err == nil {
			dls = append(dls, dl)
			continue
		}

		// If there's a "not found" error, ignore it.
		// For any other type of error, stop processing and return it.
		if !errors.Is(err, datastore.ErrNotFound) {
			return nil, err
		}
	}

	return dls, nil
}
