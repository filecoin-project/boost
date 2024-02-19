package legacy

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/filecoin-project/boost/storagemarket/types/legacytypes"
	"github.com/filecoin-project/go-statemachine/fsm"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
)

//go:generate go run github.com/golang/mock/mockgen -destination=mocks/legacy_manager_mock.go . LegacyDealManager

type LegacyDealManager interface {
	Run(ctx context.Context)
	DealCount(ctx context.Context) (int, error)
	ByPieceCid(ctx context.Context, pieceCid cid.Cid) ([]legacytypes.MinerDeal, error)
	ByPayloadCid(ctx context.Context, payloadCid cid.Cid) ([]legacytypes.MinerDeal, error)
	ByPublishCid(ctx context.Context, publishCid cid.Cid) ([]legacytypes.MinerDeal, error)
	ListDeals() ([]legacytypes.MinerDeal, error)
	ByPropCid(propCid cid.Cid) (legacytypes.MinerDeal, error)
	ListLocalDealsPage(startPropCid *cid.Cid, offset int, limit int) ([]legacytypes.MinerDeal, error)
}

var log = logging.Logger("legacydeals")

type legacyDealsManager struct {
	legacyFSM fsm.Group

	startedOnce sync.Once
	started     chan struct{}

	lk            sync.RWMutex
	dealCount     int
	pieceCidIdx   map[cid.Cid][]cid.Cid
	payloadCidIdx map[cid.Cid][]cid.Cid
	publishCidIdx map[cid.Cid][]cid.Cid
}

func NewLegacyDealsManager(legacyFSM fsm.Group) *legacyDealsManager {
	return &legacyDealsManager{
		legacyFSM:     legacyFSM,
		started:       make(chan struct{}),
		pieceCidIdx:   make(map[cid.Cid][]cid.Cid),
		payloadCidIdx: make(map[cid.Cid][]cid.Cid),
		publishCidIdx: make(map[cid.Cid][]cid.Cid),
	}
}

func (m *legacyDealsManager) Run(ctx context.Context) {
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

func (m *legacyDealsManager) refresh() error {
	start := time.Now()
	log.Infow("refreshing legacy deals list")
	dls, err := m.ListDeals()
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

func (m *legacyDealsManager) waitStarted(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.started:
		return nil
	}
}

func (m *legacyDealsManager) DealCount(ctx context.Context) (int, error) {
	if err := m.waitStarted(ctx); err != nil {
		return 0, err
	}

	m.lk.RLock()
	defer m.lk.RUnlock()

	return m.dealCount, nil
}

func (m *legacyDealsManager) ByPieceCid(ctx context.Context, pieceCid cid.Cid) ([]legacytypes.MinerDeal, error) {
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

func (m *legacyDealsManager) ByPayloadCid(ctx context.Context, payloadCid cid.Cid) ([]legacytypes.MinerDeal, error) {
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

func (m *legacyDealsManager) ByPublishCid(ctx context.Context, publishCid cid.Cid) ([]legacytypes.MinerDeal, error) {
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
func (m *legacyDealsManager) byPropCids(propCids []cid.Cid) ([]legacytypes.MinerDeal, error) {
	dls := make([]legacytypes.MinerDeal, 0, len(propCids))
	for _, propCid := range propCids {
		var d legacytypes.MinerDeal
		err := m.legacyFSM.Get(propCid).Get(&d)
		if err == nil {
			dls = append(dls, d)
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

func (m *legacyDealsManager) ListDeals() ([]legacytypes.MinerDeal, error) {
	var list []legacytypes.MinerDeal
	if err := m.legacyFSM.List(&list); err != nil {
		return nil, err
	}
	return list, nil
}

// Get deal by deal signed proposal cid
func (m *legacyDealsManager) ByPropCid(propCid cid.Cid) (legacytypes.MinerDeal, error) {
	var d legacytypes.MinerDeal
	err := m.legacyFSM.Get(propCid).Get(&d)
	if err != nil {
		return legacytypes.MinerDeal{}, err
	}
	return d, nil
}

func (m *legacyDealsManager) ListLocalDealsPage(startPropCid *cid.Cid, offset int, limit int) ([]legacytypes.MinerDeal, error) {
	if limit == 0 {
		return []legacytypes.MinerDeal{}, nil
	}

	// Get all deals
	var deals []legacytypes.MinerDeal
	if err := m.legacyFSM.List(&deals); err != nil {
		return nil, err
	}

	// Sort by creation time descending
	sort.Slice(deals, func(i, j int) bool {
		return deals[i].CreationTime.Time().After(deals[j].CreationTime.Time())
	})

	// Iterate through deals until we reach the target signed proposal cid,
	// find the offset from there, then add deals from that point up to limit
	page := make([]legacytypes.MinerDeal, 0, limit)
	startIndex := -1
	if startPropCid == nil {
		startIndex = 0
	}
	for i, dl := range deals {
		// Find the deal with a proposal cid matching startPropCid
		if startPropCid != nil && dl.ProposalCid == *startPropCid {
			// Start adding deals from offset after the first matching deal
			startIndex = i + offset
		}

		if startIndex >= 0 && i >= startIndex {
			page = append(page, dl)
		}
		if len(page) == limit {
			return page, nil
		}
	}

	return page, nil
}
