package storagemarket

import (
	"sync"

	"github.com/google/uuid"
	"golang.org/x/xerrors"
)

// dealExecs keeps track of executing deals by deal uuid
type dealExecs struct {
	lk  sync.RWMutex
	des map[uuid.UUID]*dealExec
}

func newDealExecs() *dealExecs {
	return &dealExecs{
		des: make(map[uuid.UUID]*dealExec),
	}
}

func (p *dealExecs) track(de *dealExec) {
	p.lk.Lock()
	defer p.lk.Unlock()

	p.des[de.deal.DealUuid] = de
}

func (p *dealExecs) get(dealUuid uuid.UUID) (*dealExec, error) {
	p.lk.RLock()
	defer p.lk.RUnlock()

	de, ok := p.des[dealUuid]
	if !ok {
		return nil, xerrors.Errorf("deal %s: %w", dealUuid, ErrDealExecNotFound)
	}
	return de, nil
}

func (p *dealExecs) del(dealUuid uuid.UUID) {
	p.lk.Lock()
	defer p.lk.Unlock()

	delete(p.des, dealUuid)
}
