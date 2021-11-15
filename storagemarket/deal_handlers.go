package storagemarket

import (
	"sync"

	"github.com/google/uuid"
	"golang.org/x/xerrors"
)

var ErrDealHandlerFound = xerrors.Errorf("deal handler not found")

// dealHandlers keeps track of executing deals by deal uuid
type dealHandlers struct {
	lk       sync.RWMutex
	handlers map[uuid.UUID]*dealHandler
}

func newDealHandlers() *dealHandlers {
	return &dealHandlers{
		handlers: make(map[uuid.UUID]*dealHandler),
	}
}

func (p *dealHandlers) track(dh *dealHandler) {
	p.lk.Lock()
	defer p.lk.Unlock()

	p.handlers[dh.dealUuid] = dh
}

func (p *dealHandlers) get(dealUuid uuid.UUID) (*dealHandler, error) {
	p.lk.RLock()
	defer p.lk.RUnlock()

	dh, ok := p.handlers[dealUuid]
	if !ok {
		return nil, xerrors.Errorf("deal %s: %w", dealUuid, ErrDealHandlerFound)
	}
	return dh, nil
}

func (p *dealHandlers) del(dealUuid uuid.UUID) {
	p.lk.Lock()
	defer p.lk.Unlock()

	delete(p.handlers, dealUuid)
}
