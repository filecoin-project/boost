package gql

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/google/uuid"
)

// OnUpdateFn is called when the deal transfer state changes
type OnUpdateFn func(dealUuid uuid.UUID, transferred uint64)

type mockTransfer struct {
	lk          sync.Mutex
	transferred map[uuid.UUID]uint64

	onUpdateFn OnUpdateFn
}

func newMockTransfer() *mockTransfer {
	return &mockTransfer{
		transferred: make(map[uuid.UUID]uint64),
	}
}

func (t *mockTransfer) OnUpdate(fn OnUpdateFn) {
	t.onUpdateFn = fn
}

func (t *mockTransfer) SimulateTransfer(ctx context.Context, deal *types.ProviderDealState) {
	fivePct := uint64(deal.ClientDealProposal.Proposal.PieceSize) / 20
	for i := 0; i < 20; i++ {
		t.setTransferred(deal.DealUuid, uint64(i)*fivePct+uint64(rand.Int63n(int64(fivePct))))
		select {
		case <-ctx.Done():
			return
		case <-time.After(500*time.Millisecond + time.Duration(rand.Intn(1000))*time.Millisecond):
		}
	}

	t.setTransferred(deal.DealUuid, uint64(deal.ClientDealProposal.Proposal.PieceSize))
}

func (t *mockTransfer) setTransferred(dealUuid uuid.UUID, size uint64) {
	t.lk.Lock()
	t.transferred[dealUuid] = size
	t.lk.Unlock()

	if t.onUpdateFn != nil {
		t.onUpdateFn(dealUuid, size)
	}
}

func (t *mockTransfer) Transferred(dealUuid uuid.UUID) uint64 {
	if t == nil {
		return 0
	}

	t.lk.Lock()
	defer t.lk.Unlock()

	return t.transferred[dealUuid]
}
