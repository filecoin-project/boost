package datatransfer

import (
	"context"
	"io/ioutil"
	"math/rand"
	"sync"
	"time"

	"golang.org/x/xerrors"

	"github.com/google/uuid"
)

type MockTransport struct {
	lk          sync.Mutex
	transferred map[uuid.UUID]uint64
}

func NewMockTransport() *MockTransport {
	return &MockTransport{
		transferred: make(map[uuid.UUID]uint64),
	}
}

func (t *MockTransport) SimulateTransfer(ctx context.Context, dealUuid uuid.UUID, size uint64, onTransferred OnTransferredFn) error {
	fivePct := size / 20
	for i := 0; i < 20; i++ {
		transferred := uint64(i)*fivePct + uint64(rand.Int63n(int64(fivePct)))
		t.setTransferred(dealUuid, transferred)
		if onTransferred != nil {
			onTransferred(transferred)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500*time.Millisecond + time.Duration(rand.Intn(1000))*time.Millisecond):
		}
	}

	t.setTransferred(dealUuid, size)
	if onTransferred != nil {
		onTransferred(size)
	}

	return nil
}

func (t *MockTransport) setTransferred(dealUuid uuid.UUID, size uint64) {
	t.lk.Lock()
	t.transferred[dealUuid] = size
	t.lk.Unlock()
}

func (t *MockTransport) Transferred(dealUuid uuid.UUID) uint64 {
	if t == nil {
		return 0
	}

	t.lk.Lock()
	defer t.lk.Unlock()

	return t.transferred[dealUuid]
}

type OnTransferredFn func(transferred uint64)

type ExecuteParams struct {
	TransferType   string
	TransferParams []byte
	DealUuid       uuid.UUID
	FilePath       string
	Size           uint64
	OnTransferred  OnTransferredFn
}

func (t *MockTransport) Execute(ctx context.Context, params ExecuteParams) error {
	transferParams, err := TransferLocal.UnmarshallParams(params.TransferParams)
	if err != nil {
		return xerrors.Errorf("unmarshalling data transfer params: %w", err)
	}

	bz, err := ioutil.ReadFile(transferParams.Path)
	if err != nil {
		return xerrors.Errorf("reading file %s: %w", transferParams.Path, err)
	}

	err = ioutil.WriteFile(params.FilePath, bz, 0644)
	if err != nil {
		return xerrors.Errorf("writing file %s: %w", params.FilePath, err)
	}

	return t.SimulateTransfer(ctx, params.DealUuid, params.Size, params.OnTransferred)
}
