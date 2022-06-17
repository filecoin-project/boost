package smtestutil

import (
	"context"
	"io"
	"io/ioutil"
	"sync"

	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/mock_types"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin/v8/market"
	"github.com/filecoin-project/lotus/api"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
)

type MinerStub struct {
	*mock_types.MockDealPublisher
	*mock_types.MockChainDealManager
	*mock_types.MockPieceAdder

	lk                    sync.Mutex
	unblockPublish        map[uuid.UUID]chan struct{}
	unblockWaitForPublish map[uuid.UUID]chan struct{}
	unblockAddPiece       map[uuid.UUID]chan struct{}
}

func NewMinerStub(ctrl *gomock.Controller) *MinerStub {
	return &MinerStub{
		MockDealPublisher:    mock_types.NewMockDealPublisher(ctrl),
		MockChainDealManager: mock_types.NewMockChainDealManager(ctrl),
		MockPieceAdder:       mock_types.NewMockPieceAdder(ctrl),

		unblockPublish:        make(map[uuid.UUID]chan struct{}),
		unblockWaitForPublish: make(map[uuid.UUID]chan struct{}),
		unblockAddPiece:       make(map[uuid.UUID]chan struct{}),
	}
}

func (ms *MinerStub) UnblockPublish(id uuid.UUID) {
	ms.lk.Lock()
	ch := ms.unblockPublish[id]
	ms.lk.Unlock()
	close(ch)
}

func (ms *MinerStub) UnblockWaitForPublish(id uuid.UUID) {
	ms.lk.Lock()
	ch := ms.unblockWaitForPublish[id]
	ms.lk.Unlock()
	close(ch)
}
func (ms *MinerStub) UnblockAddPiece(id uuid.UUID) {
	ms.lk.Lock()
	ch := ms.unblockAddPiece[id]
	ms.lk.Unlock()
	close(ch)
}

func (ms *MinerStub) ForDeal(dp *types.DealParams, publishCid, finalPublishCid cid.Cid, dealId abi.DealID, sectorId abi.SectorNumber,
	offset abi.PaddedPieceSize) *MinerStubBuilder {
	return &MinerStubBuilder{
		stub: ms,
		dp:   dp,

		publishCid:      publishCid,
		finalPublishCid: finalPublishCid,
		dealId:          dealId,
		sectorId:        sectorId,
		offset:          offset,
	}
}

type MinerStubBuilder struct {
	stub       *MinerStub
	dp         *types.DealParams
	publishCid cid.Cid

	finalPublishCid cid.Cid
	dealId          abi.DealID

	sectorId abi.SectorNumber
	offset   abi.PaddedPieceSize
	rb       *[]byte
}

func (mb *MinerStubBuilder) SetupAllNonBlocking() *MinerStubBuilder {
	return mb.SetupPublish(false).SetupPublishConfirm(false).SetupAddPiece(false)
}

func (mb *MinerStubBuilder) SetupAllBlocking() *MinerStubBuilder {
	return mb.SetupPublish(true).SetupPublishConfirm(true).SetupAddPiece(true)
}

func (mb *MinerStubBuilder) SetupNoOp() *MinerStubBuilder {
	mb.stub.MockDealPublisher.EXPECT().Publish(gomock.Any(), gomock.Eq(mb.dp.ClientDealProposal)).DoAndReturn(func(_ context.Context, _ market.ClientDealProposal) (cid.Cid, error) {
		return mb.publishCid, nil
	}).AnyTimes()

	mb.stub.MockChainDealManager.EXPECT().WaitForPublishDeals(gomock.Any(), gomock.Eq(mb.publishCid), gomock.Eq(mb.dp.ClientDealProposal.Proposal)).DoAndReturn(func(_ context.Context, _ cid.Cid, _ market.DealProposal) (*storagemarket.PublishDealsWaitResult, error) {
		return &storagemarket.PublishDealsWaitResult{
			DealID:   mb.dealId,
			FinalCid: mb.finalPublishCid,
		}, nil
	}).AnyTimes()

	mb.stub.MockPieceAdder.EXPECT().AddPiece(gomock.Any(), gomock.Eq(mb.dp.ClientDealProposal.Proposal.PieceSize.Unpadded()), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ abi.UnpaddedPieceSize, r io.Reader, _ api.PieceDealInfo) (abi.SectorNumber, abi.PaddedPieceSize, error) {
		return mb.sectorId, mb.offset, nil
	}).AnyTimes()

	return mb
}

func (mb *MinerStubBuilder) SetupPublish(blocking bool) *MinerStubBuilder {
	mb.stub.lk.Lock()
	if blocking {
		mb.stub.unblockPublish[mb.dp.DealUUID] = make(chan struct{})
	}
	mb.stub.lk.Unlock()

	mb.stub.MockDealPublisher.EXPECT().Publish(gomock.Any(), gomock.Eq(mb.dp.ClientDealProposal)).DoAndReturn(func(ctx context.Context, _ market.ClientDealProposal) (cid.Cid, error) {
		mb.stub.lk.Lock()
		ch := mb.stub.unblockPublish[mb.dp.DealUUID]
		mb.stub.lk.Unlock()
		if ch != nil {
			select {
			case <-ctx.Done():
				return cid.Undef, ctx.Err()
			case <-ch:
			}

		}
		if ctx.Err() != nil {
			return cid.Undef, ctx.Err()
		}

		return mb.publishCid, nil
	})

	return mb
}

func (mb *MinerStubBuilder) SetupPublishFailure(err error) *MinerStubBuilder {
	mb.stub.MockDealPublisher.EXPECT().Publish(gomock.Any(), gomock.Eq(mb.dp.ClientDealProposal)).DoAndReturn(func(_ context.Context, _ market.ClientDealProposal) (cid.Cid, error) {
		return cid.Undef, err
	})

	return mb
}

func (mb *MinerStubBuilder) SetupPublishConfirm(blocking bool) *MinerStubBuilder {
	mb.stub.lk.Lock()
	if blocking {
		mb.stub.unblockWaitForPublish[mb.dp.DealUUID] = make(chan struct{})
	}
	mb.stub.lk.Unlock()

	mb.stub.MockChainDealManager.EXPECT().WaitForPublishDeals(gomock.Any(), gomock.Eq(mb.publishCid), gomock.Eq(mb.dp.ClientDealProposal.Proposal)).DoAndReturn(func(ctx context.Context, _ cid.Cid, _ market.DealProposal) (*storagemarket.PublishDealsWaitResult, error) {
		mb.stub.lk.Lock()
		ch := mb.stub.unblockWaitForPublish[mb.dp.DealUUID]
		mb.stub.lk.Unlock()
		if ch != nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-ch:
			}

		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		return &storagemarket.PublishDealsWaitResult{
			DealID:   mb.dealId,
			FinalCid: mb.finalPublishCid,
		}, nil
	})

	return mb
}

func (mb *MinerStubBuilder) SetupPublishConfirmFailure(err error) *MinerStubBuilder {
	mb.stub.MockChainDealManager.EXPECT().WaitForPublishDeals(gomock.Any(), gomock.Eq(mb.publishCid), gomock.Eq(mb.dp.ClientDealProposal.Proposal)).DoAndReturn(func(_ context.Context, _ cid.Cid, _ market.DealProposal) (*storagemarket.PublishDealsWaitResult, error) {
		return nil, err
	})

	return mb
}

func (mb *MinerStubBuilder) SetupAddPiece(blocking bool) *MinerStubBuilder {
	mb.stub.lk.Lock()
	if blocking {
		mb.stub.unblockAddPiece[mb.dp.DealUUID] = make(chan struct{})
	}
	mb.stub.lk.Unlock()

	sdInfo := lapi.PieceDealInfo{
		DealID:       mb.dealId,
		DealProposal: &mb.dp.ClientDealProposal.Proposal,
		PublishCid:   &mb.finalPublishCid,
		DealSchedule: lapi.DealSchedule{
			StartEpoch: mb.dp.ClientDealProposal.Proposal.StartEpoch,
			EndEpoch:   mb.dp.ClientDealProposal.Proposal.EndEpoch,
		},
		KeepUnsealed: true,
	}

	var readBytes []byte
	mb.stub.MockPieceAdder.EXPECT().AddPiece(gomock.Any(), gomock.Eq(mb.dp.ClientDealProposal.Proposal.PieceSize.Unpadded()), gomock.Any(), gomock.Eq(sdInfo)).DoAndReturn(func(ctx context.Context, _ abi.UnpaddedPieceSize, r io.Reader, _ api.PieceDealInfo) (abi.SectorNumber, abi.PaddedPieceSize, error) {
		mb.stub.lk.Lock()
		ch := mb.stub.unblockAddPiece[mb.dp.DealUUID]
		mb.stub.lk.Unlock()
		if ch != nil {
			select {
			case <-ctx.Done():
				return abi.SectorNumber(0), abi.PaddedPieceSize(0), ctx.Err()
			case <-ch:
			}

		}
		if ctx.Err() != nil {
			return abi.SectorNumber(0), abi.PaddedPieceSize(0), ctx.Err()
		}

		var err error
		readBytes, err = ioutil.ReadAll(r)
		return mb.sectorId, mb.offset, err
	})

	mb.rb = &readBytes
	return mb
}

func (mb *MinerStubBuilder) SetupAddPieceFailure(err error) {
	sdInfo := lapi.PieceDealInfo{
		DealID:       mb.dealId,
		DealProposal: &mb.dp.ClientDealProposal.Proposal,
		PublishCid:   &mb.finalPublishCid,
		DealSchedule: lapi.DealSchedule{
			StartEpoch: mb.dp.ClientDealProposal.Proposal.StartEpoch,
			EndEpoch:   mb.dp.ClientDealProposal.Proposal.EndEpoch,
		},
		KeepUnsealed: true,
	}

	mb.stub.MockPieceAdder.EXPECT().AddPiece(gomock.Any(), gomock.Eq(mb.dp.ClientDealProposal.Proposal.PieceSize.Unpadded()), gomock.Any(), gomock.Eq(sdInfo)).DoAndReturn(func(_ context.Context, _ abi.UnpaddedPieceSize, r io.Reader, _ api.PieceDealInfo) (abi.SectorNumber, abi.PaddedPieceSize, error) {
		return abi.SectorNumber(0), abi.PaddedPieceSize(0), err
	})
}

func (mb *MinerStubBuilder) Output() *StubbedMinerOutput {
	return &StubbedMinerOutput{
		PublishCid:      mb.publishCid,
		FinalPublishCid: mb.finalPublishCid,
		DealID:          mb.dealId,
		SealedBytes:     mb.rb,
		SectorID:        mb.sectorId,
		Offset:          mb.offset,
	}
}

type StubbedMinerOutput struct {
	PublishCid      cid.Cid
	FinalPublishCid cid.Cid
	DealID          abi.DealID
	SealedBytes     *[]byte
	SectorID        abi.SectorNumber
	Offset          abi.PaddedPieceSize
}
