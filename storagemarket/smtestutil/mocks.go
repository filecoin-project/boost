package smtestutil

import (
	"context"
	"io"
	"strings"
	"sync"

	"github.com/filecoin-project/boost-gfm/storagemarket"
	mock_sealingpipeline "github.com/filecoin-project/boost/storagemarket/sealingpipeline/mock"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/mock_types"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	"github.com/filecoin-project/lotus/api"
	lapi "github.com/filecoin-project/lotus/api"
	sealing "github.com/filecoin-project/lotus/storage/pipeline"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
)

type MinerStub struct {
	*mock_types.MockDealPublisher
	*mock_types.MockChainDealManager
	*mock_types.MockPieceAdder
	*mock_types.MockCommpCalculator
	*mock_types.MockIndexProvider
	*mock_sealingpipeline.MockAPI

	lk                    sync.Mutex
	unblockCommp          map[uuid.UUID]chan struct{}
	unblockPublish        map[uuid.UUID]chan struct{}
	unblockWaitForPublish map[uuid.UUID]chan struct{}
	unblockAddPiece       map[uuid.UUID]chan struct{}
	unblockAnnounce       map[uuid.UUID]chan struct{}
}

func NewMinerStub(ctrl *gomock.Controller) *MinerStub {
	return &MinerStub{
		MockCommpCalculator:  mock_types.NewMockCommpCalculator(ctrl),
		MockDealPublisher:    mock_types.NewMockDealPublisher(ctrl),
		MockChainDealManager: mock_types.NewMockChainDealManager(ctrl),
		MockPieceAdder:       mock_types.NewMockPieceAdder(ctrl),
		MockIndexProvider:    mock_types.NewMockIndexProvider(ctrl),
		MockAPI:              mock_sealingpipeline.NewMockAPI(ctrl),

		unblockCommp:          make(map[uuid.UUID]chan struct{}),
		unblockPublish:        make(map[uuid.UUID]chan struct{}),
		unblockWaitForPublish: make(map[uuid.UUID]chan struct{}),
		unblockAddPiece:       make(map[uuid.UUID]chan struct{}),
		unblockAnnounce:       make(map[uuid.UUID]chan struct{}),
	}
}

func (ms *MinerStub) UnblockCommp(id uuid.UUID) {
	ms.lk.Lock()
	ch := ms.unblockCommp[id]
	ms.lk.Unlock()
	close(ch)
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

func (ms *MinerStub) ForDeal(dp *types.DealParams, publishCid, finalPublishCid cid.Cid, dealId abi.DealID, sectorsStatusDealId abi.DealID, sectorId abi.SectorNumber,
	offset abi.PaddedPieceSize) *MinerStubBuilder {
	return &MinerStubBuilder{
		stub: ms,
		dp:   dp,

		publishCid:          publishCid,
		finalPublishCid:     finalPublishCid,
		dealId:              dealId,
		sectorsStatusDealId: sectorsStatusDealId,
		sectorId:            sectorId,
		offset:              offset,
	}
}

type MinerStubBuilder struct {
	stub       *MinerStub
	dp         *types.DealParams
	publishCid cid.Cid

	finalPublishCid     cid.Cid
	dealId              abi.DealID
	sectorsStatusDealId abi.DealID

	sectorId abi.SectorNumber
	offset   abi.PaddedPieceSize
	rb       *[]byte
}

func (mb *MinerStubBuilder) SetupNoOp() *MinerStubBuilder {
	mb.stub.MockCommpCalculator.EXPECT().ComputeDataCid(gomock.Any(), gomock.Eq(mb.dp.ClientDealProposal.Proposal.PieceSize.Unpadded()), gomock.Any()).DoAndReturn(func(_ context.Context, _ abi.UnpaddedPieceSize, r io.Reader) (abi.PieceInfo, error) {
		return abi.PieceInfo{
			Size:     mb.dp.ClientDealProposal.Proposal.PieceSize,
			PieceCID: mb.dp.ClientDealProposal.Proposal.PieceCID,
		}, nil
	}).AnyTimes()

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

	mb.stub.MockIndexProvider.EXPECT().Enabled().Return(true).AnyTimes()
	mb.stub.MockIndexProvider.EXPECT().Start(gomock.Any()).AnyTimes()
	mb.stub.MockIndexProvider.EXPECT().AnnounceBoostDeal(gomock.Any(), gomock.Any()).Return(testutil.GenerateCid(), nil).AnyTimes()
	secInfo := lapi.SectorInfo{
		State: lapi.SectorState(sealing.Proving),
		Deals: []abi.DealID{mb.sectorsStatusDealId},
	}
	mb.stub.MockAPI.EXPECT().SectorsStatus(gomock.Any(), gomock.Any(), false).Return(secInfo, nil).AnyTimes()

	return mb
}

func (mb *MinerStubBuilder) SetupCommp(blocking bool, optional bool) *MinerStubBuilder {
	mb.stub.lk.Lock()
	if blocking {
		mb.stub.unblockCommp[mb.dp.DealUUID] = make(chan struct{})
	}
	mb.stub.lk.Unlock()

	exp := mb.stub.MockCommpCalculator.EXPECT().ComputeDataCid(gomock.Any(), gomock.Eq(mb.dp.ClientDealProposal.Proposal.PieceSize.Unpadded()), gomock.Any()).DoAndReturn(func(ctx context.Context, _ abi.UnpaddedPieceSize, r io.Reader) (abi.PieceInfo, error) {
		mb.stub.lk.Lock()
		ch := mb.stub.unblockCommp[mb.dp.DealUUID]
		mb.stub.lk.Unlock()
		if ch != nil {
			select {
			case <-ctx.Done():
				return abi.PieceInfo{}, ctx.Err()
			case <-ch:
			}
		}
		if ctx.Err() != nil {
			return abi.PieceInfo{}, ctx.Err()
		}

		return abi.PieceInfo{
			Size:     mb.dp.ClientDealProposal.Proposal.PieceSize,
			PieceCID: mb.dp.ClientDealProposal.Proposal.PieceCID,
		}, nil
	})
	if optional {
		exp.AnyTimes()
	}

	return mb
}

func (mb *MinerStubBuilder) SetupCommpFailure(err error) {
	mb.stub.MockCommpCalculator.EXPECT().ComputeDataCid(gomock.Any(), gomock.Eq(mb.dp.ClientDealProposal.Proposal.PieceSize.Unpadded()), gomock.Any()).DoAndReturn(func(_ context.Context, _ abi.UnpaddedPieceSize, r io.Reader) (abi.PieceInfo, error) {
		if strings.HasPrefix(err.Error(), "panic: ") {
			panic(err.Error()[len("panic: "):])
		}
		return abi.PieceInfo{}, err
	})
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
		KeepUnsealed: !mb.dp.RemoveUnsealedCopy,
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
		readBytes, err = io.ReadAll(r)
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
		KeepUnsealed: !mb.dp.RemoveUnsealedCopy,
	}

	mb.stub.MockPieceAdder.EXPECT().AddPiece(gomock.Any(), gomock.Eq(mb.dp.ClientDealProposal.Proposal.PieceSize.Unpadded()), gomock.Any(), gomock.Eq(sdInfo)).DoAndReturn(func(_ context.Context, _ abi.UnpaddedPieceSize, r io.Reader, _ api.PieceDealInfo) (abi.SectorNumber, abi.PaddedPieceSize, error) {
		return abi.SectorNumber(0), abi.PaddedPieceSize(0), err
	})
}

func (mb *MinerStubBuilder) SetupAnnounce(blocking bool, announce bool) *MinerStubBuilder {
	mb.stub.lk.Lock()
	if blocking {
		mb.stub.unblockAnnounce[mb.dp.DealUUID] = make(chan struct{})
	}
	mb.stub.lk.Unlock()

	var callCount int
	if announce {
		callCount = 1
	}

	mb.stub.MockIndexProvider.EXPECT().Enabled().AnyTimes().Return(true)
	mb.stub.MockIndexProvider.EXPECT().Start(gomock.Any()).AnyTimes()
	mb.stub.MockIndexProvider.EXPECT().AnnounceBoostDeal(gomock.Any(), gomock.Any()).Times(callCount).DoAndReturn(func(ctx context.Context, _ *types.ProviderDealState) (cid.Cid, error) {
		mb.stub.lk.Lock()
		ch := mb.stub.unblockAddPiece[mb.dp.DealUUID]
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

		return testutil.GenerateCid(), nil
	})

	// Note: The sealing state checks happen after the announce stage has
	// completed. So we would only expect to get calls to SectorsStatus
	// after announce.
	secInfo := lapi.SectorInfo{
		State: lapi.SectorState(sealing.Proving),
		Deals: []abi.DealID{mb.sectorsStatusDealId},
	}
	mb.stub.MockAPI.EXPECT().SectorsStatus(gomock.Any(), gomock.Any(), false).Return(secInfo, nil).AnyTimes()

	return mb
}

func (mb *MinerStubBuilder) Output() *StubbedMinerOutput {
	return &StubbedMinerOutput{
		PublishCid:          mb.publishCid,
		FinalPublishCid:     mb.finalPublishCid,
		DealID:              mb.dealId,
		SectorsStatusDealID: mb.sectorsStatusDealId,
		SealedBytes:         mb.rb,
		SectorID:            mb.sectorId,
		Offset:              mb.offset,
	}
}

type StubbedMinerOutput struct {
	PublishCid          cid.Cid
	FinalPublishCid     cid.Cid
	DealID              abi.DealID
	SectorsStatusDealID abi.DealID
	SealedBytes         *[]byte
	SectorID            abi.SectorNumber
	Offset              abi.PaddedPieceSize
}
