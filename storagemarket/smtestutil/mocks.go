package smtestutil

import (
	"context"
	"io"
	"io/ioutil"
	"math/rand"
	"sync"

	lapi "github.com/filecoin-project/lotus/api"

	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"

	mock_types "github.com/filecoin-project/boost/storagemarket/types/mocks"
	market2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/market"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
)

type MinerStub struct {
	*mock_types.MockDealPublisher
	*mock_types.MockMinerHelper
	*mock_types.MockPieceAdder

	lk                    sync.Mutex
	unblockPublish        map[uuid.UUID]chan struct{}
	unblockWaitForPublish map[uuid.UUID]chan struct{}
	unblockAddPiece       map[uuid.UUID]chan struct{}
}

func NewMinerStub(ctrl *gomock.Controller) *MinerStub {
	return &MinerStub{
		MockDealPublisher: mock_types.NewMockDealPublisher(ctrl),
		MockMinerHelper:   mock_types.NewMockMinerHelper(ctrl),
		MockPieceAdder:    mock_types.NewMockPieceAdder(ctrl),

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

func (ms *MinerStub) ForDeal(dp *types.DealParams) *MinerStubBuilder {
	return &MinerStubBuilder{
		stub: ms,
		dp:   dp,
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

func (mb *MinerStubBuilder) SetupPublish(blocking bool) *MinerStubBuilder {
	publishCid := testutil.GenerateCid()

	mb.stub.lk.Lock()
	if blocking {
		mb.stub.unblockPublish[mb.dp.DealUUID] = make(chan struct{})
	}
	mb.stub.lk.Unlock()

	mb.stub.MockDealPublisher.EXPECT().Publish(gomock.Any(), gomock.Eq(mb.dp.DealUUID), gomock.Eq(mb.dp.ClientDealProposal)).DoAndReturn(func(_ context.Context, _ uuid.UUID, _ market2.ClientDealProposal) (cid.Cid, error) {
		mb.stub.lk.Lock()
		ch := mb.stub.unblockPublish[mb.dp.DealUUID]
		mb.stub.lk.Unlock()
		if ch != nil {
			<-ch
		}

		return publishCid, nil
	})

	mb.publishCid = publishCid

	return mb
}

func (mb *MinerStubBuilder) SetupPublishConfirm(blocking bool) *MinerStubBuilder {
	finalPublishCid := testutil.GenerateCid()
	dealId := abi.DealID(rand.Intn(100))

	mb.stub.lk.Lock()
	if blocking {
		mb.stub.unblockWaitForPublish[mb.dp.DealUUID] = make(chan struct{})
	}
	mb.stub.lk.Unlock()

	mb.stub.MockMinerHelper.EXPECT().WaitForPublishDeals(gomock.Any(), gomock.Eq(mb.publishCid), gomock.Eq(mb.dp.ClientDealProposal.Proposal)).DoAndReturn(func(_ context.Context, _ cid.Cid, _ market2.DealProposal) (*storagemarket.PublishDealsWaitResult, error) {
		mb.stub.lk.Lock()
		ch := mb.stub.unblockWaitForPublish[mb.dp.DealUUID]
		mb.stub.lk.Unlock()
		if ch != nil {
			<-ch
		}

		return &storagemarket.PublishDealsWaitResult{
			DealID:   dealId,
			FinalCid: finalPublishCid,
		}, nil
	})

	mb.finalPublishCid = finalPublishCid
	mb.dealId = dealId
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

	sectorID := abi.SectorNumber(rand.Intn(100))
	offset := abi.PaddedPieceSize(rand.Intn(101))

	var readBytes []byte
	mb.stub.MockPieceAdder.EXPECT().AddPiece(gomock.Any(), gomock.Eq(mb.dp.ClientDealProposal.Proposal.PieceSize.Unpadded()), gomock.Any(), gomock.Eq(sdInfo)).DoAndReturn(func(_ context.Context, _ abi.UnpaddedPieceSize, r io.Reader, _ api.PieceDealInfo) (abi.SectorNumber, abi.PaddedPieceSize, error) {
		mb.stub.lk.Lock()
		ch := mb.stub.unblockAddPiece[mb.dp.DealUUID]
		mb.stub.lk.Unlock()

		if ch != nil {
			<-ch
		}

		var err error
		readBytes, err = ioutil.ReadAll(r)
		return sectorID, offset, err
	})

	mb.sectorId = sectorID
	mb.offset = offset
	mb.rb = &readBytes

	return mb
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
