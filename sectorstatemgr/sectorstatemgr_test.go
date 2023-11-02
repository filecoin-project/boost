package sectorstatemgr

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/db/migrations"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/sectorstatemgr/mock"
	sectorstatemgr_types "github.com/filecoin-project/boost/sectorstatemgr/types"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin/v9/miner"
	lotusmocks "github.com/filecoin-project/lotus/api/mocks"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestRefreshState(t *testing.T) {
	// setup for test
	ctx := context.Background()

	cfg := config.StorageConfig{
		StorageListRefreshDuration: config.Duration(100 * time.Millisecond),
	}

	ctrl := gomock.NewController(t)

	// setup mocks
	fullnodeApi := lotusmocks.NewMockFullNode(ctrl)
	minerApi1 := mock.NewMockStorageAPI(ctrl)
	minerApi2 := mock.NewMockStorageAPI(ctrl)

	maddr1, _ := address.NewIDAddress(1)
	maddr2, _ := address.NewIDAddress(2)

	mid1, _ := address.IDFromAddress(maddr1)
	mid2, _ := address.IDFromAddress(maddr2)

	aid1 := abi.ActorID(mid1)
	aid2 := abi.ActorID(mid2)

	mus := make(map[address.Address]*sync.Mutex)
	mus[maddr1] = &sync.Mutex{}
	mus[maddr2] = &sync.Mutex{}

	// setup sectorstatemgr
	mgr := &SectorStateMgr{
		cfg:             cfg,
		minerApis:       []sectorstatemgr_types.StorageAPI{minerApi1, minerApi2},
		fullnodeApi:     fullnodeApi,
		Maddrs:          []address.Address{maddr1, maddr2},
		LatestUpdates:   make(map[address.Address]*SectorStateUpdates),
		LatestUpdateMus: mus,

		PubSub: NewPubSub(),
	}

	type fixtures struct {
		mockExpectations  func()
		exerciseAndVerify func()
	}

	tests := []struct {
		description string
		f           func() fixtures
	}{
		{
			description: "four deals - sealed->unsealed, unsealed->sealed, cached, removed",
			f: func() fixtures {
				sqldb := db.CreateTestTmpDB(t)
				require.NoError(t, db.CreateAllBoostTables(ctx, sqldb, sqldb))
				require.NoError(t, migrations.Migrate(sqldb))
				mgr.sdb = db.NewSectorStateDB(sqldb)

				deals, err := db.GenerateNDeals(4)
				require.NoError(t, err)
				sid3 := abi.SectorID{Miner: aid1, Number: deals[0].SectorID}
				sid4 := abi.SectorID{Miner: aid1, Number: deals[1].SectorID}
				sid5 := abi.SectorID{Miner: aid1, Number: deals[2].SectorID}
				sid6 := abi.SectorID{Miner: aid1, Number: deals[3].SectorID}

				input_StorageList1 := map[storiface.ID][]storiface.Decl{
					"storage-location-uuid1": {
						{SectorID: sid3, SectorFileType: storiface.FTSealed},
						{SectorID: sid3, SectorFileType: storiface.FTCache},
					},
					"storage-location-uuid2": {
						{SectorID: sid4, SectorFileType: storiface.FTUnsealed},
						{SectorID: sid4, SectorFileType: storiface.FTSealed},
						{SectorID: sid4, SectorFileType: storiface.FTCache},
						{SectorID: sid5, SectorFileType: storiface.FTUpdateCache},
						{SectorID: sid6, SectorFileType: storiface.FTSealed},
					},
				}
				input_StateMinerActiveSectors1 := []*miner.SectorOnChainInfo{
					{SectorNumber: sid3.Number},
					{SectorNumber: sid4.Number},
				}

				input_StorageList2 := map[storiface.ID][]storiface.Decl{
					"storage-location-uuid1": {
						{SectorID: sid3, SectorFileType: storiface.FTUnsealed},
						{SectorID: sid3, SectorFileType: storiface.FTSealed},
						{SectorID: sid3, SectorFileType: storiface.FTCache},
					},
					"storage-location-uuid2": {
						{SectorID: sid4, SectorFileType: storiface.FTSealed},
						{SectorID: sid4, SectorFileType: storiface.FTCache},
						{SectorID: sid5, SectorFileType: storiface.FTUpdateCache},
					},
				}
				input_StateMinerActiveSectors2 := []*miner.SectorOnChainInfo{
					{SectorNumber: sid3.Number},
					{SectorNumber: sid4.Number},
				}

				mockExpectations := func() {
					minerApi1.EXPECT().StorageList(gomock.Any()).Return(input_StorageList1, nil)
					fullnodeApi.EXPECT().StateMinerActiveSectors(gomock.Any(), gomock.Eq(maddr1), gomock.Any()).Return(input_StateMinerActiveSectors1, nil)
					fullnodeApi.EXPECT().StateMinerSectors(gomock.Any(), gomock.Eq(maddr1), gomock.Any(), gomock.Any()).Return(nil, nil)

					minerApi1.EXPECT().StorageList(gomock.Any()).Return(input_StorageList2, nil)
					fullnodeApi.EXPECT().StateMinerActiveSectors(gomock.Any(), gomock.Eq(maddr1), gomock.Any()).Return(input_StateMinerActiveSectors2, nil)
					fullnodeApi.EXPECT().StateMinerSectors(gomock.Any(), gomock.Eq(maddr1), gomock.Any(), gomock.Any()).Return(nil, nil)

					minerApi2.EXPECT().StorageList(gomock.Any()).Return(nil, nil).Times(2)
					fullnodeApi.EXPECT().StateMinerActiveSectors(gomock.Any(), gomock.Eq(maddr2), gomock.Any()).Return(nil, nil).Times(2)
					fullnodeApi.EXPECT().StateMinerSectors(gomock.Any(), gomock.Eq(maddr2), gomock.Any(), gomock.Any()).Return(nil, nil).Times(2)

					fullnodeApi.EXPECT().ChainHead(gomock.Any()).Times(4)
				}

				expected1 := &SectorStateUpdates{
					Maddr: maddr1,
					Updates: map[abi.SectorID]db.SealState{
						sid3: db.SealStateUnsealed,
						sid4: db.SealStateSealed,
						sid6: db.SealStateRemoved,
					},
					ActiveSectors: map[abi.SectorID]struct{}{
						sid3: struct{}{},
						sid4: struct{}{},
					},
					SectorStates: map[abi.SectorID]db.SealState{
						sid3: db.SealStateUnsealed,
						sid4: db.SealStateSealed,
						sid5: db.SealStateCache,
					},
					SectorWithDeals: map[abi.SectorID]struct{}{},
				}

				// miner2's update should be empty
				expected2 := &SectorStateUpdates{
					Maddr:           maddr2,
					Updates:         map[abi.SectorID]db.SealState{},
					ActiveSectors:   map[abi.SectorID]struct{}{},
					SectorStates:    map[abi.SectorID]db.SealState{},
					SectorWithDeals: map[abi.SectorID]struct{}{},
				}

				exerciseAndVerify := func() {
					// setup initial state of db
					err := mgr.checkForUpdates(ctx)
					require.NoError(t, err)

					// trigger refreshState and later verify resulting struct
					ssus, err := mgr.refreshState(ctx)
					require.NoError(t, err)

					require.Equal(t, 2, len(ssus))

					var got1 *SectorStateUpdates
					var got2 *SectorStateUpdates
					for _, ssu := range ssus {
						switch ssu.Maddr {
						case maddr1:
							got1 = ssu
						case maddr2:
							got2 = ssu
						}
					}

					zero := time.Time{}

					require.NotEqual(t, got1.UpdatedAt, zero)
					require.NotEqual(t, got2.UpdatedAt, zero)

					//null timestamp, so that we can do deep equal
					got1.UpdatedAt = zero
					got2.UpdatedAt = zero

					require.True(t, reflect.DeepEqual(expected1, got1), "expected: %s, got: %s", spew.Sdump(expected1), spew.Sdump(got1))
					require.True(t, reflect.DeepEqual(expected2, got2), "expected: %s, got: %s", spew.Sdump(expected2), spew.Sdump(got2))
				}

				return fixtures{
					mockExpectations:  mockExpectations,
					exerciseAndVerify: exerciseAndVerify,
				}
			},
		},
		{
			description: "one sealed, one unsealed, one not active - different sectors",
			f: func() fixtures {
				sqldb := db.CreateTestTmpDB(t)
				require.NoError(t, db.CreateAllBoostTables(ctx, sqldb, sqldb))
				require.NoError(t, migrations.Migrate(sqldb))
				mgr.sdb = db.NewSectorStateDB(sqldb)

				deals, err := db.GenerateNDeals(3)
				require.NoError(t, err)
				sid1 := abi.SectorID{Miner: aid1, Number: deals[0].SectorID}
				sid2 := abi.SectorID{Miner: aid1, Number: deals[1].SectorID}
				sid3 := abi.SectorID{Miner: aid1, Number: deals[2].SectorID}

				input_StorageList := map[storiface.ID][]storiface.Decl{
					"storage-location-uuid1": {
						{SectorID: sid1, SectorFileType: storiface.FTUnsealed},
						{SectorID: sid1, SectorFileType: storiface.FTSealed},
						{SectorID: sid1, SectorFileType: storiface.FTCache},
					},
					"storage-location-uuid2": {
						{SectorID: sid2, SectorFileType: storiface.FTSealed},
						{SectorID: sid2, SectorFileType: storiface.FTCache},
						{SectorID: sid3, SectorFileType: storiface.FTSealed},
					},
				}
				input_StateMinerActiveSectors := []*miner.SectorOnChainInfo{
					{SectorNumber: sid1.Number},
					{SectorNumber: sid2.Number},
				}

				mockExpectations := func() {
					minerApi1.EXPECT().StorageList(gomock.Any()).Return(input_StorageList, nil)
					fullnodeApi.EXPECT().StateMinerSectors(gomock.Any(), gomock.Eq(maddr1), gomock.Any(), gomock.Any()).Return(nil, nil)
					fullnodeApi.EXPECT().StateMinerActiveSectors(gomock.Any(), gomock.Eq(maddr1), gomock.Any()).Return(input_StateMinerActiveSectors, nil)

					minerApi2.EXPECT().StorageList(gomock.Any()).Return(nil, nil)
					fullnodeApi.EXPECT().StateMinerSectors(gomock.Any(), gomock.Eq(maddr2), gomock.Any(), gomock.Any()).Return(nil, nil)
					fullnodeApi.EXPECT().StateMinerActiveSectors(gomock.Any(), gomock.Eq(maddr2), gomock.Any()).Return(nil, nil)

					fullnodeApi.EXPECT().ChainHead(gomock.Any()).Times(2)
				}

				expected1 := &SectorStateUpdates{
					Maddr: maddr1,
					Updates: map[abi.SectorID]db.SealState{
						sid1: db.SealStateUnsealed,
						sid2: db.SealStateSealed,
						sid3: db.SealStateSealed,
					},
					ActiveSectors: map[abi.SectorID]struct{}{
						sid1: struct{}{},
						sid2: struct{}{},
					},
					SectorStates: map[abi.SectorID]db.SealState{
						sid1: db.SealStateUnsealed,
						sid2: db.SealStateSealed,
						sid3: db.SealStateSealed,
					},
					SectorWithDeals: map[abi.SectorID]struct{}{},
				}

				// miner2's update should be empty
				expected2 := &SectorStateUpdates{
					Maddr:           maddr2,
					Updates:         map[abi.SectorID]db.SealState{},
					ActiveSectors:   map[abi.SectorID]struct{}{},
					SectorStates:    map[abi.SectorID]db.SealState{},
					SectorWithDeals: map[abi.SectorID]struct{}{},
				}

				exerciseAndVerify := func() {
					ssus, err := mgr.refreshState(ctx)
					require.NoError(t, err)
					require.Equal(t, 2, len(ssus))
					var got1 *SectorStateUpdates
					var got2 *SectorStateUpdates
					for _, ssu := range ssus {
						switch ssu.Maddr {
						case maddr1:
							got1 = ssu
						case maddr2:
							got2 = ssu
						}
					}

					zero := time.Time{}
					require.NotEqual(t, got1.UpdatedAt, zero)
					require.NotEqual(t, got2.UpdatedAt, zero)

					//null timestamp, so that we can do deep equal
					got1.UpdatedAt = zero
					got2.UpdatedAt = zero

					require.True(t, reflect.DeepEqual(expected1, got1), "expected: %s, got: %s", spew.Sdump(expected1), spew.Sdump(got1))
					require.True(t, reflect.DeepEqual(expected2, got2), "expected: %s, got: %s", spew.Sdump(expected2), spew.Sdump(got2))
				}

				return fixtures{
					mockExpectations:  mockExpectations,
					exerciseAndVerify: exerciseAndVerify,
				}
			},
		},
		{
			description: "different sectors, different miners",
			f: func() fixtures {
				sqldb := db.CreateTestTmpDB(t)
				require.NoError(t, db.CreateAllBoostTables(ctx, sqldb, sqldb))
				require.NoError(t, migrations.Migrate(sqldb))
				mgr.sdb = db.NewSectorStateDB(sqldb)

				deals, err := db.GenerateNDeals(3)
				require.NoError(t, err)
				sid1 := abi.SectorID{Miner: aid1, Number: deals[0].SectorID}
				sid2 := abi.SectorID{Miner: aid2, Number: deals[1].SectorID}

				input_StorageList1 := map[storiface.ID][]storiface.Decl{
					"storage-location-uuid1": {
						{SectorID: sid1, SectorFileType: storiface.FTUnsealed},
						{SectorID: sid1, SectorFileType: storiface.FTSealed},
						{SectorID: sid1, SectorFileType: storiface.FTCache},
					},
				}
				input_StorageList2 := map[storiface.ID][]storiface.Decl{
					"storage-location-uuid2": {
						{SectorID: sid2, SectorFileType: storiface.FTSealed},
						{SectorID: sid2, SectorFileType: storiface.FTCache},
					},
				}

				input_StateMinerActiveSectors1 := []*miner.SectorOnChainInfo{
					{SectorNumber: sid1.Number},
				}

				input_StateMinerActiveSectors2 := []*miner.SectorOnChainInfo{
					{SectorNumber: sid2.Number},
				}

				mockExpectations := func() {
					minerApi1.EXPECT().StorageList(gomock.Any()).Return(input_StorageList1, nil)
					fullnodeApi.EXPECT().ChainHead(gomock.Any()).Times(1)
					fullnodeApi.EXPECT().StateMinerSectors(gomock.Any(), gomock.Eq(maddr1), gomock.Any(), gomock.Any()).Return(nil, nil)
					fullnodeApi.EXPECT().StateMinerActiveSectors(gomock.Any(), gomock.Eq(maddr1), gomock.Any()).Return(input_StateMinerActiveSectors1, nil)

					minerApi2.EXPECT().StorageList(gomock.Any()).Return(input_StorageList2, nil)
					fullnodeApi.EXPECT().ChainHead(gomock.Any()).Times(1)
					fullnodeApi.EXPECT().StateMinerSectors(gomock.Any(), gomock.Eq(maddr2), gomock.Any(), gomock.Any()).Return(nil, nil)
					fullnodeApi.EXPECT().StateMinerActiveSectors(gomock.Any(), gomock.Eq(maddr2), gomock.Any()).Return(input_StateMinerActiveSectors2, nil)
				}

				expected1 := &SectorStateUpdates{
					Maddr: maddr1,
					Updates: map[abi.SectorID]db.SealState{
						sid1: db.SealStateUnsealed,
					},
					ActiveSectors: map[abi.SectorID]struct{}{
						sid1: struct{}{},
					},
					SectorStates: map[abi.SectorID]db.SealState{
						sid1: db.SealStateUnsealed,
					},
					SectorWithDeals: map[abi.SectorID]struct{}{},
				}

				expected2 := &SectorStateUpdates{
					Maddr: maddr2,
					Updates: map[abi.SectorID]db.SealState{
						sid2: db.SealStateSealed,
					},
					ActiveSectors: map[abi.SectorID]struct{}{
						sid2: struct{}{},
					},
					SectorStates: map[abi.SectorID]db.SealState{
						sid2: db.SealStateSealed,
					},
					SectorWithDeals: map[abi.SectorID]struct{}{},
				}

				exerciseAndVerify := func() {
					ssus, err := mgr.refreshState(ctx)
					require.NoError(t, err)
					require.Equal(t, 2, len(ssus))
					var got1 *SectorStateUpdates
					var got2 *SectorStateUpdates
					for _, ssu := range ssus {
						switch ssu.Maddr {
						case maddr1:
							got1 = ssu
						case maddr2:
							got2 = ssu
						default:
							require.Fail(t, "unknown miner")
						}
					}

					zero := time.Time{}
					require.NotEqual(t, got1.UpdatedAt, zero)
					require.NotEqual(t, got2.UpdatedAt, zero)

					//null timestamp, so that we can do deep equal
					got1.UpdatedAt = zero
					got2.UpdatedAt = zero

					require.True(t, reflect.DeepEqual(expected1, got1), "expected: %s, got: %s", spew.Sdump(expected1), spew.Sdump(got1))
					require.True(t, reflect.DeepEqual(expected2, got2), "expected: %s, got: %s", spew.Sdump(expected2), spew.Sdump(got2))
				}

				return fixtures{
					mockExpectations:  mockExpectations,
					exerciseAndVerify: exerciseAndVerify,
				}
			},
		},
	}

	for _, tt := range tests {
		// f() builds fixtures for a specific test case, namely all required input for test and expected result
		fixt := tt.f()

		// mockExpectations()
		fixt.mockExpectations()

		// exerciseAndVerify()
		fixt.exerciseAndVerify()
	}
}
