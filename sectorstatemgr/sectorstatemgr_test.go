package sectorstatemgr

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/db/migrations"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/sectorstatemgr/mock"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	lotusmocks "github.com/filecoin-project/lotus/api/mocks"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
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
	minerApi := mock.NewMockStorageAPI(ctrl)

	maddr, _ := address.NewIDAddress(1)
	mid, _ := address.IDFromAddress(maddr)
	aid := abi.ActorID(mid)

	// setup sectorstatemgr
	mgr := &SectorStateMgr{
		cfg:         cfg,
		minerApi:    minerApi,
		fullnodeApi: fullnodeApi,
		Maddr:       maddr,

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
				sid3 := abi.SectorID{Miner: aid, Number: deals[0].SectorID}
				sid4 := abi.SectorID{Miner: aid, Number: deals[1].SectorID}
				sid5 := abi.SectorID{Miner: aid, Number: deals[2].SectorID}
				sid6 := abi.SectorID{Miner: aid, Number: deals[3].SectorID}

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
					minerApi.EXPECT().StorageList(gomock.Any()).Return(input_StorageList1, nil)
					fullnodeApi.EXPECT().StateMinerActiveSectors(gomock.Any(), gomock.Any(), gomock.Any()).Return(input_StateMinerActiveSectors1, nil)
					fullnodeApi.EXPECT().StateMinerSectors(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)

					minerApi.EXPECT().StorageList(gomock.Any()).Return(input_StorageList2, nil)
					fullnodeApi.EXPECT().StateMinerActiveSectors(gomock.Any(), gomock.Any(), gomock.Any()).Return(input_StateMinerActiveSectors2, nil)
					fullnodeApi.EXPECT().StateMinerSectors(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)

					fullnodeApi.EXPECT().ChainHead(gomock.Any()).Times(2)
				}

				expected2 := &SectorStateUpdates{
					Updates: map[abi.SectorID]db.SealState{
						sid3: db.SealStateUnsealed,
						sid4: db.SealStateSealed,
						sid6: db.SealStateRemoved,
					},
					ActiveSectors: map[abi.SectorID]struct{}{
						sid3: {},
						sid4: {},
					},
					SectorStates: map[abi.SectorID]db.SealState{
						sid3: db.SealStateUnsealed,
						sid4: db.SealStateSealed,
						sid5: db.SealStateCache,
					},
					SectorWithDeals: map[abi.SectorID]struct{}{},
				}

				exerciseAndVerify := func() {
					// setup initial state of db
					err := mgr.checkForUpdates(ctx)
					require.NoError(t, err)

					// trigger refreshState and later verify resulting struct
					got2, err := mgr.refreshState(ctx)
					require.NoError(t, err)

					zero := time.Time{}
					require.NotEqual(t, got2.UpdatedAt, zero)

					//null timestamp, so that we can do deep equal
					got2.UpdatedAt = zero

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
				sid1 := abi.SectorID{Miner: aid, Number: deals[0].SectorID}
				sid2 := abi.SectorID{Miner: aid, Number: deals[1].SectorID}
				sid3 := abi.SectorID{Miner: aid, Number: deals[2].SectorID}

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
					minerApi.EXPECT().StorageList(gomock.Any()).Return(input_StorageList, nil)
					fullnodeApi.EXPECT().ChainHead(gomock.Any()).Times(1)
					fullnodeApi.EXPECT().StateMinerSectors(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
					fullnodeApi.EXPECT().StateMinerActiveSectors(gomock.Any(), gomock.Any(), gomock.Any()).Return(input_StateMinerActiveSectors, nil)
				}

				expected := &SectorStateUpdates{
					Updates: map[abi.SectorID]db.SealState{
						sid1: db.SealStateUnsealed,
						sid2: db.SealStateSealed,
						sid3: db.SealStateSealed,
					},
					ActiveSectors: map[abi.SectorID]struct{}{
						sid1: {},
						sid2: {},
					},
					SectorStates: map[abi.SectorID]db.SealState{
						sid1: db.SealStateUnsealed,
						sid2: db.SealStateSealed,
						sid3: db.SealStateSealed,
					},
					SectorWithDeals: map[abi.SectorID]struct{}{},
				}

				exerciseAndVerify := func() {
					got, err := mgr.refreshState(ctx)
					require.NoError(t, err)

					zero := time.Time{}
					require.NotEqual(t, got.UpdatedAt, zero)

					//null timestamp, so that we can do deep equal
					got.UpdatedAt = zero

					require.True(t, reflect.DeepEqual(expected, got), "expected: %s, got: %s", spew.Sdump(expected), spew.Sdump(got))
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
