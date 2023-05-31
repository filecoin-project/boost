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
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestRefreshState(t *testing.T) {
	type fixtures struct {
		input    interface{}
		expected *SectorStateUpdates
	}

	tests := []struct {
		description string
		f           func() fixtures
	}{
		{
			description: "first",
			f: func() fixtures {
				deals, err := db.GenerateNDeals(2)
				require.NoError(t, err)
				sid1 := abi.SectorID{Miner: abi.ActorID(1), Number: deals[0].SectorID}
				sid2 := abi.SectorID{Miner: abi.ActorID(1), Number: deals[1].SectorID}
				list := map[storiface.ID][]storiface.Decl{
					"storage-location-uuid1": {
						{
							SectorID:       sid1,
							SectorFileType: storiface.FTUnsealed,
						},
						{
							SectorID:       sid1,
							SectorFileType: storiface.FTSealed,
						},
						{
							SectorID:       sid1,
							SectorFileType: storiface.FTCache,
						},
					},
					"storage-location-uuid2": {
						{
							SectorID:       sid2,
							SectorFileType: storiface.FTSealed,
						},
						{
							SectorID:       sid2,
							SectorFileType: storiface.FTCache,
						},
					},
				}

				return fixtures{
					input: list,
					expected: &SectorStateUpdates{
						Updates: map[abi.SectorID]db.SealState{
							sid1: db.SealStateUnsealed,
							sid2: db.SealStateSealed,
						},
						ActiveSectors: map[abi.SectorID]struct{}{},
						SectorStates: map[abi.SectorID]db.SealState{
							sid1: db.SealStateUnsealed,
							sid2: db.SealStateSealed,
						},
					},
				}
			},
		},
	}

	// setup for test
	ctx := context.Background()

	cfg := config.StorageConfig{
		StorageListRefreshDuration: config.Duration(100 * time.Millisecond),
	}

	sqldb := db.CreateTestTmpDB(t)
	require.NoError(t, db.CreateAllBoostTables(ctx, sqldb, sqldb))
	require.NoError(t, migrations.Migrate(sqldb))

	sdb := db.NewSectorStateDB(sqldb)

	ctrl := gomock.NewController(t)

	// setup mocks
	fullnodeApi := lotusmocks.NewMockFullNode(ctrl)
	minerApi := mock.NewMockStorageAPI(ctrl)

	maddr, _ := address.NewIDAddress(0)

	// setup sectorstatemgr
	mgr := &SectorStateMgr{
		cfg:         cfg,
		minerApi:    minerApi,
		fullnodeApi: fullnodeApi,
		Maddr:       maddr,

		PubSub: NewPubSub(),

		sdb: sdb,
	}

	for _, tt := range tests {
		// f() builds fixtures for a specific test case, namely all required input for test and expected result
		fixt := tt.f()

		minerApi.EXPECT().StorageList(gomock.Any()).Return(fixt.input, nil)
		fullnodeApi.EXPECT().ChainHead(gomock.Any()).Times(1)
		fullnodeApi.EXPECT().StateMinerActiveSectors(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)

		got, err := mgr.refreshState(ctx)
		require.NoError(t, err)

		zero := time.Time{}
		require.NotEqual(t, got.UpdatedAt, zero)

		//null timestamp, so that we can do deep equal
		got.UpdatedAt = zero

		require.True(t, reflect.DeepEqual(fixt.expected, got), "expected: %s, got: %s", spew.Sdump(fixt.expected), spew.Sdump(got))
	}

}
