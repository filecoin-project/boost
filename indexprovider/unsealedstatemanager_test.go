package indexprovider

import (
	"context"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/db/migrations"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/markets/idxprov"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	"github.com/golang/mock/gomock"
	mock_provider "github.com/ipni/index-provider/mock"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestUnsealedStateManager(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	prov := mock_provider.NewMockInterface(ctrl)

	sqldb := db.CreateTestTmpDB(t)
	require.NoError(t, db.CreateAllBoostTables(ctx, sqldb, sqldb))
	require.NoError(t, migrations.Migrate(sqldb))

	dealsDB := db.NewDealsDB(sqldb)
	sectorStateDB := db.NewSectorStateDB(sqldb)
	storageMiner := &mockApiStorageMiner{}

	wrapper := Wrapper{
		enabled:     true,
		dealsDB:     dealsDB,
		prov:        prov,
		meshCreator: &meshCreatorStub{},
	}
	usm := UnsealedStateManager{
		idxprov: wrapper,
		dealsDB: dealsDB,
		sdb:     sectorStateDB,
		api:     storageMiner,
	}

	// Check for updates with an empty response from MinerAPI.StorageList()
	err := usm.checkForUpdates(ctx)
	require.NoError(t, err)

	// Add a deal to the database
	deals, err := db.GenerateNDeals(1)
	require.NoError(t, err)
	err = dealsDB.Insert(ctx, &deals[0])
	require.NoError(t, err)

	// Set the response from MinerAPI.StorageList() to be two unsealed sectors
	storageMiner.storageList = map[storiface.ID][]storiface.Decl{
		// This sector matches the deal in the database
		"uuid-existing-deal": {{
			SectorID:       abi.SectorID{Number: deals[0].SectorID},
			SectorFileType: storiface.FTUnsealed,
		}},
		// This sector should be ignored because the sector ID doesn't match
		// any deal in the boost table
		"uuid-unknown-deal": {{
			SectorID:       abi.SectorID{Number: deals[0].SectorID + 1},
			SectorFileType: storiface.FTUnsealed,
		}},
	}
	// Expect checkForUpdates to call NotifyPut exactly once
	prov.EXPECT().NotifyPut(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1)

	// Call check for updates twice. Each time it will get the same response
	// from MinerAPI.StorageList(). However the second time it should recognize
	// that it has already called NotifyPut for the given deal. So it should
	// only called NotifyPut once.
	err = usm.checkForUpdates(ctx)
	require.NoError(t, err)
	err = usm.checkForUpdates(ctx)
	require.NoError(t, err)

	// Set the response from MinerAPI.StorageList() such that the sector that
	// was previous sealed is now unsealed
	storageMiner.storageList = map[storiface.ID][]storiface.Decl{
		"uuid-sealed": {{
			SectorID:       abi.SectorID{Number: deals[0].SectorID},
			SectorFileType: storiface.FTSealed,
		}},
	}
	// Expect checkForUpdates to call NotifyRemove exactly once
	prov.EXPECT().NotifyRemove(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)

	// Check for updates
	err = usm.checkForUpdates(ctx)
	require.NoError(t, err)
}

type mockApiStorageMiner struct {
	storageList map[storiface.ID][]storiface.Decl
}

var _ ApiStorageMiner = (*mockApiStorageMiner)(nil)

func (m mockApiStorageMiner) StorageList(ctx context.Context) (map[storiface.ID][]storiface.Decl, error) {
	return m.storageList, nil
}

type meshCreatorStub struct {
}

var _ idxprov.MeshCreator = (*meshCreatorStub)(nil)

func (m *meshCreatorStub) Connect(context.Context) error {
	return nil
}
