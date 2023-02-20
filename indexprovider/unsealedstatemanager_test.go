package indexprovider

import (
	"context"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/db/migrations"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	"github.com/filecoin-project/lotus/markets/idxprov"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	"github.com/golang/mock/gomock"
	"github.com/ipni/index-provider/metadata"
	mock_provider "github.com/ipni/index-provider/mock"
	"github.com/stretchr/testify/require"
	"testing"
)

// Empty response from MinerAPI.StorageList()
func TestUnsealedStateManagerEmptyStorageList(t *testing.T) {
	usm, _, _ := setup(t)

	// Check for updates with an empty response from MinerAPI.StorageList()
	err := usm.checkForUpdates(context.Background())
	require.NoError(t, err)
}

// Only announce sectors that are in the boost database
func TestUnsealedStateManagerMatchingDealOnly(t *testing.T) {
	ctx := context.Background()
	usm, storageMiner, prov := setup(t)

	// Add a deal to the database
	deals, err := db.GenerateNDeals(1)
	require.NoError(t, err)
	err = usm.dealsDB.Insert(ctx, &deals[0])
	require.NoError(t, err)

	// Set the response from MinerAPI.StorageList() to be two unsealed sectors
	minerID, err := address.IDFromAddress(deals[0].ClientDealProposal.Proposal.Provider)
	require.NoError(t, err)
	storageMiner.storageList = map[storiface.ID][]storiface.Decl{
		// This sector matches the deal in the database
		"uuid-existing-deal": {{
			SectorID:       abi.SectorID{Miner: abi.ActorID(minerID), Number: deals[0].SectorID},
			SectorFileType: storiface.FTUnsealed,
		}},
		// This sector should be ignored because the sector ID doesn't match
		// any deal in the boost table
		"uuid-unknown-deal": {{
			SectorID:       abi.SectorID{Miner: abi.ActorID(minerID), Number: deals[0].SectorID + 1},
			SectorFileType: storiface.FTUnsealed,
		}},
	}

	// Expect checkForUpdates to call NotifyPut exactly once, because only
	// one sector is in the boost database
	prov.EXPECT().NotifyPut(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1)

	err = usm.checkForUpdates(ctx)
	require.NoError(t, err)
}

// Tests that various scenarios of sealing state changes produce the expected
// calls to NotifyPut / NotifyRemove
func TestUnsealedStateManagerStateChange(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name                 string
		storageListResponse1 func(sectorID abi.SectorID) *storiface.Decl
		storageListResponse2 func(sectorID abi.SectorID) *storiface.Decl
		expect               func(*mock_provider.MockInterfaceMockRecorder, market.DealProposal)
	}{{
		name: "unsealed -> sealed",
		storageListResponse1: func(sectorID abi.SectorID) *storiface.Decl {
			return &storiface.Decl{
				SectorID:       sectorID,
				SectorFileType: storiface.FTUnsealed,
			}
		},
		storageListResponse2: func(sectorID abi.SectorID) *storiface.Decl {
			return &storiface.Decl{
				SectorID:       sectorID,
				SectorFileType: storiface.FTSealed,
			}
		},
		expect: func(prov *mock_provider.MockInterfaceMockRecorder, prop market.DealProposal) {
			// Expect a call to NotifyPut with fast retrieval = true (unsealed)
			prov.NotifyPut(gomock.Any(), gomock.Any(), gomock.Any(), metadata.Default.New(&metadata.GraphsyncFilecoinV1{
				PieceCID:      prop.PieceCID,
				VerifiedDeal:  prop.VerifiedDeal,
				FastRetrieval: true,
			})).Times(1)

			// Expect a call to NotifyPut with fast retrieval = false (sealed)
			prov.NotifyPut(gomock.Any(), gomock.Any(), gomock.Any(), metadata.Default.New(&metadata.GraphsyncFilecoinV1{
				PieceCID:      prop.PieceCID,
				VerifiedDeal:  prop.VerifiedDeal,
				FastRetrieval: false,
			})).Times(1)
		},
	}, {
		name: "sealed -> unsealed",
		storageListResponse1: func(sectorID abi.SectorID) *storiface.Decl {
			return &storiface.Decl{
				SectorID:       sectorID,
				SectorFileType: storiface.FTSealed,
			}
		},
		storageListResponse2: func(sectorID abi.SectorID) *storiface.Decl {
			return &storiface.Decl{
				SectorID:       sectorID,
				SectorFileType: storiface.FTUnsealed,
			}
		},
		expect: func(prov *mock_provider.MockInterfaceMockRecorder, prop market.DealProposal) {
			// Expect a call to NotifyPut with fast retrieval = false (sealed)
			prov.NotifyPut(gomock.Any(), gomock.Any(), gomock.Any(), metadata.Default.New(&metadata.GraphsyncFilecoinV1{
				PieceCID:      prop.PieceCID,
				VerifiedDeal:  prop.VerifiedDeal,
				FastRetrieval: false,
			})).Times(1)

			// Expect a call to NotifyPut with fast retrieval = true (unsealed)
			prov.NotifyPut(gomock.Any(), gomock.Any(), gomock.Any(), metadata.Default.New(&metadata.GraphsyncFilecoinV1{
				PieceCID:      prop.PieceCID,
				VerifiedDeal:  prop.VerifiedDeal,
				FastRetrieval: true,
			})).Times(1)
		},
	}, {
		name: "unsealed -> unsealed (no change)",
		storageListResponse1: func(sectorID abi.SectorID) *storiface.Decl {
			return &storiface.Decl{
				SectorID:       sectorID,
				SectorFileType: storiface.FTUnsealed,
			}
		},
		storageListResponse2: func(sectorID abi.SectorID) *storiface.Decl {
			return &storiface.Decl{
				SectorID:       sectorID,
				SectorFileType: storiface.FTUnsealed,
			}
		},
		expect: func(prov *mock_provider.MockInterfaceMockRecorder, prop market.DealProposal) {
			// Expect only one call to NotifyPut with fast retrieval = true (unsealed)
			// because the state of the sector doesn't change on the second call
			prov.NotifyPut(gomock.Any(), gomock.Any(), gomock.Any(), metadata.Default.New(&metadata.GraphsyncFilecoinV1{
				PieceCID:      prop.PieceCID,
				VerifiedDeal:  prop.VerifiedDeal,
				FastRetrieval: true,
			})).Times(1)
		},
	}, {
		name: "unsealed -> removed",
		storageListResponse1: func(sectorID abi.SectorID) *storiface.Decl {
			return &storiface.Decl{
				SectorID:       sectorID,
				SectorFileType: storiface.FTUnsealed,
			}
		},
		storageListResponse2: func(sectorID abi.SectorID) *storiface.Decl {
			return nil
		},
		expect: func(prov *mock_provider.MockInterfaceMockRecorder, prop market.DealProposal) {
			// Expect a call to NotifyPut with fast retrieval = true (unsealed)
			prov.NotifyPut(gomock.Any(), gomock.Any(), gomock.Any(), metadata.Default.New(&metadata.GraphsyncFilecoinV1{
				PieceCID:      prop.PieceCID,
				VerifiedDeal:  prop.VerifiedDeal,
				FastRetrieval: true,
			})).Times(1)

			// Expect a call to NotifyRemove because the sector is no longer in the list response
			prov.NotifyRemove(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
		},
	}, {
		name: "sealed -> removed",
		storageListResponse1: func(sectorID abi.SectorID) *storiface.Decl {
			return &storiface.Decl{
				SectorID:       sectorID,
				SectorFileType: storiface.FTSealed,
			}
		},
		storageListResponse2: func(sectorID abi.SectorID) *storiface.Decl {
			return nil
		},
		expect: func(prov *mock_provider.MockInterfaceMockRecorder, prop market.DealProposal) {
			// Expect a call to NotifyPut with fast retrieval = false (sealed)
			prov.NotifyPut(gomock.Any(), gomock.Any(), gomock.Any(), metadata.Default.New(&metadata.GraphsyncFilecoinV1{
				PieceCID:      prop.PieceCID,
				VerifiedDeal:  prop.VerifiedDeal,
				FastRetrieval: false,
			})).Times(1)

			// Expect a call to NotifyRemove because the sector is no longer in the list response
			prov.NotifyRemove(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
		},
	}, {
		name: "removed -> unsealed",
		storageListResponse1: func(sectorID abi.SectorID) *storiface.Decl {
			return nil
		},
		storageListResponse2: func(sectorID abi.SectorID) *storiface.Decl {
			return &storiface.Decl{
				SectorID:       sectorID,
				SectorFileType: storiface.FTUnsealed,
			}
		},
		expect: func(prov *mock_provider.MockInterfaceMockRecorder, prop market.DealProposal) {
			// Expect a call to NotifyPut with fast retrieval = true (unsealed)
			prov.NotifyPut(gomock.Any(), gomock.Any(), gomock.Any(), metadata.Default.New(&metadata.GraphsyncFilecoinV1{
				PieceCID:      prop.PieceCID,
				VerifiedDeal:  prop.VerifiedDeal,
				FastRetrieval: true,
			})).Times(1)
		},
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			usm, storageMiner, prov := setup(t)

			// Add a deal to the database
			deals, err := db.GenerateNDeals(1)
			require.NoError(t, err)
			err = usm.dealsDB.Insert(ctx, &deals[0])
			require.NoError(t, err)

			// Set up expectations (automatically verified when the test exits)
			prop := deals[0].ClientDealProposal.Proposal
			tc.expect(prov.EXPECT(), prop)

			minerID, err := address.IDFromAddress(deals[0].ClientDealProposal.Proposal.Provider)
			require.NoError(t, err)

			// Set the first response from MinerAPI.StorageList()
			resp1 := tc.storageListResponse1(abi.SectorID{Miner: abi.ActorID(minerID), Number: deals[0].SectorID})
			storageMiner.storageList = map[storiface.ID][]storiface.Decl{}
			if resp1 != nil {
				storageMiner.storageList["uuid"] = []storiface.Decl{*resp1}
			}

			// Trigger check for updates
			err = usm.checkForUpdates(ctx)
			require.NoError(t, err)

			// Set the second response from MinerAPI.StorageList()
			resp2 := tc.storageListResponse2(abi.SectorID{Miner: abi.ActorID(minerID), Number: deals[0].SectorID})
			storageMiner.storageList = map[storiface.ID][]storiface.Decl{}
			if resp2 != nil {
				storageMiner.storageList["uuid"] = []storiface.Decl{*resp2}
			}

			// Trigger check for updates again
			err = usm.checkForUpdates(ctx)
			require.NoError(t, err)
		})
	}
}

func setup(t *testing.T) (*UnsealedStateManager, *mockApiStorageMiner, *mock_provider.MockInterface) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	prov := mock_provider.NewMockInterface(ctrl)

	sqldb := db.CreateTestTmpDB(t)
	require.NoError(t, db.CreateAllBoostTables(ctx, sqldb, sqldb))
	require.NoError(t, migrations.Migrate(sqldb))

	dealsDB := db.NewDealsDB(sqldb)
	sectorStateDB := db.NewSectorStateDB(sqldb)
	storageMiner := &mockApiStorageMiner{}

	wrapper := &Wrapper{
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
	return &usm, storageMiner, prov
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
