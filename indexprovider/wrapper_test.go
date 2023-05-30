package indexprovider

import (
	"context"
	"testing"

	"github.com/filecoin-project/boost-gfm/storagemarket"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/db/migrations"
	"github.com/filecoin-project/boost/indexprovider/mock"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	"github.com/filecoin-project/lotus/markets/idxprov"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	"github.com/golang/mock/gomock"
	"github.com/ipni/index-provider/metadata"
	mock_provider "github.com/ipni/index-provider/mock"
	"github.com/stretchr/testify/require"
)

// Empty response from MinerAPI.StorageList()
func TestUnsealedStateManagerEmptyStorageList(t *testing.T) {
	wrapper, legacyStorageProvider, _, _ := setup(t)
	legacyStorageProvider.EXPECT().ListLocalDeals().AnyTimes().Return(nil, nil)

	// Check for updates with an empty response from MinerAPI.StorageList()
	err := wrapper.handleUpdates(context.Background(), nil)
	require.NoError(t, err)
}

// Only announce sectors for deals that are in the boost database or
// legacy datastore
func TestSectorStateManagerMatchingDealOnly(t *testing.T) {
	ctx := context.Background()

	runTest := func(t *testing.T, wrapper *Wrapper, storageMiner *mockApiStorageMiner, prov *mock_provider.MockInterface, provAddr address.Address, sectorNum abi.SectorNumber) {
		// Set the response from MinerAPI.StorageList() to be two unsealed sectors
		minerID, err := address.IDFromAddress(provAddr)
		require.NoError(t, err)

		// Expect handleUpdates to call NotifyPut exactly once, because only
		// one sector from the storage list is in the database
		prov.EXPECT().NotifyPut(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1)

		sus := map[abi.SectorID]db.SealState{
			abi.SectorID{Miner: abi.ActorID(minerID), Number: sectorNum}:     db.SealStateUnsealed,
			abi.SectorID{Miner: abi.ActorID(minerID), Number: sectorNum + 1}: db.SealStateUnsealed,
		}
		err = wrapper.handleUpdates(ctx, sus)
		require.NoError(t, err)
	}

	t.Run("deal in boost db", func(t *testing.T) {
		wrapper, legacyStorageProvider, storageMiner, prov := setup(t)
		legacyStorageProvider.EXPECT().ListLocalDeals().Return(nil, nil)

		// Add a deal to the database
		deals, err := db.GenerateNDeals(1)
		require.NoError(t, err)
		err = wrapper.dealsDB.Insert(ctx, &deals[0])
		require.NoError(t, err)

		provAddr := deals[0].ClientDealProposal.Proposal.Provider
		sectorNum := deals[0].SectorID
		runTest(t, wrapper, storageMiner, prov, provAddr, sectorNum)
	})

	t.Run("deal in legacy datastore", func(t *testing.T) {
		wrapper, legacyStorageProvider, storageMiner, prov := setup(t)

		// Simulate returning a deal from the legacy datastore
		boostDeals, err := db.GenerateNDeals(1)
		require.NoError(t, err)

		sectorNum := abi.SectorNumber(10)
		deals := []storagemarket.MinerDeal{{
			ClientDealProposal: boostDeals[0].ClientDealProposal,
			SectorNumber:       sectorNum,
		}}
		legacyStorageProvider.EXPECT().ListLocalDeals().Return(deals, nil)

		provAddr := deals[0].ClientDealProposal.Proposal.Provider
		runTest(t, wrapper, storageMiner, prov, provAddr, sectorNum)
	})
}

// Tests that various scenarios of sealing state changes produce the expected
// calls to NotifyPut / NotifyRemove
func TestSectorStateManagerStateChangeToIndexer(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name         string
		initialState func(sectorID abi.SectorID) *storiface.Decl
		sus          func(sectorID abi.SectorID) map[abi.SectorID]db.SealState
		expect       func(*mock_provider.MockInterfaceMockRecorder, market.DealProposal)
	}{{
		name: "unsealed -> sealed",
		initialState: func(sectorID abi.SectorID) *storiface.Decl {
			return &storiface.Decl{
				SectorID:       sectorID,
				SectorFileType: storiface.FTUnsealed,
			}
		},
		sus: func(sectorID abi.SectorID) map[abi.SectorID]db.SealState {
			return map[abi.SectorID]db.SealState{
				sectorID: db.SealStateSealed,
			}
		},
		expect: func(prov *mock_provider.MockInterfaceMockRecorder, prop market.DealProposal) {
			// Expect a call to NotifyPut with fast retrieval = true (unsealed)
			//prov.NotifyPut(gomock.Any(), gomock.Any(), gomock.Any(), metadata.Default.New(&metadata.GraphsyncFilecoinV1{
			//PieceCID:      prop.PieceCID,
			//VerifiedDeal:  prop.VerifiedDeal,
			//FastRetrieval: true,
			//})).Times(1)

			// Expect a call to NotifyPut with fast retrieval = false (sealed)
			prov.NotifyPut(gomock.Any(), gomock.Any(), gomock.Any(), metadata.Default.New(&metadata.GraphsyncFilecoinV1{
				PieceCID:      prop.PieceCID,
				VerifiedDeal:  prop.VerifiedDeal,
				FastRetrieval: false,
			})).Times(1)
		},
	}, {
		name: "sealed -> unsealed",
		initialState: func(sectorID abi.SectorID) *storiface.Decl {
			return &storiface.Decl{
				SectorID:       sectorID,
				SectorFileType: storiface.FTSealed,
			}
		},
		sus: func(sectorID abi.SectorID) map[abi.SectorID]db.SealState {
			return map[abi.SectorID]db.SealState{
				sectorID: db.SealStateUnsealed,
			}
		},
		expect: func(prov *mock_provider.MockInterfaceMockRecorder, prop market.DealProposal) {
			// Expect a call to NotifyPut with fast retrieval = false (sealed)
			//prov.NotifyPut(gomock.Any(), gomock.Any(), gomock.Any(), metadata.Default.New(&metadata.GraphsyncFilecoinV1{
			//PieceCID:      prop.PieceCID,
			//VerifiedDeal:  prop.VerifiedDeal,
			//FastRetrieval: false,
			//})).Times(1)

			// Expect a call to NotifyPut with fast retrieval = true (unsealed)
			prov.NotifyPut(gomock.Any(), gomock.Any(), gomock.Any(), metadata.Default.New(&metadata.GraphsyncFilecoinV1{
				PieceCID:      prop.PieceCID,
				VerifiedDeal:  prop.VerifiedDeal,
				FastRetrieval: true,
			})).Times(1)
		},
	}, {
		name: "unsealed -> unsealed (no change)",
		initialState: func(sectorID abi.SectorID) *storiface.Decl {
			return &storiface.Decl{
				SectorID:       sectorID,
				SectorFileType: storiface.FTUnsealed,
			}
		},
		sus: func(sectorID abi.SectorID) map[abi.SectorID]db.SealState {
			return nil
			//return map[abi.SectorID]db.SealState{
			//sectorID: db.SealStateUnsealed,
			//}
		},
		expect: func(prov *mock_provider.MockInterfaceMockRecorder, prop market.DealProposal) {
			// Expect only one call to NotifyPut with fast retrieval = true (unsealed)
			// because the state of the sector doesn't change on the second call
			//prov.NotifyPut(gomock.Any(), gomock.Any(), gomock.Any(), metadata.Default.New(&metadata.GraphsyncFilecoinV1{
			//PieceCID:      prop.PieceCID,
			//VerifiedDeal:  prop.VerifiedDeal,
			//FastRetrieval: true,
			//})).Times(1)
		},
	}, {
		name: "unsealed -> removed",
		initialState: func(sectorID abi.SectorID) *storiface.Decl {
			return &storiface.Decl{
				SectorID:       sectorID,
				SectorFileType: storiface.FTUnsealed,
			}
		},
		sus: func(sectorID abi.SectorID) map[abi.SectorID]db.SealState {
			return map[abi.SectorID]db.SealState{
				sectorID: db.SealStateRemoved,
			}
		},
		expect: func(prov *mock_provider.MockInterfaceMockRecorder, prop market.DealProposal) {
			// Expect a call to NotifyPut with fast retrieval = true (unsealed)
			//prov.NotifyPut(gomock.Any(), gomock.Any(), gomock.Any(), metadata.Default.New(&metadata.GraphsyncFilecoinV1{
			//PieceCID:      prop.PieceCID,
			//VerifiedDeal:  prop.VerifiedDeal,
			//FastRetrieval: true,
			//})).Times(1)

			// Expect a call to NotifyRemove because the sector is no longer in the list response
			prov.NotifyRemove(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
		},
	}, {
		name: "sealed -> removed",
		initialState: func(sectorID abi.SectorID) *storiface.Decl {
			return &storiface.Decl{
				SectorID:       sectorID,
				SectorFileType: storiface.FTSealed,
			}
		},
		sus: func(sectorID abi.SectorID) map[abi.SectorID]db.SealState {
			return map[abi.SectorID]db.SealState{
				sectorID: db.SealStateRemoved,
			}
		},
		expect: func(prov *mock_provider.MockInterfaceMockRecorder, prop market.DealProposal) {
			// Expect a call to NotifyPut with fast retrieval = false (sealed)
			//prov.NotifyPut(gomock.Any(), gomock.Any(), gomock.Any(), metadata.Default.New(&metadata.GraphsyncFilecoinV1{
			//PieceCID:      prop.PieceCID,
			//VerifiedDeal:  prop.VerifiedDeal,
			//FastRetrieval: false,
			//})).Times(1)

			// Expect a call to NotifyRemove because the sector is no longer in the list response
			prov.NotifyRemove(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
		},
	}, {
		name: "removed -> unsealed",
		initialState: func(sectorID abi.SectorID) *storiface.Decl {
			return nil
		},
		sus: func(sectorID abi.SectorID) map[abi.SectorID]db.SealState {
			return map[abi.SectorID]db.SealState{
				sectorID: db.SealStateUnsealed,
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
	}, {
		name: "unsealed -> cache",
		initialState: func(sectorID abi.SectorID) *storiface.Decl {
			return &storiface.Decl{
				SectorID:       sectorID,
				SectorFileType: storiface.FTUnsealed,
			}
		},
		sus: func(sectorID abi.SectorID) map[abi.SectorID]db.SealState {
			return map[abi.SectorID]db.SealState{
				sectorID: db.SealStateCache,
			}
		},
		expect: func(prov *mock_provider.MockInterfaceMockRecorder, prop market.DealProposal) {
			// Expect only one call to NotifyPut with fast retrieval = true (unsealed)
			// because we ignore a state change to cache
			//prov.NotifyPut(gomock.Any(), gomock.Any(), gomock.Any(), metadata.Default.New(&metadata.GraphsyncFilecoinV1{
			//PieceCID:      prop.PieceCID,
			//VerifiedDeal:  prop.VerifiedDeal,
			//FastRetrieval: true,
			//})).Times(1)
		},
	}, {
		name: "cache -> unsealed",
		initialState: func(sectorID abi.SectorID) *storiface.Decl {
			return &storiface.Decl{
				SectorID:       sectorID,
				SectorFileType: storiface.FTCache,
			}
		},
		sus: func(sectorID abi.SectorID) map[abi.SectorID]db.SealState {
			return map[abi.SectorID]db.SealState{
				sectorID: db.SealStateUnsealed,
			}
		},
		expect: func(prov *mock_provider.MockInterfaceMockRecorder, prop market.DealProposal) {
			// Expect only one call to NotifyPut with fast retrieval = true (unsealed)
			// because we ignore a state change to cache
			prov.NotifyPut(gomock.Any(), gomock.Any(), gomock.Any(), metadata.Default.New(&metadata.GraphsyncFilecoinV1{
				PieceCID:      prop.PieceCID,
				VerifiedDeal:  prop.VerifiedDeal,
				FastRetrieval: true,
			})).Times(1)
		},
	}, {
		name: "cache -> sealed",
		initialState: func(sectorID abi.SectorID) *storiface.Decl {
			return &storiface.Decl{
				SectorID:       sectorID,
				SectorFileType: storiface.FTCache,
			}
		},
		sus: func(sectorID abi.SectorID) map[abi.SectorID]db.SealState {
			return map[abi.SectorID]db.SealState{
				sectorID: db.SealStateSealed,
			}
		},
		expect: func(prov *mock_provider.MockInterfaceMockRecorder, prop market.DealProposal) {
			// Expect only one call to NotifyPut with fast retrieval = true (unsealed)
			// because we ignore a state change to cache
			prov.NotifyPut(gomock.Any(), gomock.Any(), gomock.Any(), metadata.Default.New(&metadata.GraphsyncFilecoinV1{
				PieceCID:      prop.PieceCID,
				VerifiedDeal:  prop.VerifiedDeal,
				FastRetrieval: false,
			})).Times(1)
		},
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wrapper, legacyStorageProvider, storageMiner, prov := setup(t)
			legacyStorageProvider.EXPECT().ListLocalDeals().AnyTimes().Return(nil, nil)

			// Add a deal to the database
			deals, err := db.GenerateNDeals(1)
			require.NoError(t, err)
			err = wrapper.dealsDB.Insert(ctx, &deals[0])
			require.NoError(t, err)

			// Set up expectations (automatically verified when the test exits)
			prop := deals[0].ClientDealProposal.Proposal
			tc.expect(prov.EXPECT(), prop)

			minerID, err := address.IDFromAddress(deals[0].ClientDealProposal.Proposal.Provider)
			require.NoError(t, err)

			// Set the current state from db -- response from MinerAPI.StorageList()
			resp1 := tc.initialState(abi.SectorID{Miner: abi.ActorID(minerID), Number: deals[0].SectorID})
			storageMiner.storageList = map[storiface.ID][]storiface.Decl{}
			if resp1 != nil {
				storageMiner.storageList["uuid"] = []storiface.Decl{*resp1}
			}

			// Handle updates
			err = wrapper.handleUpdates(ctx, tc.sus(abi.SectorID{Miner: abi.ActorID(minerID), Number: deals[0].SectorID}))
			require.NoError(t, err)
		})
	}
}

// Verify that multiple storage file types are handled from StorageList correctly
func TestUnsealedStateManagerStorageList(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name         string
		initialState func(sectorID abi.SectorID) []storiface.Decl
		sus          func(sectorID abi.SectorID) map[abi.SectorID]db.SealState
		expect       func(*mock_provider.MockInterfaceMockRecorder, market.DealProposal)
	}{
		{
			name: "unsealed and sealed status",
			sus: func(sectorID abi.SectorID) map[abi.SectorID]db.SealState {
				return map[abi.SectorID]db.SealState{
					sectorID: db.SealStateUnsealed,
					sectorID: db.SealStateCache,
				}
			},
			initialState: func(sectorID abi.SectorID) []storiface.Decl {
				return []storiface.Decl{
					{
						SectorID:       sectorID,
						SectorFileType: storiface.FTUnsealed,
					},
					{
						SectorID:       sectorID,
						SectorFileType: storiface.FTSealed,
					},
				}
			},
			expect: func(prov *mock_provider.MockInterfaceMockRecorder, prop market.DealProposal) {
				// Expect only one call to NotifyPut with fast retrieval = true (unsealed)
				// because we ignore a state change to cache
				//prov.NotifyPut(gomock.Any(), gomock.Any(), gomock.Any(), metadata.Default.New(&metadata.GraphsyncFilecoinV1{
				//PieceCID:      prop.PieceCID,
				//VerifiedDeal:  prop.VerifiedDeal,
				//FastRetrieval: true,
				//})).Times(1)
			},
		}, {
			name: "unsealed and cached status",
			sus: func(sectorID abi.SectorID) map[abi.SectorID]db.SealState {
				return map[abi.SectorID]db.SealState{
					sectorID: db.SealStateUnsealed,
					sectorID: db.SealStateCache,
				}
			},
			initialState: func(sectorID abi.SectorID) []storiface.Decl {
				return []storiface.Decl{
					{
						SectorID:       sectorID,
						SectorFileType: storiface.FTUnsealed,
					},
					{
						SectorID:       sectorID,
						SectorFileType: storiface.FTCache,
					},
				}
			},
			expect: func(prov *mock_provider.MockInterfaceMockRecorder, prop market.DealProposal) {
				// Expect only one call to NotifyPut with fast retrieval = true (unsealed)
				// because we ignore a state change to cache
				//prov.NotifyPut(gomock.Any(), gomock.Any(), gomock.Any(), metadata.Default.New(&metadata.GraphsyncFilecoinV1{
				//PieceCID:      prop.PieceCID,
				//VerifiedDeal:  prop.VerifiedDeal,
				//FastRetrieval: true,
				//})).Times(1)
			},
		}, {
			name: "sealed and cached status",
			sus: func(sectorID abi.SectorID) map[abi.SectorID]db.SealState {
				return map[abi.SectorID]db.SealState{
					sectorID: db.SealStateUnsealed,
					sectorID: db.SealStateCache,
				}
			},
			initialState: func(sectorID abi.SectorID) []storiface.Decl {
				return []storiface.Decl{
					{
						SectorID:       sectorID,
						SectorFileType: storiface.FTSealed,
					},
					{
						SectorID:       sectorID,
						SectorFileType: storiface.FTCache,
					},
				}
			},
			expect: func(prov *mock_provider.MockInterfaceMockRecorder, prop market.DealProposal) {
				// Expect only one call to NotifyPut with fast retrieval = true (unsealed)
				// because we ignore a state change to cache
				//prov.NotifyPut(gomock.Any(), gomock.Any(), gomock.Any(), metadata.Default.New(&metadata.GraphsyncFilecoinV1{
				//PieceCID:      prop.PieceCID,
				//VerifiedDeal:  prop.VerifiedDeal,
				//FastRetrieval: false,
				//})).Times(1)
			},
		}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wrapper, legacyStorageProvider, storageMiner, prov := setup(t)
			legacyStorageProvider.EXPECT().ListLocalDeals().AnyTimes().Return(nil, nil)

			// Add a deal to the database
			deals, err := db.GenerateNDeals(1)
			require.NoError(t, err)
			err = wrapper.dealsDB.Insert(ctx, &deals[0])
			require.NoError(t, err)

			// Set up expectations (automatically verified when the test exits)
			prop := deals[0].ClientDealProposal.Proposal
			tc.expect(prov.EXPECT(), prop)

			minerID, err := address.IDFromAddress(deals[0].ClientDealProposal.Proposal.Provider)
			require.NoError(t, err)

			// Set the first response from MinerAPI.StorageList()
			storageMiner.storageList = map[storiface.ID][]storiface.Decl{}
			resp1 := tc.initialState(abi.SectorID{Miner: abi.ActorID(minerID), Number: deals[0].SectorID})
			storageMiner.storageList["uuid"] = resp1

			// Trigger check for updates
			err = wrapper.handleUpdates(ctx, tc.sus(abi.SectorID{Miner: abi.ActorID(minerID), Number: deals[0].SectorID}))
			require.NoError(t, err)
		})
	}
}

func setup(t *testing.T) (*Wrapper, *mock.MockStorageProvider, *mockApiStorageMiner, *mock_provider.MockInterface) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	prov := mock_provider.NewMockInterface(ctrl)

	sqldb := db.CreateTestTmpDB(t)
	require.NoError(t, db.CreateAllBoostTables(ctx, sqldb, sqldb))
	require.NoError(t, migrations.Migrate(sqldb))

	dealsDB := db.NewDealsDB(sqldb)
	storageMiner := &mockApiStorageMiner{}
	storageProvider := mock.NewMockStorageProvider(ctrl)

	wrapper := &Wrapper{
		enabled:     true,
		dealsDB:     dealsDB,
		prov:        prov,
		legacyProv:  storageProvider,
		meshCreator: &meshCreatorStub{},
	}

	return wrapper, storageProvider, storageMiner, prov
}

type mockApiStorageMiner struct {
	storageList map[storiface.ID][]storiface.Decl
}

func (m mockApiStorageMiner) StorageList(ctx context.Context) (map[storiface.ID][]storiface.Decl, error) {
	return m.storageList, nil
}

func (m mockApiStorageMiner) StorageRedeclareLocal(ctx context.Context, id *storiface.ID, dropMissing bool) error {
	return nil
}

type meshCreatorStub struct {
}

var _ idxprov.MeshCreator = (*meshCreatorStub)(nil)

func (m *meshCreatorStub) Connect(context.Context) error {
	return nil
}
