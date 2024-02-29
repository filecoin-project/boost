package pdcleaner

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"

	bdb "github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/db/migrations"
	"github.com/filecoin-project/boost/extern/boostd-data/client"
	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/boost/extern/boostd-data/svc"
	mocks_legacy "github.com/filecoin-project/boost/lib/legacy/mocks"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	"github.com/filecoin-project/lotus/api"
	lotusmocks "github.com/filecoin-project/lotus/api/mocks"
	test "github.com/filecoin-project/lotus/chain/events/state/mock"
	chaintypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/stretchr/testify/require"
)

func TestPieceDirectoryCleaner(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	sqldb := bdb.CreateTestTmpDB(t)
	require.NoError(t, bdb.CreateAllBoostTables(ctx, sqldb, sqldb))
	require.NoError(t, migrations.Migrate(sqldb))

	dealsDB := bdb.NewDealsDB(sqldb)
	directDB := bdb.NewDirectDealsDB(sqldb)

	bdsvc, err := svc.NewLevelDB("")
	require.NoError(t, err)
	ln, err := bdsvc.Start(ctx, "localhost:0")
	require.NoError(t, err)

	cl := client.NewStore()
	err = cl.Dial(ctx, fmt.Sprintf("ws://%s", ln))
	require.NoError(t, err)
	defer cl.Close(ctx)

	pieceCount := 5
	readers := make(map[abi.SectorNumber]car.SectionReader)
	for i := 0; i < pieceCount; i++ {
		// Create a random CAR file
		_, carFilePath := piecedirectory.CreateCarFile(t, i+1)
		carFile, err := os.Open(carFilePath)
		require.NoError(t, err)
		defer carFile.Close()

		carReader, err := car.OpenReader(carFilePath)
		require.NoError(t, err)
		defer carReader.Close()
		carv1Reader, err := carReader.DataReader()
		require.NoError(t, err)

		readers[abi.SectorNumber(i+1)] = carv1Reader
	}

	// Any calls to get a reader over data should return a reader over the random CAR file
	pr := piecedirectory.CreateMockPieceReaders(t, readers)

	pm := piecedirectory.NewPieceDirectory(cl, pr, 1)
	pm.Start(ctx)

	type dealData struct {
		sector      abi.SectorNumber
		chainDealID abi.DealID
		piece       cid.Cid
		used        bool
	}

	deals, err := bdb.GenerateDeals()
	req.NoError(err)

	// Create and update a map to keep track of chainDealID and UUID bindings
	dealMap := make(map[uuid.UUID]*dealData)
	for _, deal := range deals {
		dealMap[deal.DealUuid] = &dealData{chainDealID: deal.ChainDealID, used: false}
	}

	// Add deals to LID and note down details to update SQL DB
	for sectorNumber, reader := range readers {
		pieceCid := piecedirectory.CalculateCommp(t, reader).PieceCID

		var uid uuid.UUID
		var cdid abi.DealID

		for id, data := range dealMap {
			// If this value from deals list has not be used
			if !data.used {
				uid = id // Use the UUID from deals list
				cdid = data.chainDealID
				data.used = true
				data.sector = sectorNumber // Use the sector number from deals list
				data.piece = pieceCid
				break
			}
		}

		// Add deal info for each piece
		di := model.DealInfo{
			DealUuid:    uid.String(),
			ChainDealID: cdid,
			SectorID:    sectorNumber,
			PieceOffset: 0,
			PieceLength: 0,
		}
		err := pm.AddDealForPiece(ctx, pieceCid, di)
		require.NoError(t, err)
	}

	// Setup Full node, legacy manager
	ctrl := gomock.NewController(t)
	fn := lotusmocks.NewMockFullNode(ctrl)
	legacyProv := mocks_legacy.NewMockLegacyDealManager(ctrl)
	provAddr, err := address.NewIDAddress(1523)
	require.NoError(t, err)

	// Start a new PieceDirectoryCleaner
	pdc := newPDC(dealsDB, directDB, legacyProv, pm, fn)
	pdc.ctx = ctx

	chainHead, err := test.MockTipset(provAddr, 1)
	require.NoError(t, err)
	chainHeadFn := func(ctx context.Context) (*chaintypes.TipSet, error) {
		return chainHead, nil
	}

	// Add deals to SQL DB
	cDealMap := make(map[string]*api.MarketDeal)
	for i, deal := range deals {
		data, ok := dealMap[deal.DealUuid]
		require.True(t, ok)
		deal.SectorID = data.sector
		deal.ClientDealProposal.Proposal.PieceCID = data.piece
		deal.ClientDealProposal.Proposal.EndEpoch = 3 // because chain head is always 5
		deal.Checkpoint = dealcheckpoints.Complete
		p, err := deal.SignedProposalCid()
		require.NoError(t, err)
		t.Logf("signed p %s", p.String())
		// Test a slashed deal
		if i == 0 {
			deal.Checkpoint = dealcheckpoints.Accepted
			deal.ClientDealProposal.Proposal.EndEpoch = 6
			cDealMap[strconv.FormatInt(int64(deal.ChainDealID), 10)] = &api.MarketDeal{
				Proposal: deal.ClientDealProposal.Proposal,
				State: market.DealState{
					SlashEpoch: 3, // Slash this deal
				},
			}
			err = dealsDB.Insert(ctx, &deal)
			req.NoError(err)
			continue
		}
		cDealMap[strconv.FormatInt(int64(deal.ChainDealID), 10)] = &api.MarketDeal{
			Proposal: deal.ClientDealProposal.Proposal,
			State: market.DealState{
				SlashEpoch: -1,
			},
		}
		err = dealsDB.Insert(ctx, &deal)
		req.NoError(err)
	}

	// Confirm we have 5 pieces in LID
	pl, err := pm.ListPieces(ctx)
	require.NoError(t, err)
	require.Len(t, pl, 5)

	fn.EXPECT().ChainHead(gomock.Any()).DoAndReturn(chainHeadFn).AnyTimes()
	fn.EXPECT().StateMarketDeals(gomock.Any(), gomock.Any()).Return(cDealMap, nil).AnyTimes()
	fn.EXPECT().StateGetClaims(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
	legacyProv.EXPECT().ListDeals().Return(nil, nil).AnyTimes()
	legacyProv.EXPECT().ByPieceCid(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	err = pdc.CleanOnce()
	require.NoError(t, err)

	// Confirm we have 5 pieces in LID after clean up
	pl, err = pm.ListPieces(ctx)
	require.NoError(t, err)
	require.Len(t, pl, 0)
}
