package migrations_tests

import (
	"context"
	"testing"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/db/migrations"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
)

func TestDealFastRetrieval(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	sqldb := db.CreateTestTmpDB(t)
	req.NoError(db.CreateAllBoostTables(ctx, sqldb, sqldb))

	// Run migrations up to the one that adds the FastRetrieval field to Deals
	goose.SetBaseFS(migrations.EmbedMigrations)
	req.NoError(goose.SetDialect("sqlite3"))
	req.NoError(goose.UpTo(sqldb, ".", 20221124191002))

	// Generate 1 deal
	deals, err := db.GenerateNDeals(1)
	req.NoError(err)

	deal := deals[0]

	_, err = sqldb.Exec(`INSERT INTO Deals ("ID", "CreatedAt", "DealProposalSignature", "PieceCID", "PieceSize",
                   "VerifiedDeal", "IsOffline", "ClientAddress", "ProviderAddress","Label", "StartEpoch", "EndEpoch",
                   "StoragePricePerEpoch", "ProviderCollateral", "ClientCollateral", "ClientPeerID", "DealDataRoot",
                   "InboundFilePath", "TransferType", "TransferParams", "TransferSize", "ChainDealID", "PublishCID",
                   "SectorID", "Offset", "Length", "Checkpoint", "CheckpointAt", "Error", "Retry", "SignedProposalCID") 
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		deal.DealUuid, deal.CreatedAt, []byte("test"), deal.ClientDealProposal.Proposal.PieceCID.String(),
		deal.ClientDealProposal.Proposal.PieceSize, deal.ClientDealProposal.Proposal.VerifiedDeal, deal.IsOffline,
		deal.ClientDealProposal.Proposal.Client.String(), deal.ClientDealProposal.Proposal.Provider.String(), "test",
		deal.ClientDealProposal.Proposal.StartEpoch, deal.ClientDealProposal.Proposal.EndEpoch, deal.ClientDealProposal.Proposal.StoragePricePerEpoch.Uint64(),
		deal.ClientDealProposal.Proposal.ProviderCollateral.Int64(), deal.ClientDealProposal.Proposal.ClientCollateral.Uint64(), deal.ClientPeerID.String(),
		deal.DealDataRoot.String(), deal.InboundFilePath, deal.Transfer.Type, deal.Transfer.Params, deal.Transfer.Size, deal.ChainDealID,
		deal.PublishCID.String(), deal.SectorID, deal.Offset, deal.Length, deal.Checkpoint, deal.CheckpointAt, deal.Err, deal.Retry, []byte("test"))

	require.NoError(t, err)

	//Run migration
	req.NoError(goose.UpByOne(sqldb, "."))

	rows, err := sqldb.Query("SELECT FastRetrieval FROM Deals WHERE ID =?", deal.DealUuid)
	require.NoError(t, err)
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {

		var ft bool

		err = rows.Scan(&ft)
		require.NoError(t, err)
		require.True(t, ft)
	}
}
