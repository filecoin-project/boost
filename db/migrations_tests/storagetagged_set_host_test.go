package migrations_tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/db/migrations"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
)

func TestStorageTaggedSetHost(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	sqldb := db.CreateTestTmpDB(t)
	req.NoError(db.CreateAllBoostTables(ctx, sqldb, sqldb))

	// Run migrations up to the one that adds the TransferHost field to StorageTagged
	goose.SetBaseFS(migrations.EmbedMigrations)
	req.NoError(goose.SetDialect("sqlite3"))
	req.NoError(goose.UpTo(sqldb, ".", 20220908122510))

	deals, err := db.GenerateNDeals(2)
	req.NoError(err)

	// Set the transfer params such that each deal has a different host
	getHost := func(i int) string {
		return fmt.Sprintf("files.org:%d", 1000+i)
	}
	for i, deal := range deals {
		deal.Transfer = types.Transfer{
			Type:   "http",
			Params: []byte(fmt.Sprintf(`{"url":"http://%s/file.car"}`, getHost(i))),
			Size:   uint64(1024),
		}
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
	}

	// Simulate tagging a deal
	taggedStorageDB := db.NewStorageDB(sqldb)
	err = taggedStorageDB.Tag(ctx, deals[0].DealUuid, 1024, "")
	req.NoError(err)

	// Run the migration that reads the deal transfer params and sets
	// StorageTagged.TransferHost
	req.NoError(goose.UpByOne(sqldb, "."))

	// Check that after migrating up, the host is set correctly
	rows, err := sqldb.QueryContext(ctx, "SELECT TransferHost FROM StorageTagged")
	req.NoError(err)
	defer rows.Close() //nolint:errcheck

	rowIdx := 0
	for ; rows.Next(); rowIdx++ {
		var host string
		err := rows.Scan(&host)
		req.NoError(err)
		req.Equal(getHost(0), host)
	}

	// Even though there are two deals in DB, there is only one deal that is
	// tagged, so there should only be one row
	req.Equal(1, rowIdx)
}
