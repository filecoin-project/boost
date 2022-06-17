package db

import (
	"context"
	"testing"

	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/builtin/v8/market"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestProposalLogDB(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	sqldb := CreateTestTmpDB(t)
	require.NoError(t, CreateAllBoostTables(ctx, sqldb, sqldb))
	require.NoError(t, Migrate(sqldb))

	ldb := NewProposalLogsDB(sqldb)

	addr1, err := address.NewIDAddress(1)
	require.NoError(t, err)
	addr2, err := address.NewIDAddress(2)
	require.NoError(t, err)
	d1 := types.DealParams{
		DealUUID: uuid.New(),
		ClientDealProposal: market.ClientDealProposal{
			Proposal: market.DealProposal{
				PieceSize: 1111,
				Client:    addr1,
			},
		},
	}
	d2 := types.DealParams{
		DealUUID: uuid.New(),
		ClientDealProposal: market.ClientDealProposal{
			Proposal: market.DealProposal{
				PieceSize: 2222,
				Client:    addr1,
			},
		},
	}
	d3 := types.DealParams{
		DealUUID: uuid.New(),
		ClientDealProposal: market.ClientDealProposal{
			Proposal: market.DealProposal{
				PieceSize: 3333,
				Client:    addr2,
			},
		},
	}
	req.NoError(ldb.InsertLog(ctx, d1, true, ""))
	req.NoError(ldb.InsertLog(ctx, d2, false, "reason1"))
	req.NoError(ldb.InsertLog(ctx, d3, false, "reason2"))

	// Test list all
	logs, err := ldb.List(ctx, nil, nil, 0, 0)
	req.NoError(err)
	req.Len(logs, 3)
	req.Equal(d1.DealUUID, logs[2].DealUUID)
	req.Equal(true, logs[2].Accepted)
	req.Equal("", logs[2].Reason)
	req.Equal(d1.ClientDealProposal.Proposal.Client, logs[2].ClientAddress)
	req.Equal(d1.ClientDealProposal.Proposal.PieceSize, logs[2].PieceSize)
	req.Equal(d2.DealUUID, logs[1].DealUUID)
	req.Equal(false, logs[1].Accepted)
	req.Equal("reason1", logs[1].Reason)
	req.Equal(d3.DealUUID, logs[0].DealUUID)
	req.Equal(false, logs[0].Accepted)
	req.Equal("reason2", logs[0].Reason)

	// Test pagination
	logs, err = ldb.List(ctx, nil, &logs[1].CreatedAt, 1, 1)
	req.NoError(err)
	req.Len(logs, 1)
	req.Equal(d1.DealUUID, logs[0].DealUUID)

	// Test accepted filter
	accepted := true
	logs, err = ldb.List(ctx, &accepted, nil, 0, 0)
	req.NoError(err)
	req.Len(logs, 1)
	req.Equal(d1.DealUUID, logs[0].DealUUID)

	accepted = false
	logs, err = ldb.List(ctx, &accepted, nil, 0, 0)
	req.NoError(err)
	req.Len(logs, 2)
	req.Equal(d3.DealUUID, logs[0].DealUUID)
	req.Equal(d2.DealUUID, logs[1].DealUUID)

	// Test pagination with accepted filter
	logs, err = ldb.List(ctx, &accepted, nil, 1, 1)
	req.NoError(err)
	req.Len(logs, 1)
	req.Equal(d2.DealUUID, logs[0].DealUUID)

	// Test Count
	count, err := ldb.Count(ctx, nil)
	req.NoError(err)
	req.Equal(3, count)

	accepted = true
	count, err = ldb.Count(ctx, &accepted)
	req.NoError(err)
	req.Equal(1, count)

	accepted = false
	count, err = ldb.Count(ctx, &accepted)
	req.NoError(err)
	req.Equal(2, count)
}
