package httptransport

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/stretchr/testify/require"
)

func TestNewAuthTokenDB(t *testing.T) {
	ctx := context.Background()
	rqr := require.New(t)

	// Create auth DB
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	db := NewAuthTokenDB(ds)

	// Expect ErrTokenNotFound when getting a non-existent token
	_, err := db.Get(ctx, "doesnt-exist")
	rqr.Error(err)
	rqr.ErrorIs(err, ErrTokenNotFound)

	// Put some data into the DB
	authToken, err := GenerateAuthToken()
	rqr.NoError(err)
	proposalCid, err := cid.Parse("bafkqaaa")
	rqr.NoError(err)
	payloadCid, err := cid.Parse("bafkqaab")
	rqr.NoError(err)
	val := AuthValue{
		ID:          "12345",
		ProposalCid: proposalCid,
		PayloadCid:  payloadCid,
		Size:        1234,
	}
	err = db.Put(ctx, authToken, val)
	rqr.NoError(err)

	// Get the data back
	got, err := db.Get(ctx, authToken)
	rqr.NoError(err)
	rqr.Equal(val, *got)

	checkpoint := time.Now()

	authToken2, err := GenerateAuthToken()
	rqr.NoError(err)
	val2 := AuthValue{
		ID:          "54321",
		ProposalCid: proposalCid,
		PayloadCid:  payloadCid,
		Size:        4321,
	}
	err = db.Put(ctx, authToken2, val2)
	rqr.NoError(err)

	// Delete by time such that only the first item should be removed
	expired, err := db.DeleteExpired(ctx, checkpoint)
	rqr.NoError(err)

	rqr.Len(expired, 1)
	rqr.Equal(val, expired[0])

	// Get by auth token should now return ErrTokenNotFound for first item
	_, err = db.Get(ctx, authToken)
	rqr.Error(err)
	rqr.ErrorIs(err, ErrTokenNotFound)

	// Delete the second item by id
	err = db.Delete(ctx, authToken2)
	rqr.NoError(err)

	// Get by auth token should now return ErrTokenNotFound for second item
	_, err = db.Get(ctx, authToken2)
	rqr.Error(err)
	rqr.ErrorIs(err, ErrTokenNotFound)
}
