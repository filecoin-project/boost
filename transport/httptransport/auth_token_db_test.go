package httptransport

import (
	"context"
	"testing"

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
	data := []byte("data")
	authToken, err := db.Put(ctx, data)
	rqr.NoError(err)

	// Get the data back
	got, err := db.Get(ctx, authToken)
	rqr.NoError(err)
	rqr.Equal(data, got)

	// Delete the data by auth token
	err = db.Delete(ctx, authToken)
	rqr.NoError(err)

	// Get by auth token should now return ErrTokenNotFound
	_, err = db.Get(ctx, authToken)
	rqr.Error(err)
	rqr.ErrorIs(err, ErrTokenNotFound)
}
