package httptransport

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"golang.org/x/xerrors"
)

// ErrTokenNotFound is returned when an auth token is not found in the database
var ErrTokenNotFound = errors.New("auth token not found")

// AuthTokenDB keeps a database of auth tokens with associated data
type AuthTokenDB struct {
	ds datastore.Batching
}

func NewAuthTokenDB(ds datastore.Batching) *AuthTokenDB {
	return &AuthTokenDB{
		ds: namespace.Wrap(ds, datastore.NewKey("/auth-token")),
	}
}

// Put adds some data to the DB and returns an auth token
func (db *AuthTokenDB) Put(ctx context.Context, data []byte) (authtok string, e error) {
	// Create a new auth token and add it to the datastore
	authTokenBuff := make([]byte, 256)
	if _, err := rand.Read(authTokenBuff); err != nil {
		return "", fmt.Errorf("generating auth token: %w", err)
	}
	authToken := hex.EncodeToString(authTokenBuff)

	authTokenKey := datastore.NewKey(authToken)
	err := db.ds.Put(ctx, authTokenKey, data)
	if err != nil {
		return "", fmt.Errorf("adding auth token to datastore: %w", err)
	}

	return authToken, nil
}

// Get data by auth token
func (db *AuthTokenDB) Get(ctx context.Context, authToken string) ([]byte, error) {
	data, err := db.ds.Get(ctx, datastore.NewKey(authToken))
	if err != nil {
		if xerrors.Is(err, datastore.ErrNotFound) {
			return nil, ErrTokenNotFound
		}
		return nil, fmt.Errorf("getting auth token from datastore: %w", err)
	}

	return data, nil
}

// Delete data by auth token
func (db *AuthTokenDB) Delete(ctx context.Context, authToken string) error {
	return db.ds.Delete(ctx, datastore.NewKey(authToken))
}
