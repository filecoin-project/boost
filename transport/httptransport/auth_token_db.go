package httptransport

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-datastore/query"
)

// ErrTokenNotFound is returned when an auth token is not found in the database
var ErrTokenNotFound = errors.New("auth token not found")

// AuthValue is the data associated with an auth token in the auth token DB
type AuthValue struct {
	ID          string
	ProposalCid cid.Cid
	PayloadCid  cid.Cid
	Size        uint64
}

type authValueTS struct {
	AuthValue
	CreatedAt time.Time
}

// AuthTokenDB keeps a database of auth tokens with associated data
type AuthTokenDB struct {
	ds datastore.Batching
}

func NewAuthTokenDB(ds datastore.Batching) *AuthTokenDB {
	return &AuthTokenDB{
		ds: namespace.Wrap(ds, datastore.NewKey("/auth-token")),
	}
}

// Put adds the auth values to the DB by auth token
func (db *AuthTokenDB) Put(ctx context.Context, authToken string, val AuthValue) error {
	avts := authValueTS{
		AuthValue: val,
		CreatedAt: time.Now(),
	}
	authValueJson, err := json.Marshal(avts)
	if err != nil {
		return fmt.Errorf("marshaling auth value JSON: %w", err)
	}

	authTokenKey := datastore.NewKey(authToken)
	err = db.ds.Put(ctx, authTokenKey, authValueJson)
	if err != nil {
		return fmt.Errorf("adding auth token to datastore: %w", err)
	}

	return nil
}

// Get data by auth token
func (db *AuthTokenDB) Get(ctx context.Context, authToken string) (*AuthValue, error) {
	data, err := db.ds.Get(ctx, datastore.NewKey(authToken))
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			return nil, ErrTokenNotFound
		}
		return nil, fmt.Errorf("getting auth token from datastore: %w", err)
	}

	var val authValueTS
	err = json.Unmarshal(data, &val)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling json from datastore: %w", err)
	}
	return &val.AuthValue, nil
}

// Delete auth token from the datastore
func (db *AuthTokenDB) Delete(ctx context.Context, authToken string) error {
	return db.ds.Delete(ctx, datastore.NewKey(authToken))
}

// Delete expired auth tokens and return the values for expired tokens
func (db *AuthTokenDB) DeleteExpired(ctx context.Context, before time.Time) ([]AuthValue, error) {
	// Query all items in the datastore
	qres, err := db.ds.Query(ctx, query.Query{})
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer qres.Close() //nolint:errcheck

	batch, err := db.ds.Batch(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating auth token db batch: %w", err)
	}

	// Select expired tokens
	expired := make([]AuthValue, 0)
	for r := range qres.Next() {
		var val authValueTS
		err = json.Unmarshal(r.Value, &val)
		if err != nil {
			return nil, fmt.Errorf("unmarshaling json from datastore: %w", err)
		}

		if val.CreatedAt.Before(before) {
			err := batch.Delete(ctx, datastore.NewKey(r.Key))
			if err != nil {
				return nil, fmt.Errorf("batch delete on expired auth token: %w", err)
			}
			expired = append(expired, val.AuthValue)
		}
	}

	err = batch.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("deleting batch of expired auth tokens: %w", err)
	}

	return expired, nil
}

func GenerateAuthToken() (string, error) {
	authTokenBuff := make([]byte, 256)
	if _, err := rand.Read(authTokenBuff); err != nil {
		return "", fmt.Errorf("generating auth token: %w", err)
	}
	return hex.EncodeToString(authTokenBuff), nil
}
