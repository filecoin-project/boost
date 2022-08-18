package couchbase

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/filecoin-project/boost/cmd/boostd-data/model"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	ds "github.com/ipfs/go-datastore"
	carindex "github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multihash"
)

var (
	// LevelDB key value for storing next free cursor.
	keyNextCursor   uint64 = 0
	dskeyNextCursor datastore.Key

	// LevelDB key prefix for PieceCid to cursor table.
	// LevelDB keys will be built by concatenating PieceCid to this prefix.
	prefixPieceCidToCursor  uint64 = 1
	sprefixPieceCidToCursor string

	// LevelDB key prefix for Multihash to PieceCids table.
	// LevelDB keys will be built by concatenating Multihash to this prefix.
	prefixMhtoPieceCids  uint64 = 2
	sprefixMhtoPieceCids string

	size = binary.MaxVarintLen64
)

func init() {
	buf := make([]byte, size)
	binary.PutUvarint(buf, keyNextCursor)
	dskeyNextCursor = datastore.NewKey(string(buf))

	buf = make([]byte, size)
	binary.PutUvarint(buf, prefixPieceCidToCursor)
	sprefixPieceCidToCursor = string(buf)

	buf = make([]byte, size)
	binary.PutUvarint(buf, prefixMhtoPieceCids)
	sprefixMhtoPieceCids = string(buf)
}

type DB struct {
	col *gocb.Collection
}

func newDB() (*DB, error) {
	bucketName := "piecestore"
	username := "Administrator"
	password := "boostdemo"

	cluster, err := gocb.Connect("couchbase://127.0.0.1", gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
	})
	if err != nil {
		return nil, err
	}

	bucket := cluster.Bucket(bucketName)

	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		return nil, err
	}

	return &DB{col: bucket.DefaultCollection()}, nil
}

// NextCursor
func (db *DB) NextCursor(ctx context.Context) (uint64, string, error) {
	b, err := db.Get(ctx, dskeyNextCursor)
	if err != nil {
		return 0, "", err
	}

	cursor, _ := binary.Uvarint(b)
	return cursor, fmt.Sprintf("%d", cursor) + "/", nil // adding "/" because of Query method in go-datastore
}

// SetNextCursor
func (db *DB) SetNextCursor(ctx context.Context, cursor uint64) error {
	buf := make([]byte, size)
	binary.PutUvarint(buf, cursor)

	return db.Put(ctx, dskeyNextCursor, buf)
}

// GetPieceCidsByMultihash
func (db *DB) GetPieceCidsByMultihash(ctx context.Context, mh multihash.Multihash) ([]cid.Cid, error) {
	key := datastore.NewKey(fmt.Sprintf("%s%s", sprefixMhtoPieceCids, mh.String()))

	val, err := db.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get value for multihash %s, err: %w", mh, err)
	}

	var pcids []cid.Cid
	if err := json.Unmarshal(val, &pcids); err != nil {
		return nil, fmt.Errorf("failed to unmarshal pieceCids slice: %w", err)
	}

	return pcids, nil
}

func (db *DB) Get(ctx context.Context, key datastore.Key) (value []byte, err error) {
	var getResult *gocb.GetResult
	getResult, err = db.col.Get("u:"+key.String(), nil)
	if err != nil {
		return nil, err
	}

	//cas := getResult.Cas()

	var val []byte
	err = getResult.Content(&val)
	if err != nil {
		return nil, err
	}

	return val, nil
}

func (db *DB) Put(ctx context.Context, key datastore.Key, value []byte) error {
	_, err := db.col.Upsert("u:"+key.String(), value, nil)

	return err
}

// SetMultihashToPieceCid
func (db *DB) SetMultihashesToPieceCid(ctx context.Context, recs []carindex.Record, pieceCid cid.Cid) error {
	for _, r := range recs {
		mh := r.Cid.Hash()

		err := func() error {
			key := datastore.NewKey(fmt.Sprintf("%s%s", sprefixMhtoPieceCids, mh.String()))

			// do we already have an entry for this multihash ?
			val, err := db.Get(ctx, key)
			if err != nil && err != ds.ErrNotFound {
				return fmt.Errorf("failed to get value for multihash %s, err: %w", mh, err)
			}

			// if we don't have an existing entry for this mh, create one
			if err == ds.ErrNotFound {
				v := []cid.Cid{pieceCid}
				b, err := json.Marshal(v)
				if err != nil {
					return fmt.Errorf("failed to marshal pieceCids slice: %w", err)
				}

				if err := db.Put(ctx, key, b); err != nil {
					return fmt.Errorf("failed to put mh=%s, err=%w", mh, err)
				}
				return nil
			}

			// else, append the pieceCid to the existing list
			var pcids []cid.Cid
			if err := json.Unmarshal(val, &pcids); err != nil {
				return fmt.Errorf("failed to unmarshal pieceCids slice: %w", err)
			}

			// if we already have the pieceCid indexed for the multihash, nothing to do here.
			if has(pcids, pieceCid) {
				return nil
			}

			pcids = append(pcids, pieceCid)

			b, err := json.Marshal(pcids)
			if err != nil {
				return fmt.Errorf("failed to marshal pieceCids slice: %w", err)
			}
			if err := db.Put(ctx, key, b); err != nil {
				return fmt.Errorf("failed to put mh=%s, err%w", mh, err)
			}

			return nil
		}()
		if err != nil {
			return err
		}
	}

	return nil
}

// SetPieceCidToMetadata
func (db *DB) SetPieceCidToMetadata(ctx context.Context, pieceCid cid.Cid, md model.Metadata) error {
	b, err := json.Marshal(md)
	if err != nil {
		return err
	}

	key := datastore.NewKey(fmt.Sprintf("%s%s", sprefixPieceCidToCursor, pieceCid.String()))

	return db.Put(ctx, key, b)
}

// GetPieceCidToMetadata
func (db *DB) GetPieceCidToMetadata(ctx context.Context, pieceCid cid.Cid) (model.Metadata, error) {
	var metadata model.Metadata

	key := datastore.NewKey(fmt.Sprintf("%s%s", sprefixPieceCidToCursor, pieceCid.String()))

	b, err := db.Get(ctx, key)
	if err != nil {
		return metadata, err
	}

	err = json.Unmarshal(b, &metadata)
	if err != nil {
		return metadata, err
	}

	return metadata, nil
}

// AllRecords
func (db *DB) AllRecords(ctx context.Context, cursor uint64) ([]model.Record, error) {
	return nil, errors.New("not impl")
	//var records []model.Record

	//buf := make([]byte, size)
	//binary.PutUvarint(buf, cursor)

	//var q query.Query
	//q.Prefix = fmt.Sprintf("%d/", cursor)
	//results, err := db.Query(ctx, q)
	//if err != nil {
	//return nil, err
	//}

	//for {
	//r, ok := results.NextSync()
	//if !ok {
	//break
	//}

	//k := r.Key[len(q.Prefix)+1:]

	//m, err := multihash.FromHexString(k)
	//if err != nil {
	//return nil, err
	//}

	//kcid := cid.NewCidV1(cid.Raw, m)

	//offset, _ := binary.Uvarint(r.Value)

	//records = append(records, model.Record{
	//Cid:    kcid,
	//Offset: offset,
	//})
	//}

	//return records, nil
}

// AddOffset
func (db *DB) AddOffset(ctx context.Context, cursorPrefix string, m multihash.Multihash, offset uint64) error {
	key := datastore.NewKey(fmt.Sprintf("%s%s", cursorPrefix, m.String()))

	value := make([]byte, size)
	binary.PutUvarint(value, offset)

	return db.Put(ctx, key, value)
}

// GetOffset
func (db *DB) GetOffset(ctx context.Context, cursorPrefix string, m multihash.Multihash) (uint64, error) {
	key := datastore.NewKey(fmt.Sprintf("%s%s", cursorPrefix, m.String()))

	b, err := db.Get(ctx, key)
	if err != nil {
		return 0, err
	}

	offset, _ := binary.Uvarint(b)
	return offset, nil
}

func has(list []cid.Cid, v cid.Cid) bool {
	for _, l := range list {
		if l.Equals(v) {
			return true
		}
	}
	return false
}
