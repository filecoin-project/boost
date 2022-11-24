package ldb

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/filecoin-project/boost/tracing"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	levelds "github.com/ipfs/go-ds-leveldb"
	carindex "github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multihash"
	"github.com/syndtr/goleveldb/leveldb/opt"
	ldbopts "github.com/syndtr/goleveldb/leveldb/opt"
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
)

func init() {
	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(buf, keyNextCursor)
	dskeyNextCursor = datastore.NewKey(string(buf))

	buf = make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(buf, prefixPieceCidToCursor)
	sprefixPieceCidToCursor = string(buf)

	buf = make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(buf, prefixMhtoPieceCids)
	sprefixMhtoPieceCids = string(buf)
}

type DB struct {
	datastore.Batching
}

func newDB(path string, readonly bool) (*DB, error) {
	ldb, err := levelds.NewDatastore(path, &levelds.Options{
		Compression:         ldbopts.SnappyCompression,
		NoSync:              true,
		Strict:              ldbopts.StrictAll,
		ReadOnly:            readonly,
		CompactionTableSize: 4 * opt.MiB,
	})
	if err != nil {
		return nil, fmt.Errorf("creating level db datstore: %w", err)
	}

	return &DB{ldb}, nil
}

func (db *DB) InitCursor(ctx context.Context) error {
	_, err := db.Get(ctx, dskeyNextCursor)
	if err == nil {
		// Cursor has already been initialized, so just return
		log.Debug("leveldb cursor already initialized")
		return nil
	}

	if errors.Is(err, ds.ErrNotFound) {
		// Cursor has not yet been initialized so initialize it
		log.Debug("initializing leveldb cursor")
		err = db.SetNextCursor(ctx, 100)
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("initializing database cursor: %w", err)
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
	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(buf, cursor)

	return db.Put(ctx, dskeyNextCursor, buf)
}

// GetPieceCidsByMultihash
func (db *DB) GetPieceCidsByMultihash(ctx context.Context, mh multihash.Multihash) ([]cid.Cid, error) {
	ctx, span := tracing.Tracer.Start(ctx, "db.get_piece_cids_by_multihash")
	defer span.End()

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

// SetMultihashToPieceCid
func (db *DB) SetMultihashesToPieceCid(ctx context.Context, recs []carindex.Record, pieceCid cid.Cid) error {
	ctx, span := tracing.Tracer.Start(ctx, "db.set_multihashes_to_piece_cid")
	defer span.End()

	batch, err := db.Batch(ctx)
	if err != nil {
		return fmt.Errorf("failed to create ds batch: %w", err)
	}

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

				if err := batch.Put(ctx, key, b); err != nil {
					return fmt.Errorf("failed to batch put mh=%s, err=%w", mh, err)
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
			if err := batch.Put(ctx, key, b); err != nil {
				return fmt.Errorf("failed to batch put mh=%s, err%w", mh, err)
			}

			return nil
		}()
		if err != nil {
			return err
		}
	}

	if err := batch.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}

	key := datastore.NewKey(sprefixMhtoPieceCids)
	if err := db.Sync(ctx, key); err != nil {
		return fmt.Errorf("failed to sync puts: %w", err)
	}

	return nil
}

// SetPieceCidToMetadata
func (db *DB) SetPieceCidToMetadata(ctx context.Context, pieceCid cid.Cid, md LeveldbMetadata) error {
	ctx, span := tracing.Tracer.Start(ctx, "db.set_piece_cid_to_metadata")
	defer span.End()

	b, err := json.Marshal(md)
	if err != nil {
		return err
	}

	key := datastore.NewKey(fmt.Sprintf("%s/%s", sprefixPieceCidToCursor, pieceCid.String()))

	return db.Put(ctx, key, b)
}

// GetPieceCidToMetadata
func (db *DB) GetPieceCidToMetadata(ctx context.Context, pieceCid cid.Cid) (LeveldbMetadata, error) {
	ctx, span := tracing.Tracer.Start(ctx, "db.get_piece_cid_to_metadata")
	defer span.End()

	var metadata LeveldbMetadata

	key := datastore.NewKey(fmt.Sprintf("%s/%s", sprefixPieceCidToCursor, pieceCid.String()))

	b, err := db.Get(ctx, key)
	if err != nil {
		return metadata, fmt.Errorf("getting piece metadata for piece %s: %w", pieceCid, err)
	}

	err = json.Unmarshal(b, &metadata)
	if err != nil {
		return metadata, fmt.Errorf("unmarshaling piece metadata for piece %s: %w", pieceCid, err)
	}

	return metadata, nil
}

func (db *DB) SetCarSize(ctx context.Context, pieceCid cid.Cid, size uint64) error {
	ctx, span := tracing.Tracer.Start(ctx, "db.set_car_size")
	defer span.End()

	md, err := db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		return fmt.Errorf("getting piece metadata for piece %s: %w", pieceCid, err)
	}

	// Set the car size on each deal (should be the same for all deals)
	for _, dl := range md.Deals {
		dl.CarLength = size
	}

	return db.SetPieceCidToMetadata(ctx, pieceCid, md)
}

func (db *DB) MarkIndexErrored(ctx context.Context, pieceCid cid.Cid, err error) error {
	ctx, span := tracing.Tracer.Start(ctx, "db.mark_piece_index_errored")
	defer span.End()

	md, err := db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		return fmt.Errorf("getting piece metadata for piece %s: %w", pieceCid, err)
	}

	if md.Error != "" {
		// If the error state has already been set, don't over-write the existing error
		return nil
	}

	md.Error = err.Error()
	md.ErrorType = fmt.Sprintf("%T", err)

	return db.SetPieceCidToMetadata(ctx, pieceCid, md)
}

// AllRecords
func (db *DB) AllRecords(ctx context.Context, cursor uint64) ([]model.Record, error) {
	ctx, span := tracing.Tracer.Start(ctx, "db.all_records")
	defer span.End()

	var records []model.Record

	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(buf, cursor)

	var q query.Query
	q.Prefix = fmt.Sprintf("%d/", cursor)
	results, err := db.Query(ctx, q)
	if err != nil {
		return nil, err
	}

	for {
		r, ok := results.NextSync()
		if !ok {
			break
		}

		k := r.Key[len(q.Prefix)+1:]

		m, err := multihash.FromHexString(k)
		if err != nil {
			return nil, err
		}

		kcid := cid.NewCidV1(cid.Raw, m)

		offset, n := binary.Uvarint(r.Value)
		size, n := binary.Uvarint(r.Value[n:])

		records = append(records, model.Record{
			Cid: kcid,
			OffsetSize: model.OffsetSize{
				Offset: offset,
				Size:   size,
			},
		})
	}

	return records, nil
}

// AddIndexRecord
func (db *DB) AddIndexRecord(ctx context.Context, cursorPrefix string, rec model.Record) error {
	ctx, span := tracing.Tracer.Start(ctx, "db.add_index_record")
	defer span.End()

	key := datastore.NewKey(fmt.Sprintf("%s%s", cursorPrefix, rec.Cid.Hash().String()))

	value := make([]byte, 2*binary.MaxVarintLen64)
	no := binary.PutUvarint(value, rec.Offset)
	ns := binary.PutUvarint(value[no:], rec.Size)

	return db.Put(ctx, key, value[:no+ns])
}

// GetOffsetSize
func (db *DB) GetOffsetSize(ctx context.Context, cursorPrefix string, m multihash.Multihash) (*model.OffsetSize, error) {
	ctx, span := tracing.Tracer.Start(ctx, "db.get_offset")
	defer span.End()

	key := datastore.NewKey(fmt.Sprintf("%s%s", cursorPrefix, m.String()))

	b, err := db.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	offset, n := binary.Uvarint(b)
	size, n := binary.Uvarint(b[n:])
	return &model.OffsetSize{
		Offset: offset,
		Size:   size,
	}, nil
}

func (db *DB) ListPieces(ctx context.Context) ([]cid.Cid, error) {
	ctx, span := tracing.Tracer.Start(ctx, "db.list_pieces")
	defer span.End()

	q := query.Query{
		Prefix:   "/" + sprefixPieceCidToCursor + "/",
		KeysOnly: true,
	}
	results, err := db.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("listing pieces in database: %w", err)
	}

	var pieceCids []cid.Cid
	for {
		r, ok := results.NextSync()
		if !ok {
			break
		}

		k := r.Key[len(q.Prefix):]
		pieceCid, err := cid.Parse(k)
		if err != nil {
			return nil, fmt.Errorf("parsing piece cid '%s': %w", k, err)
		}

		pieceCids = append(pieceCids, pieceCid)
	}

	return pieceCids, nil
}

func has(list []cid.Cid, v cid.Cid) bool {
	for _, l := range list {
		if l.Equals(v) {
			return true
		}
	}
	return false
}
