package couchbase

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"strconv"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/ipfs/go-cid"
	carindex "github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multihash"
)

const bucketName = "piecestore"

var (
	// couchbase key prefix for PieceCid to metadata table.
	// couchbase keys will be built by concatenating prefix with PieceCid.
	prefixPieceCidToMetadata  uint64 = 1
	sprefixPieceCidToMetadata string

	// couchbase key prefix for Multihash to PieceCids table.
	// couchbase keys will be built by concatenating prefix with Multihash.
	prefixMhtoPieceCids  uint64 = 2
	sprefixMhtoPieceCids string

	// couchbase key prefix for PieceCid to offsets map.
	// couchbase keys will be built by concatenating prefix with PieceCid.
	prefixPieceCidToOffsets  uint64 = 3
	sprefixPieceCidToOffsets string

	maxVarIntSize = binary.MaxVarintLen64

	binaryTranscoder = gocb.NewRawBinaryTranscoder()
)

func init() {
	buf := make([]byte, maxVarIntSize)
	binary.PutUvarint(buf, prefixPieceCidToMetadata)
	sprefixPieceCidToMetadata = string(buf)

	buf = make([]byte, maxVarIntSize)
	binary.PutUvarint(buf, prefixMhtoPieceCids)
	sprefixMhtoPieceCids = string(buf)

	buf = make([]byte, maxVarIntSize)
	binary.PutUvarint(buf, prefixPieceCidToOffsets)
	sprefixPieceCidToOffsets = string(buf)
}

type DB struct {
	cluster *gocb.Cluster
	col     *gocb.Collection
}

func newDB(ctx context.Context) (*DB, error) {
	username := "Administrator"
	password := "boostdemo"

	connStr := "couchbase://127.0.0.1"
	cluster, err := gocb.Connect(connStr, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("connecting to couchbase DB %s: %w", connStr, err)
	}

	_, err = cluster.Buckets().GetBucket(bucketName, &gocb.GetBucketOptions{Context: ctx})
	if err != nil {
		if !errors.Is(err, gocb.ErrBucketNotFound) {
			return nil, fmt.Errorf("getting bucket %s: %w", bucketName, err)
		}

		err = cluster.Buckets().CreateBucket(gocb.CreateBucketSettings{
			BucketSettings: gocb.BucketSettings{
				Name:       bucketName,
				RAMQuotaMB: 256,
				BucketType: gocb.CouchbaseBucketType,
			},
		}, &gocb.CreateBucketOptions{Context: ctx})
		if err != nil {
			return nil, fmt.Errorf("creating bucket %s: %w", bucketName, err)
		}

		// TODO: For some reason WaitUntilReady times out if we don't put
		// this sleep here
		time.Sleep(time.Second)
	}

	err = cluster.QueryIndexes().CreatePrimaryIndex(bucketName, &gocb.CreatePrimaryQueryIndexOptions{
		IgnoreIfExists: true,
		Context:        ctx,
	})
	if err != nil {
		return nil, fmt.Errorf("creating primary index on %s: %w", bucketName, err)
	}

	bucket := cluster.Bucket(bucketName)
	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		return nil, fmt.Errorf("waiting for couchbase bucket to be ready: %w", err)
	}

	return &DB{cluster: cluster, col: bucket.DefaultCollection()}, nil
}

// GetPieceCidsByMultihash
func (db *DB) GetPieceCidsByMultihash(ctx context.Context, mh multihash.Multihash) ([]cid.Cid, error) {
	key := fmt.Sprintf("%s%s", sprefixMhtoPieceCids, mh.String())
	k := toCouchKey(key)

	var getResult *gocb.GetResult
	getResult, err := db.col.Get(k, &gocb.GetOptions{Context: ctx})
	if err != nil {
		return nil, fmt.Errorf("getting piece cids by multihash %s: %w", mh, err)
	}

	//cas := getResult.Cas()

	var cidStrs []string
	err = getResult.Content(&cidStrs)
	if err != nil {
		return nil, fmt.Errorf("getting piece cids content by multihash %s: %w", mh, err)
	}

	pcids := make([]cid.Cid, 0, len(cidStrs))
	for _, c := range cidStrs {
		pcid, err := cid.Decode(c)
		if err != nil {
			return nil, fmt.Errorf("getting piece cids by multihash %s: parsing piece cid %s: %w", mh, pcid, err)
		}
		pcids = append(pcids, pcid)
	}

	return pcids, nil
}

const throttleSize = 32

// SetMultihashToPieceCid
func (db *DB) SetMultihashesToPieceCid(ctx context.Context, recs []carindex.Record, pieceCid cid.Cid) error {
	throttle := make(chan struct{}, throttleSize)
	var eg errgroup.Group
	for _, r := range recs {
		mh := r.Cid.Hash()

		throttle <- struct{}{}
		eg.Go(func() error {
			defer func() { <-throttle }()

			cbKey := toCouchKey(sprefixMhtoPieceCids + mh.String())

			// Create the array
			upsertDocResult, err := db.col.Upsert(cbKey, []int{}, &gocb.UpsertOptions{Context: ctx})
			if err != nil {
				return fmt.Errorf("adding multihash %s to piece %s: upsert doc: %w", mh, pieceCid, err)
			}

			// Add the piece cid to the set of piece cids
			mops := []gocb.MutateInSpec{
				gocb.ArrayAddUniqueSpec("", pieceCid.String(), &gocb.ArrayAddUniqueSpecOptions{}),
			}
			_, err = db.col.MutateIn(cbKey, mops, &gocb.MutateInOptions{
				Context: ctx,
				Cas:     upsertDocResult.Cas(),
			})
			if err != nil {
				return fmt.Errorf("adding multihash %s to piece %s: mutate doc: %w", mh, pieceCid, err)
			}

			return nil
		})
	}

	return eg.Wait()
}

// SetPieceCidToMetadata
func (db *DB) SetPieceCidToMetadata(ctx context.Context, pieceCid cid.Cid, md model.Metadata) error {
	k := toCouchKey(sprefixPieceCidToMetadata + pieceCid.String())
	_, err := db.col.Upsert(k, md, &gocb.UpsertOptions{Context: ctx})
	if err != nil {
		return fmt.Errorf("setting piece %s metadata: %w", pieceCid, err)
	}

	return nil
}

func (db *DB) AddDealForPiece(ctx context.Context, pieceCid cid.Cid, dealInfo model.DealInfo) error {
	cbKey := toCouchKey(sprefixPieceCidToMetadata + pieceCid.String())

	// Add the deal to the list of deals
	mops := []gocb.MutateInSpec{
		gocb.ArrayAppendSpec("deals", dealInfo, nil),
	}
	_, err := db.col.MutateIn(cbKey, mops, &gocb.MutateInOptions{Context: ctx})
	if err != nil {
		return fmt.Errorf("adding deal %s to piece %s: mutate doc: %w", dealInfo.DealUuid, pieceCid, err)
	}

	return nil
}

// GetPieceCidToMetadata
func (db *DB) GetPieceCidToMetadata(ctx context.Context, pieceCid cid.Cid) (model.Metadata, error) {
	var metadata model.Metadata

	var getResult *gocb.GetResult
	k := toCouchKey(sprefixPieceCidToMetadata + pieceCid.String())
	getResult, err := db.col.Get(k, &gocb.GetOptions{Context: ctx})
	if err != nil {
		return model.Metadata{}, fmt.Errorf("getting piece cid to metadata for piece %s: %w", pieceCid, err)
	}

	//cas := getResult.Cas()

	err = getResult.Content(&metadata)
	if err != nil {
		return model.Metadata{}, fmt.Errorf("getting piece cid to metadata content for piece %s: %w", pieceCid, err)
	}

	return metadata, nil
}

// AllRecords
func (db *DB) AllRecords(ctx context.Context, pieceCid cid.Cid) ([]model.Record, error) {
	cbMap := db.col.Map(pieceCid.String())
	recMap, err := cbMap.Iterator()
	if err != nil {
		return nil, fmt.Errorf("getting all records for piece %s: %w", pieceCid, err)
	}

	recs := make([]model.Record, 0, len(recMap))
	for mhStr, offsetIfce := range recMap {
		mh, err := multihash.FromHexString(mhStr)
		if err != nil {
			return nil, fmt.Errorf("parsing piece cid %s multihash value '%s': %w", pieceCid, mhStr, err)
		}
		offsetStr, ok := offsetIfce.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected type for piece cid %s offset value: %T", pieceCid, offsetIfce)
		}
		offset, err := strconv.ParseUint(offsetStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parsing piece cid %s offset value '%s' as uint64: %w", pieceCid, offsetStr, err)
		}

		recs = append(recs, model.Record{Cid: cid.NewCidV1(cid.Raw, mh), Offset: offset})
	}

	return recs, nil
}

// AddOffsets
func (db *DB) AddOffsets(ctx context.Context, pieceCid cid.Cid, idx carindex.IterableIndex) error {
	mhToOffset := make(map[string]string)
	err := idx.ForEach(func(m multihash.Multihash, offset uint64) error {
		mhToOffset[m.String()] = fmt.Sprintf("%d", offset)
		return nil
	})
	if err != nil {
		return err
	}

	_, err = db.col.Upsert(pieceCid.String(), mhToOffset, &gocb.UpsertOptions{Context: ctx})
	if err != nil {
		return fmt.Errorf("adding offsets for piece %s: %w", pieceCid, err)
	}

	return nil
}

// GetOffset
func (db *DB) GetOffset(ctx context.Context, pieceCid cid.Cid, m multihash.Multihash) (uint64, error) {
	var val string
	cbMap := db.col.Map(pieceCid.String())
	err := cbMap.At(m.String(), &val)
	if err != nil {
		return 0, fmt.Errorf("getting offset for piece %s multihash %s: %w", pieceCid, m, err)
	}

	num, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing piece %s offset value '%s' as uint64: %w", pieceCid, val, err)
	}

	return num, nil
}

func toCouchKey(key string) string {
	return "u:" + key
}

func isNotFoundErr(err error) bool {
	return errors.Is(err, gocb.ErrDocumentNotFound)
}
