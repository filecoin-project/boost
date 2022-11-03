package couchbase

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/couchbase/gocb/v2"
	"github.com/filecoin-project/boost/tracing"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/ipfs/go-cid"
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

type DBSettingsAuth struct {
	Username string
	Password string
}

type DBSettingsBucket struct {
	RAMQuotaMB uint64
}

type DBSettings struct {
	ConnectString string
	Auth          DBSettingsAuth
	Bucket        DBSettingsBucket
}

const connectTimeout = 5 * time.Second

func newDB(ctx context.Context, settings DBSettings) (*DB, error) {
	cluster, err := gocb.Connect(settings.ConnectString, gocb.ClusterOptions{
		TimeoutsConfig: gocb.TimeoutsConfig{
			ConnectTimeout: connectTimeout,
		},
		Authenticator: gocb.PasswordAuthenticator{
			Username: settings.Auth.Username,
			Password: settings.Auth.Password,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("connecting to couchbase DB %s: %w", settings.ConnectString, err)
	}

	_, err = cluster.Buckets().GetBucket(bucketName, &gocb.GetBucketOptions{Context: ctx, Timeout: connectTimeout})
	if err != nil {
		if !errors.Is(err, gocb.ErrBucketNotFound) {
			msg := fmt.Sprintf("getting bucket %s for couchbase server %s", bucketName, settings.ConnectString)
			return nil, fmt.Errorf(msg+": %w\nCheck the couchbase server is running and the username / password are correct", err)
		}

		err = cluster.Buckets().CreateBucket(gocb.CreateBucketSettings{
			BucketSettings: gocb.BucketSettings{
				Name:       bucketName,
				RAMQuotaMB: settings.Bucket.RAMQuotaMB,
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
	ctx, span := tracing.Tracer.Start(ctx, "db.get_piece_cids_by_multihash")
	defer span.End()

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

// SetMultihashesToPieceCid
func (db *DB) SetMultihashesToPieceCid(ctx context.Context, mhs []multihash.Multihash, pieceCid cid.Cid) error {
	ctx, span := tracing.Tracer.Start(ctx, "db.set_multihashes_to_piece_cid")
	defer span.End()

	throttle := make(chan struct{}, throttleSize)
	var eg errgroup.Group
	for _, mh := range mhs {
		mh := mh

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
	ctx, span := tracing.Tracer.Start(ctx, "db.set_piece_cid_to_metadata")
	defer span.End()

	k := toCouchKey(sprefixPieceCidToMetadata + pieceCid.String())
	_, err := db.col.Upsert(k, md, &gocb.UpsertOptions{Context: ctx})
	if err != nil {
		return fmt.Errorf("setting piece %s metadata: %w", pieceCid, err)
	}

	return nil
}

func (db *DB) AddDealForPiece(ctx context.Context, pieceCid cid.Cid, dealInfo model.DealInfo) error {
	ctx, span := tracing.Tracer.Start(ctx, "db.add_deal_for_piece")
	defer span.End()

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
	ctx, span := tracing.Tracer.Start(ctx, "db.get_piece_cid_to_metadata")
	defer span.End()

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
	ctx, span := tracing.Tracer.Start(ctx, "db.all_records")
	defer span.End()

	cbKey := toCouchKey(sprefixPieceCidToOffsets + pieceCid.String())
	cbMap := db.col.Map(cbKey)
	recMap, err := cbMap.Iterator()
	if err != nil {
		return nil, fmt.Errorf("getting all records for piece %s: %w", pieceCid, err)
	}

	recs := make([]model.Record, 0, len(recMap))
	for mhStr, offsetSizeIfce := range recMap {
		mh, err := multihash.FromHexString(mhStr)
		if err != nil {
			return nil, fmt.Errorf("parsing piece cid %s multihash value '%s': %w", pieceCid, mhStr, err)
		}
		val, ok := offsetSizeIfce.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("unexpected type for piece cid %s offset/size value: %T", pieceCid, offsetSizeIfce)
		}

		offsetIfce, ok := val["o"]
		if !ok {
			return nil, fmt.Errorf("parsing piece %s offset value '%s': missing Offset map key", pieceCid, val)
		}
		offsetStr, ok := offsetIfce.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected type for piece cid %s offset value: %T", pieceCid, offsetIfce)
		}
		offset, err := strconv.ParseUint(offsetStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parsing piece %s offset value '%s' as uint64: %w", pieceCid, val, err)
		}

		sizeIfce, ok := val["s"]
		if !ok {
			return nil, fmt.Errorf("parsing piece %s offset value '%s': missing Size map key", pieceCid, val)
		}
		sizeStr, ok := sizeIfce.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected type for piece cid %s size value: %T", pieceCid, offsetIfce)
		}
		size, err := strconv.ParseUint(sizeStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parsing piece %s size value '%s' as uint64: %w", pieceCid, val, err)
		}

		recs = append(recs, model.Record{Cid: cid.NewCidV1(cid.Raw, mh), OffsetSize: model.OffsetSize{
			Offset: offset,
			Size:   size,
		}})
	}

	return recs, nil
}

type offsetSize struct {
	// Need to use strings instead of uint64 because uint64 may not fit into
	// Javascript number
	Offset string `json:"o"`
	Size   string `json:"s"`
}

// AddIndexRecords
func (db *DB) AddIndexRecords(ctx context.Context, pieceCid cid.Cid, recs []model.Record) error {
	ctx, span := tracing.Tracer.Start(ctx, "db.add_index_records")
	defer span.End()

	mhToOffsetSize := make(map[string]offsetSize)
	for _, rec := range recs {
		mhToOffsetSize[rec.Cid.Hash().String()] = offsetSize{
			Offset: fmt.Sprintf("%d", rec.Offset),
			Size:   fmt.Sprintf("%d", rec.Size),
		}
	}

	cbKey := toCouchKey(sprefixPieceCidToOffsets + pieceCid.String())
	_, err := db.col.Upsert(cbKey, mhToOffsetSize, &gocb.UpsertOptions{Context: ctx})
	if err != nil {
		return fmt.Errorf("adding offset / sizes for piece %s: %w", pieceCid, err)
	}

	return nil
}

// GetOffsetSize
func (db *DB) GetOffsetSize(ctx context.Context, pieceCid cid.Cid, m multihash.Multihash) (*model.OffsetSize, error) {
	ctx, span := tracing.Tracer.Start(ctx, "db.get_offset_size")
	defer span.End()

	cbKey := toCouchKey(sprefixPieceCidToOffsets + pieceCid.String())
	cbMap := db.col.Map(cbKey)
	var val offsetSize
	err := cbMap.At(m.String(), &val)
	if err != nil {
		return nil, fmt.Errorf("getting offset/size for piece %s multihash %s: %w", pieceCid, m, err)
	}

	offset, err := strconv.ParseUint(val.Offset, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parsing piece %s offset value '%s' as uint64: %w", pieceCid, val, err)
	}
	size, err := strconv.ParseUint(val.Size, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parsing piece %s size value '%s' as uint64: %w", pieceCid, val, err)
	}

	return &model.OffsetSize{Offset: offset, Size: size}, nil
}

func toCouchKey(key string) string {
	return "u:" + key
}

func isNotFoundErr(err error) bool {
	return errors.Is(err, gocb.ErrDocumentNotFound)
}
