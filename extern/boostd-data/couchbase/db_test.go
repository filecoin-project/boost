package couchbase

import (
	"context"
	"fmt"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

func TestBitMask(t *testing.T) {
	tcs := []struct {
		bits     int
		expected [2]byte
	}{{
		bits:     0,
		expected: [2]byte{0, 0},
	}, {
		bits:     1,
		expected: [2]byte{0, 1},
	}, {
		bits:     3,
		expected: [2]byte{0, 7},
	}, {
		bits:     8,
		expected: [2]byte{0, 255},
	}, {
		bits:     9,
		expected: [2]byte{1, 255},
	}, {
		bits:     15,
		expected: [2]byte{127, 255},
	}, {
		bits:     16,
		expected: [2]byte{255, 255},
	}}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("%d", tc.bits), func(t *testing.T) {
			mask := get2ByteMask(tc.bits)
			require.Equal(t, tc.expected, mask)
		})
	}
}

func TestGetShardPrefix(t *testing.T) {
	tcs := []struct {
		shardIndex int
		expected   string
	}{{
		shardIndex: 0,
		expected:   string([]byte{0, 0}),
	}, {
		shardIndex: 1,
		expected:   string([]byte{0, 1}),
	}, {
		shardIndex: 3,
		expected:   string([]byte{0, 3}),
	}, {
		shardIndex: 8,
		expected:   string([]byte{0, 8}),
	}, {
		shardIndex: 256,
		expected:   string([]byte{1, 0}),
	}, {
		shardIndex: 257,
		expected:   string([]byte{1, 1}),
	}, {
		shardIndex: (1 << 16) - 1,
		expected:   string([]byte{255, 255}),
	}}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("%d", tc.shardIndex), func(t *testing.T) {
			prefix, err := getShardPrefix(tc.shardIndex)
			require.NoError(t, err)
			require.Equal(t, tc.expected, prefix)
		})
	}

	t.Run(fmt.Sprintf("%d", 1<<16), func(t *testing.T) {
		_, err := getShardPrefix(1 << 16)
		require.Error(t, err)
	})
}

var testCouchSettings = DBSettings{
	ConnectString: "couchbase://127.0.0.1",
	Auth: DBSettingsAuth{
		Username: "Administrator",
		Password: "boostdemo",
	},
	PieceMetadataBucket: DBSettingsBucket{
		RAMQuotaMB: 128,
	},
	MultihashToPiecesBucket: DBSettingsBucket{
		RAMQuotaMB: 128,
	},
	PieceOffsetsBucket: DBSettingsBucket{
		RAMQuotaMB: 128,
	},
}

func TestSharding(t *testing.T) {
	// Skip until the tests are refactored such that we can create a couchbase
	// instance from docker
	t.Skip()

	// Reduce the maximum records per shard such that lots of shards will be created
	saved := maxRecsPerShard
	maxRecsPerShard = 5
	defer func() {
		maxRecsPerShard = saved
	}()

	fileSize := 2 * 1024 * 1024

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create a new couchbase db
	db, err := newDB(context.Background(), testCouchSettings)
	require.NoError(t, err)

	// Create a CAR file
	randomFilePath, err := testutil.CreateRandomFile(t.TempDir(), 1, fileSize)
	require.NoError(t, err)
	_, carFilePath, err := testutil.CreateDenseCARv2(t.TempDir(), randomFilePath)
	require.NoError(t, err)
	carFile, err := os.Open(carFilePath)
	require.NoError(t, err)
	defer carFile.Close()
	idx, err := car.ReadOrGenerateIndex(carFile)
	require.NoError(t, err)

	// Get the records from the CAR file
	pieceCid, err := cid.Parse("baga6ea4seaqnfhocd544oidrgsss2ahoaomvxuaqxfmlsizljtzsuivjl5hamka")
	require.NoError(t, err)

	var recs []model.Record
	err = idx.(index.IterableIndex).ForEach(func(m multihash.Multihash, offset uint64) error {
		cid := cid.NewCidV1(cid.Raw, m)

		recs = append(recs, model.Record{
			Cid: cid,
			OffsetSize: model.OffsetSize{
				Offset: offset,
				Size:   0,
			},
		})

		return nil
	})
	require.NoError(t, err)

	// Add the records to the db
	err = db.AddIndexRecords(ctx, pieceCid, recs)
	require.NoError(t, err)

	// Pick a random record and get its offset and size
	rec := recs[len(recs)/2]
	hash := rec.Cid.Hash()
	offsetSize, err := db.GetOffsetSize(ctx, pieceCid, hash, len(recs))
	require.NoError(t, err)
	require.Equal(t, rec.Offset, offsetSize.Offset)
	require.Equal(t, rec.Size, offsetSize.Size)

	// Get all records and ensure that they are the same as the records that
	// were put in
	allRecs, err := db.AllRecords(ctx, pieceCid, len(recs))
	require.NoError(t, err)
	require.Equal(t, len(recs), len(allRecs))
	require.ElementsMatch(t, recs, allRecs)
}

func TestFlagging(t *testing.T) {
	// Skip until the tests are refactored such that we can create a couchbase
	// instance from docker
	//t.Skip()

	fileSize := 2 * 1024 * 1024

	ctx, cancel := context.WithTimeout(context.Background(), 5000*time.Second)
	defer cancel()

	// Create a new couchbase db
	db, err := newDB(context.Background(), testCouchSettings)
	require.NoError(t, err)

	//_, err = db.cluster.Query("DELETE FROM `"+metaBucket+"`._default.`piece-tracker`", &gocb.QueryOptions{Context: ctx})
	//require.NoError(t, err)
	//_, err = db.cluster.Query("DELETE FROM `"+metaBucket+"`._default.`piece-flagged`", &gocb.QueryOptions{Context: ctx})
	//require.NoError(t, err)

	// Create a CAR file
	randomFilePath, err := testutil.CreateRandomFile(t.TempDir(), 1, fileSize)
	require.NoError(t, err)
	_, carFilePath, err := testutil.CreateDenseCARv2(t.TempDir(), randomFilePath)
	require.NoError(t, err)
	carFile, err := os.Open(carFilePath)
	require.NoError(t, err)
	defer carFile.Close()
	idx, err := car.ReadOrGenerateIndex(carFile)
	require.NoError(t, err)

	// Get the records from the CAR file
	pieceCid, err := cid.Parse("baga6ea4seaqnfhocd544oidrgsss2ahoaomvxuaqxfmlsizljtzsuivjl5hamka")
	require.NoError(t, err)

	var recs []model.Record
	err = idx.(index.IterableIndex).ForEach(func(m multihash.Multihash, offset uint64) error {
		cid := cid.NewCidV1(cid.Raw, m)

		recs = append(recs, model.Record{
			Cid: cid,
			OffsetSize: model.OffsetSize{
				Offset: offset,
				Size:   0,
			},
		})

		return nil
	})
	require.NoError(t, err)

	// Add the records to the db
	err = db.AddIndexRecords(ctx, pieceCid, recs)
	require.NoError(t, err)

	time.Sleep(2 * time.Millisecond)

	pcids, err := db.NextPiecesToCheck(ctx, 0)
	require.NoError(t, err)
	require.Len(t, pcids, 1)
}
