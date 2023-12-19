package svc

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/filecoin-project/boost/extern/boostd-data/client"
	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/boost/extern/boostd-data/testutils"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/stretchr/testify/require"
)

var tlg = logging.Logger("tlg")

func TestSizeLimit(t *testing.T) {
	// This test takes an hour so don't run as part of regular
	// test suite
	t.Skip()

	_ = logging.SetLogLevel("cbtest", "debug")
	_ = logging.SetLogLevel("tlg", "info")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()

	t.Run("leveldb", func(t *testing.T) {
		bdsvc, err := NewLevelDB("")
		require.NoError(t, err)

		testSizeLimit(ctx, t, bdsvc, "localhost:0")
	})

	t.Run("yugabyte", func(t *testing.T) {
		_ = logging.SetLogLevel("boostd-data-yb", "debug")

		bdsvc := SetupYugabyte(t)
		addr := "localhost:0"
		testSizeLimit(ctx, t, bdsvc, addr)
	})
}

func testSizeLimit(ctx context.Context, t *testing.T, bdsvc *Service, addr string) {
	ln, err := bdsvc.Start(ctx, addr)
	require.NoError(t, err)

	cl := client.NewStore()
	err = cl.Dial(context.Background(), fmt.Sprintf("ws://%s", ln.String()))
	require.NoError(t, err)
	defer cl.Close(ctx)

	pieceCid, err := cid.Parse("baga6ea4seaqnfhocd544oidrgsss2ahoaomvxuaqxfmlsizljtzsuivjl5hamka")
	require.NoError(t, err)

	// 32GB file with 1k block size
	recordCount := 32 * 1024 * 1024
	baseCid := testutils.GenerateCid().Bytes()
	recStart := time.Now()
	tlg.Infof("generating %d records", recordCount)
	var records []model.Record
	for i := 0; i < recordCount; i++ {
		c, err := generateRandomCid(baseCid)
		require.NoError(t, err)
		records = append(records, model.Record{
			Cid: c,
			OffsetSize: model.OffsetSize{
				Offset: math.MaxUint64,
				Size:   math.MaxUint64,
			},
		})
	}
	tlg.Infof("generated %d records in %s", recordCount, time.Since(recStart))

	bz, err := json.Marshal(records[0])
	tlg.Infof("%s (%d bytes)", bz, len(bz))

	addStart := time.Now()
	tlg.Infof("adding index")
	err = cl.AddIndex(ctx, pieceCid, records, true)
	require.NoError(t, err)
	tlg.Infof("added index in %s", time.Since(addStart))

	getStart := time.Now()
	tlg.Infof("getting index")
	idx, err := cl.GetIndex(ctx, pieceCid)
	require.NoError(t, err)
	tlg.Infof("got index in %s", time.Since(getStart))

	entries, ok := toEntries(idx)
	require.True(t, ok)
	tlg.Infof("got %d entries back", len(entries))
	require.Len(t, entries, recordCount)

	_, has := entries[records[recordCount/2].Cid.Hash().String()]
	require.True(t, has)
}

func generateRandomCid(baseCid []byte) (cid.Cid, error) {
	buff := make([]byte, len(baseCid))
	copy(buff, baseCid)

	_, err := rand.Read(buff[len(buff)-8:])
	if err != nil {
		return cid.Undef, err
	}

	_, c, err := cid.CidFromBytes(buff)
	if err != nil {
		return cid.Undef, fmt.Errorf("generating cid: %w", err)
	}

	return c, nil
}
