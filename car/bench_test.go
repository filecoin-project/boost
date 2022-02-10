package car

import (
	"context"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/filecoin-project/boost/testutil"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-merkledag"
	"github.com/ipld/go-car"
	"github.com/stretchr/testify/require"
)

func TestBenchCarReaderSeeker(t *testing.T) {
	ctx := context.Background()

	rawSize := 1024 * 1024 * 1024
	//rawSize := 2 * 1024 * 1024
	t.Logf("Benchmark file of size %d (%.2f MB)", rawSize, float64(rawSize)/(1024*1024))

	rseed := 5
	source := io.LimitReader(rand.New(rand.NewSource(int64(rseed))), int64(rawSize))
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	bs := bstore.NewBlockstore(ds)
	bserv := blockservice.New(bs, nil)
	dserv := merkledag.NewDAGService(bserv)

	t.Log("starting import")
	importStart := time.Now()
	nd, err := DAGImport(dserv, source)
	require.NoError(t, err)
	t.Logf("import took %s", time.Since(importStart))

	// Get the size of the CAR file
	t.Log("getting car file size")
	carSizeStart := time.Now()
	cw := &testutil.CountWriter{}
	err = car.WriteCar(context.Background(), dserv, []cid.Cid{nd.Cid()}, cw)
	require.NoError(t, err)
	t.Logf("car size: %d bytes (%.2f MB) - took %s", cw.Total, float64(cw.Total)/(1024*1024), time.Since(carSizeStart))

	// Just write to io.Discard
	t.Run("car offset writer", func(t *testing.T) {
		cow := NewCarOffsetWriter(nd.Cid(), bs, NewBlockInfoCache())
		start := time.Now()
		err := cow.Write(ctx, io.Discard, 0)
		elapsed := time.Now().Sub(start)
		require.NoError(t, err)
		mbPerS := (float64(cw.Total) / (1024 * 1024)) / (float64(elapsed) / float64(time.Second))
		t.Logf("write of %.2f MB took %s: %.2f MB / s", float64(cw.Total)/(1024*1024), elapsed, mbPerS)
	})

	// Read with CarReadSeeker
	t.Run("car read seeker", func(t *testing.T) {
		cow := NewCarOffsetWriter(nd.Cid(), bs, NewBlockInfoCache())
		crs := NewCarReaderSeeker(ctx, cow, uint64(cw.Total))
		buff := make([]byte, 100*1024*1024)
		var totalRead int
		start := time.Now()
		for {
			count, err := crs.Read(buff)
			totalRead += count
			if err != nil {
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
			}
		}
		elapsed := time.Now().Sub(start)
		require.Equal(t, cw.Total, totalRead)
		mbPerS := (float64(cw.Total) / (1024 * 1024)) / (float64(elapsed) / float64(time.Second))
		t.Logf("read of %.2f MB took %s: %.2f MB / s", float64(cw.Total)/(1024*1024), elapsed, mbPerS)
	})
}
