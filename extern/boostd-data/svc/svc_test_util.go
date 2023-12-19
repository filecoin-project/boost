package svc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/filecoin-project/boost/extern/boostd-data/client"
	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/boost/extern/boostd-data/testutils"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func testService(ctx context.Context, t *testing.T, bdsvc *Service, addr string) {
	ln, err := bdsvc.Start(ctx, addr)
	require.NoError(t, err)

	cl := client.NewStore()
	err = cl.Dial(context.Background(), fmt.Sprintf("ws://%s", ln))
	require.NoError(t, err)
	defer cl.Close(ctx)

	sampleidx := "fixtures/baga6ea4seaqnfhocd544oidrgsss2ahoaomvxuaqxfmlsizljtzsuivjl5hamka.full.idx"

	pieceCid, err := cid.Parse("baga6ea4seaqnfhocd544oidrgsss2ahoaomvxuaqxfmlsizljtzsuivjl5hamka")
	require.NoError(t, err)

	subject, err := loadIndex(sampleidx)
	require.NoError(t, err)

	records, err := getRecords(subject)
	require.NoError(t, err)

	randomuuid, err := uuid.Parse("4d8f5ce6-dbfd-40dc-8b03-29308e97357b")
	require.NoError(t, err)

	err = cl.AddIndex(ctx, pieceCid, records, true)
	require.NoError(t, err)

	di := model.DealInfo{
		DealUuid:    randomuuid.String(),
		SectorID:    abi.SectorNumber(1),
		PieceOffset: 1,
		PieceLength: 2,
		CarLength:   3,
	}

	// Add a deal for the piece
	err = cl.AddDealForPiece(ctx, pieceCid, di)
	require.NoError(t, err)

	// Add the same deal a second time to test uniqueness
	err = cl.AddDealForPiece(ctx, pieceCid, di)
	require.NoError(t, err)

	// There should only be one deal
	dis, err := cl.GetPieceDeals(ctx, pieceCid)
	require.NoError(t, err)
	require.Len(t, dis, 1)
	require.Equal(t, di, dis[0])

	// Add a second deal
	di2 := model.DealInfo{
		DealUuid:    uuid.NewString(),
		SectorID:    abi.SectorNumber(11),
		PieceOffset: 11,
		PieceLength: 12,
		CarLength:   13,
	}
	err = cl.AddDealForPiece(ctx, pieceCid, di2)
	require.NoError(t, err)

	// There should now be two deals
	dis, err = cl.GetPieceDeals(ctx, pieceCid)
	require.NoError(t, err)
	require.Len(t, dis, 2)
	require.Contains(t, dis, di)
	require.Contains(t, dis, di2)

	b, err := hex.DecodeString("1220ff63d7689e2d9567d1a90a7a68425f430137142e1fbc28fe4780b9ee8a5ef842")
	require.NoError(t, err)

	mhash, err := multihash.Cast(b)
	require.NoError(t, err)

	offset, err := cl.GetOffsetSize(ctx, pieceCid, mhash)
	require.NoError(t, err)
	require.EqualValues(t, 3039040395, offset.Offset)
	require.EqualValues(t, 0, offset.Size)

	pcids, err := cl.PiecesContainingMultihash(ctx, mhash)
	require.NoError(t, err)
	require.Len(t, pcids, 1)
	require.Equal(t, pieceCid, pcids[0])

	allPieceCids, err := cl.ListPieces(ctx)
	require.NoError(t, err)
	require.Len(t, allPieceCids, 1)
	require.Equal(t, pieceCid, allPieceCids[0])

	indexed, err := cl.IsIndexed(ctx, pieceCid)
	require.NoError(t, err)
	require.True(t, indexed)

	recs, err := cl.GetRecords(ctx, pieceCid)
	require.NoError(t, err)
	require.Equal(t, len(records), len(recs))

	loadedSubject, err := cl.GetIndex(ctx, pieceCid)
	require.NoError(t, err)

	ok, err := compareIndices(subject, loadedSubject)
	require.NoError(t, err)
	require.True(t, ok)
}

func testServiceFuzz(ctx context.Context, t *testing.T, addr string) {
	cl := client.NewStore()
	err := cl.Dial(context.Background(), "ws://"+addr)
	require.NoError(t, err)
	defer cl.Close(ctx)

	var idxs []index.Index
	for i := 0; i < 10; i++ {
		size := (5 + (i % 3)) << 20
		idxs = append(idxs, createCarIndex(t, size, i+1))
	}

	throttle := make(chan struct{}, 64)
	var eg errgroup.Group
	for _, idx := range idxs {
		idx := idx
		eg.Go(func() error {
			records, err := getRecords(idx)
			require.NoError(t, err)

			randomuuid := uuid.New()
			pieceCid := testutils.GenerateCid()
			err = cl.AddIndex(ctx, pieceCid, records, true)
			require.NoError(t, err)

			di := model.DealInfo{
				DealUuid:    randomuuid.String(),
				SectorID:    abi.SectorNumber(1),
				PieceOffset: 1,
				PieceLength: 2,
				CarLength:   3,
			}

			err = cl.AddDealForPiece(ctx, pieceCid, di)
			require.NoError(t, err)

			dis, err := cl.GetPieceDeals(ctx, pieceCid)
			require.NoError(t, err)
			require.Len(t, dis, 1)
			require.Equal(t, di, dis[0])

			indexed, err := cl.IsIndexed(ctx, pieceCid)
			require.NoError(t, err)
			require.True(t, indexed)

			recs, err := cl.GetRecords(ctx, pieceCid)
			require.NoError(t, err)
			require.Equal(t, len(records), len(recs))

			var offsetEG errgroup.Group
			for _, r := range recs {
				if rand.Float32() > 0.1 {
					continue
				}

				idx := idx
				c := r.Cid
				throttle <- struct{}{}
				offsetEG.Go(func() error {
					defer func() { <-throttle }()

					mhash := c.Hash()
					var err error
					err1 := idx.GetAll(c, func(expected uint64) bool {
						var offsetSize *model.OffsetSize
						offsetSize, err = cl.GetOffsetSize(ctx, pieceCid, mhash)
						if err != nil {
							return false
						}
						if expected != offsetSize.Offset {
							err = fmt.Errorf("cid %s: expected offset %d, got offset %d", c, expected, offsetSize.Offset)
							return false
						}
						return true
					})
					if err != nil {
						return err
					}
					if err1 != nil {
						return err1
					}

					pcids, err := cl.PiecesContainingMultihash(ctx, mhash)
					if err != nil {
						return err
					}
					if len(pcids) != 1 {
						return fmt.Errorf("expected 1 piece, got %d", len(pcids))
					}
					if pieceCid != pcids[0] {
						return fmt.Errorf("expected piece %s, got %s", pieceCid, pcids[0])
					}
					return nil
				})
			}
			err = offsetEG.Wait()
			require.NoError(t, err)

			loadedSubject, err := cl.GetIndex(ctx, pieceCid)
			require.NoError(t, err)

			ok, err := compareIndices(idx, loadedSubject)
			require.NoError(t, err)
			require.True(t, ok)

			return nil
		})
	}

	err = eg.Wait()
	require.NoError(t, err)
}

func createCarIndex(t *testing.T, size int, rseed int) index.Index {
	// Create a CAR file
	randomFilePath, err := testutil.CreateRandomFile(t.TempDir(), rseed, size)
	require.NoError(t, err)
	_, carFilePath, err := testutil.CreateDenseCARv2(t.TempDir(), randomFilePath)
	require.NoError(t, err)
	carFile, err := os.Open(carFilePath)
	require.NoError(t, err)
	defer carFile.Close()
	idx, err := car.ReadOrGenerateIndex(carFile)
	require.NoError(t, err)
	return idx
}

func loadIndex(path string) (index.Index, error) {
	defer func(now time.Time) {
		log.Debugw("loadindex", "took", time.Since(now).String())
	}(time.Now())

	idxf, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer idxf.Close()

	subject, err := index.ReadFrom(idxf)
	if err != nil {
		return nil, err
	}

	return subject, nil
}

func getRecords(subject index.Index) ([]model.Record, error) {
	records := make([]model.Record, 0)

	switch idx := subject.(type) {
	case index.IterableIndex:
		err := idx.ForEach(func(m multihash.Multihash, offset uint64) error {

			cid := cid.NewCidV1(cid.Raw, m)

			records = append(records, model.Record{
				Cid: cid,
				OffsetSize: model.OffsetSize{
					Offset: offset,
					Size:   0,
				},
			})

			return nil
		})
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("wanted %v but got %v\n", multicodec.CarMultihashIndexSorted, idx.Codec())
	}
	return records, nil
}

func compareIndices(subject, subjectDb index.Index) (bool, error) {
	var b bytes.Buffer
	w := bufio.NewWriter(&b)

	_, err := subject.Marshal(w)
	if err != nil {
		return false, err
	}

	var b2 bytes.Buffer
	w2 := bufio.NewWriter(&b2)

	_, err = subjectDb.Marshal(w2)
	if err != nil {
		return false, err
	}

	equal := bytes.Equal(b.Bytes(), b2.Bytes())

	if !equal {
		a, oka := toEntries(subject)
		b, okb := toEntries(subjectDb)
		if oka && okb {
			if len(a) != len(b) {
				return false, fmt.Errorf("index length mismatch: first %d / second %d", len(a), len(b))
			}
			for mh, oa := range a {
				ob, ok := b[mh]
				if !ok {
					return false, fmt.Errorf("second index missing multihash %s", mh)
				}
				if oa != ob {
					return false, fmt.Errorf("offset mismatch for multihash %s: first %d / second %d", mh, oa, ob)
				}
			}
		}
	}

	return equal, nil
}

func testCleanup(ctx context.Context, t *testing.T, bdsvc *Service, addr string) {
	ln, err := bdsvc.Start(ctx, addr)
	require.NoError(t, err)

	cl := client.NewStore()
	err = cl.Dial(context.Background(), fmt.Sprintf("ws://%s", ln))
	require.NoError(t, err)
	defer cl.Close(ctx)

	sampleidx := "fixtures/baga6ea4seaqnfhocd544oidrgsss2ahoaomvxuaqxfmlsizljtzsuivjl5hamka.full.idx"

	pieceCid, err := cid.Parse("baga6ea4seaqnfhocd544oidrgsss2ahoaomvxuaqxfmlsizljtzsuivjl5hamka")
	require.NoError(t, err)

	subject, err := loadIndex(sampleidx)
	require.NoError(t, err)

	records, err := getRecords(subject)
	require.NoError(t, err)

	err = cl.AddIndex(ctx, pieceCid, records, true)
	require.NoError(t, err)

	di := model.DealInfo{
		DealUuid:    uuid.NewString(),
		SectorID:    abi.SectorNumber(1),
		PieceOffset: 1,
		PieceLength: 2,
		CarLength:   3,
	}
	di2 := model.DealInfo{
		DealUuid:    uuid.NewString(),
		SectorID:    abi.SectorNumber(10),
		PieceOffset: 11,
		PieceLength: 12,
		CarLength:   13,
	}

	// Add two deals for the piece
	err = cl.AddDealForPiece(ctx, pieceCid, di)
	require.NoError(t, err)
	err = cl.AddDealForPiece(ctx, pieceCid, di2)
	require.NoError(t, err)

	dis, err := cl.GetPieceDeals(ctx, pieceCid)
	require.NoError(t, err)
	require.Len(t, dis, 2)

	// Remove one deal for the piece
	err = cl.RemoveDealForPiece(ctx, pieceCid, di.DealUuid)
	require.NoError(t, err)

	dis, err = cl.GetPieceDeals(ctx, pieceCid)
	require.NoError(t, err)
	require.Len(t, dis, 1)

	b, err := hex.DecodeString("1220ff63d7689e2d9567d1a90a7a68425f430137142e1fbc28fe4780b9ee8a5ef842")
	require.NoError(t, err)

	mhash, err := multihash.Cast(b)
	require.NoError(t, err)

	offset, err := cl.GetOffsetSize(ctx, pieceCid, mhash)
	require.NoError(t, err)
	require.EqualValues(t, 3039040395, offset.Offset)

	pcids, err := cl.PiecesContainingMultihash(ctx, mhash)
	require.NoError(t, err)
	require.Len(t, pcids, 1)
	require.Equal(t, pieceCid, pcids[0])

	indexed, err := cl.IsIndexed(ctx, pieceCid)
	require.NoError(t, err)
	require.True(t, indexed)

	// Remove the other deal for the piece.
	// After this call there are no deals left, so it should also cause the
	// piece metadata and indexes to be removed.
	err = cl.RemoveDealForPiece(ctx, pieceCid, di2.DealUuid)
	require.NoError(t, err)

	_, err = cl.GetPieceDeals(ctx, pieceCid)
	require.ErrorContains(t, err, "not found")

	_, err = cl.GetOffsetSize(ctx, pieceCid, mhash)
	require.ErrorContains(t, err, "not found")

	_, err = cl.GetRecords(ctx, pieceCid)
	require.ErrorContains(t, err, "not found")

	_, err = cl.PiecesContainingMultihash(ctx, mhash)
	require.ErrorContains(t, err, "not found")

	indexed, err = cl.IsIndexed(ctx, pieceCid)
	require.NoError(t, err)
	require.False(t, indexed)
}

func toEntries(idx index.Index) (map[string]uint64, bool) {
	it, ok := idx.(index.IterableIndex)
	if !ok {
		return nil, false
	}

	entries := make(map[string]uint64)
	err := it.ForEach(func(mh multihash.Multihash, o uint64) error {
		entries[mh.String()] = o
		return nil
	})
	if err != nil {
		return nil, false
	}
	return entries, true
}
