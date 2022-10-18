package svc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"math/rand"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/boostd-data/client"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestService(t *testing.T) {
	logging.SetLogLevel("*", "debug")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t.Run("level db", func(t *testing.T) {
		testService(ctx, t, "ldb")
	})
	t.Run("couchbase", func(t *testing.T) {
		testService(ctx, t, "couchbase")
	})
}

func testService(ctx context.Context, t *testing.T, name string) {
	addr, cleanup, err := Setup(ctx, name, "")
	require.NoError(t, err)

	cl, err := client.NewStore("http://" + addr)
	require.NoError(t, err)

	sampleidx := "fixtures/baga6ea4seaqnfhocd544oidrgsss2ahoaomvxuaqxfmlsizljtzsuivjl5hamka.full.idx"

	pieceCid, err := cid.Parse("baga6ea4seaqnfhocd544oidrgsss2ahoaomvxuaqxfmlsizljtzsuivjl5hamka")
	require.NoError(t, err)

	subject, err := loadIndex(sampleidx)
	require.NoError(t, err)

	records, err := getRecords(subject)
	require.NoError(t, err)

	randomuuid := uuid.New()

	err = cl.AddIndex(ctx, pieceCid, records)
	require.NoError(t, err)

	di := model.DealInfo{
		DealUuid:    randomuuid,
		SectorID:    abi.SectorNumber(1),
		PieceOffset: 1,
		PieceLength: 2,
		CarLength:   3,
	}

	err = cl.AddDealForPiece(ctx, pieceCid, di)
	require.NoError(t, err)

	b, err := hex.DecodeString("1220ff63d7689e2d9567d1a90a7a68425f430137142e1fbc28fe4780b9ee8a5ef842")
	require.NoError(t, err)

	mhash, err := multihash.Cast(b)
	require.NoError(t, err)

	offset, err := cl.GetOffsetSize(ctx, pieceCid, mhash)
	require.NoError(t, err)
	require.EqualValues(t, 3039040395, offset.Offset)
	require.EqualValues(t, 0, offset.Size)

	pcids, err := cl.PiecesContaining(ctx, mhash)
	require.NoError(t, err)
	require.Len(t, pcids, 1)
	require.Equal(t, pieceCid, pcids[0])

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

	loadedSubject, err := cl.GetIndex(ctx, pieceCid)
	require.NoError(t, err)

	ok, err := compareIndices(subject, loadedSubject)
	require.NoError(t, err)
	require.True(t, ok)

	cleanup()
}

func TestServiceFuzz(t *testing.T) {
	t.Skip()
	logging.SetLogLevel("*", "info")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	t.Run("level db", func(t *testing.T) {
		testServiceFuzz(ctx, t, "ldb")
	})
	t.Run("couchbase", func(t *testing.T) {
		testServiceFuzz(ctx, t, "couchbase")
	})
}

func testServiceFuzz(ctx context.Context, t *testing.T, name string) {
	addr, cleanup, err := Setup(ctx, name, "")
	require.NoError(t, err)
	defer cleanup()

	cl, err := client.NewStore("http://" + addr)
	require.NoError(t, err)

	var idxs []index.Index
	for i := 0; i < 10; i++ {
		idxs = append(idxs, createCarIndex(t))
	}

	throttle := make(chan struct{}, 64)
	var eg errgroup.Group
	for _, idx := range idxs {
		idx := idx
		eg.Go(func() error {
			records, err := getRecords(idx)
			require.NoError(t, err)

			randomuuid := uuid.New()
			pieceCid := testutil.GenerateCid()
			err = cl.AddIndex(ctx, pieceCid, records)
			require.NoError(t, err)

			di := model.DealInfo{
				DealUuid:    randomuuid,
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

				throttle <- struct{}{}
				mhash := r.Cid.Hash()
				offsetEG.Go(func() error {
					defer func() { <-throttle }()

					var err error
					idx.GetAll(r.Cid, func(expected uint64) bool {
						var offsetSize *model.OffsetSize
						offsetSize, err = cl.GetOffsetSize(ctx, pieceCid, mhash)
						if err != nil {
							return false
						}
						if expected != offsetSize.Offset {
							err = fmt.Errorf("cid %s: expected offset %d, got offset %d", r.Cid, expected, offsetSize.Offset)
							return false
						}
						return true
					})
					if err != nil {
						return err
					}

					pcids, err := cl.PiecesContaining(ctx, mhash)
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

func createCarIndex(t *testing.T) index.Index {
	// Create a CAR file
	rseed := rand.Int()
	size := (5 + rand.Intn(3)) << 20

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

func setupService(ctx context.Context, t *testing.T, db string) (string, func()) {
	addr := "localhost:0"
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(ctx, db, "")
	require.NoError(t, err)

	done := make(chan struct{})

	log.Infow("server is listening", "addr", ln.Addr())

	go func() {
		err = srv.Serve(ln)
		if err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}

		done <- struct{}{}
	}()

	cleanup := func() {
		log.Debug("shutting down server")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := srv.Shutdown(ctx); err != nil {
			panic(err) // failure/timeout shutting down the server gracefully
		}

		<-done
	}

	return ln.Addr().String(), cleanup
}

func loadIndex(path string) (index.Index, error) {
	defer func(now time.Time) {
		log.Debugw("loadindex", "took", fmt.Sprintf("%s", time.Since(now)))
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
		return nil, errors.New(fmt.Sprintf("wanted %v but got %v\n", multicodec.CarMultihashIndexSorted, idx.Codec()))
	}
	return records, nil
}

func compareIndices(subject, subjectDb index.Index) (bool, error) {
	var b bytes.Buffer
	w := bufio.NewWriter(&b)

	subject.Marshal(w)

	var b2 bytes.Buffer
	w2 := bufio.NewWriter(&b2)

	subjectDb.Marshal(w2)

	res := bytes.Compare(b.Bytes(), b2.Bytes())

	return res == 0, nil
}
