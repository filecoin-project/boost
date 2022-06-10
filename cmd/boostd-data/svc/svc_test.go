package svc

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/filecoin-project/boost/cmd/boostd-data/client"
	"github.com/filecoin-project/boost/cmd/boostd-data/model"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
)

func init() {
	logging.SetLogLevel("*", "debug")
}

func TestLdbService(t *testing.T) {
	addr, cleanup := setupService(t, "ldb")

	cl, err := client.NewPieceMeta("http://" + addr)
	if err != nil {
		t.Fatal(err)
	}

	sampleidx := "fixtures/baga6ea4seaqnfhocd544oidrgsss2ahoaomvxuaqxfmlsizljtzsuivjl5hamka.full.idx"

	pieceCid, err := cid.Parse("baga6ea4seaqnfhocd544oidrgsss2ahoaomvxuaqxfmlsizljtzsuivjl5hamka")
	if err != nil {
		t.Fatal(err)
	}

	subject, err := loadIndex(sampleidx)
	if err != nil {
		t.Fatal(err)
	}

	records, err := getRecords(subject)
	if err != nil {
		t.Fatal(err)
	}

	err = cl.AddIndex(pieceCid, records)
	if err != nil {
		t.Fatal(err)
	}

	b, err := hex.DecodeString("1220ff63d7689e2d9567d1a90a7a68425f430137142e1fbc28fe4780b9ee8a5ef842")
	if err != nil {
		t.Fatal(err)
	}

	mhash, err := multihash.Cast(b)
	if err != nil {
		t.Fatal(err)
	}

	offset, err := cl.GetOffset(pieceCid, mhash)
	if err != nil {
		t.Fatal(err)
	}

	if offset != 3039040395 {
		t.Fatal("got wrong offset")
	}

	//dealInfo := model.DealInfo{}
	//err = cl.AddDealForPiece(pieceCid, dealInfo)
	//if err != nil {
	//t.Fatal(err)
	//}

	log.Debug("sleeping for a while.. running tests..")

	cleanup()
}

func setupService(t *testing.T, db string) (string, func()) {
	addr := "localhost:0"
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	srv := New(db, "")

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

			//fmt.Println("reading offset for: ", m.String(), offset)

			cid := cid.NewCidV1(cid.Raw, m)

			records = append(records, model.Record{
				Cid:    cid,
				Offset: offset,
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
