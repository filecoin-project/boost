package svc

import (
	"bufio"
	"bytes"
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
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/google/uuid"
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
	addr, cleanup, err := Setup("ldb")
	if err != nil {
		t.Fatal(err)
	}

	cl, err := client.NewStore("http://" + addr)
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

	randomuuid := uuid.New()

	err = cl.AddIndex(pieceCid, records)
	if err != nil {
		t.Fatal(err)
	}

	di := model.DealInfo{
		DealUuid:    randomuuid,
		SectorID:    abi.SectorNumber(1),
		PieceOffset: 1,
		PieceLength: 2,
		CarLength:   3,
	}

	err = cl.AddDealForPiece(pieceCid, di)
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

	pcids, err := cl.PiecesContaining(mhash)

	if len(pcids) != 1 {
		t.Fatalf("expected len of 1 for pieceCids, got: %d", len(pcids))
	}

	if !pcids[0].Equals(pieceCid) {
		t.Fatal("expected for pieceCids to match")
	}

	dis, err := cl.GetPieceDeals(pieceCid)

	if len(dis) != 1 {
		t.Fatalf("expected len of 1 for dis, got: %d", len(dis))
	}

	if dis[0] != di {
		t.Fatal("expected for dealInfos to match")
	}

	indexed, err := cl.IsIndexed(pieceCid)
	if err != nil {
		t.Fatal(err)
	}

	if !indexed {
		t.Fatal("expected pieceCid to be indexed")
	}

	recs, err := cl.GetRecords(pieceCid)
	if err != nil {
		t.Fatal(err)
	}

	if len(recs) == 0 {
		t.Fatal("expected to get records back from GetIndex")
	}

	loadedSubject, err := cl.GetIndex(pieceCid)
	if err != nil {
		t.Fatal(err)
	}

	ok, err := compareIndices(subject, loadedSubject)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		log.Fatal("compare failed")
	}

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
