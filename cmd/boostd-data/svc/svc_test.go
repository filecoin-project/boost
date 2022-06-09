package svc

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/filecoin-project/boost/cmd/boostd-data/client"
	"github.com/filecoin-project/boost/cmd/boostd-data/model"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
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

	pieceCid, err := cid.Parse("baga6ea4seaqj2j4zfi2xk7okc7fnuw42pip6vjv2tnc4ojsbzlt3rfrdroa7qly")
	if err != nil {
		t.Fatal(err)
	}
	dealInfo := model.DealInfo{}
	err = cl.AddDealForPiece(pieceCid, dealInfo)
	if err != nil {
		t.Fatal(err)
	}

	log.Debug("sleeping for a while.. running tests..")
	time.Sleep(2 * time.Second)

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
