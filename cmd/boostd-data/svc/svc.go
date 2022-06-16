package svc

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/ethereum/go-ethereum/rpc"
	"github.com/filecoin-project/boost/cmd/boostd-data/couchbase"
	"github.com/filecoin-project/boost/cmd/boostd-data/ldb"
	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
)

var (
	log = logging.Logger("svc")
)

func New(db string, repopath string) *http.Server {
	server := rpc.NewServer()

	switch db {
	case "couchbase":
		ds := couchbase.NewStore()
		server.RegisterName("boostddata", ds)
	case "ldb":
		ds := ldb.NewStore(repopath)
		server.RegisterName("boostddata", ds)
	default:
		panic(fmt.Sprintf("unknown db: %s", db))
	}

	router := mux.NewRouter()
	router.Handle("/", server)

	log.Infow("server is listening", "addr", "localhost:8089")

	return &http.Server{Handler: router}
}

func Setup(db string) (string, func(), error) {
	addr := "localhost:0"
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", nil, err
	}
	srv := New(db, "")

	done := make(chan struct{})

	log.Infow("server is listening", "addr", ln.Addr())

	go func() {
		err = srv.Serve(ln)
		if err != nil && err != http.ErrServerClosed {
			panic(err)
		}

		done <- struct{}{}
	}()

	cleanup := func() {
		log.Debug("shutting down server")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := srv.Shutdown(ctx); err != nil {
			panic(err)
		}

		<-done
	}

	return ln.Addr().String(), cleanup, nil
}
