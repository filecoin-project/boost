package svc

import (
	"fmt"
	"net/http"

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
		ds := couchbase.NewPieceMetaService()
		server.RegisterName("boostddata", ds)
	case "ldb":
		ds := ldb.NewPieceMetaService(repopath)
		server.RegisterName("boostddata", ds)
	default:
		panic(fmt.Sprintf("unknown db: %s", db))
	}

	router := mux.NewRouter()
	router.Handle("/", server)

	log.Infow("server is listening", "addr", "localhost:8089")

	return &http.Server{Handler: router}
}
