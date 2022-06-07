package main

import (
	"flag"
	"fmt"
	"net/http"

	"github.com/ethereum/go-ethereum/rpc"
	"github.com/filecoin-project/boost/cmd/boostd-data/couchbase"
	"github.com/filecoin-project/boost/cmd/boostd-data/ldb"
	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
)

var (
	repopath string
	db       string

	log = logging.Logger("boostd-data")
)

func init() {
	logging.SetLogLevel("*", "debug")

	flag.StringVar(&db, "db", "", "db type for boostd-data (couchbase or ldb)")
	flag.StringVar(&repopath, "repopath", "", "path for repo")
}

func main() {
	flag.Parse()

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

	log.Infow("http.listen", "boostd-data is listening", "localhost:8089")
	http.ListenAndServe("localhost:8089", router)
}
