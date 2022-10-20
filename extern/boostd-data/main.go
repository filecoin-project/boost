package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/filecoin-project/boostd-data/svc"
	logging "github.com/ipfs/go-log/v2"
)

var (
	repopath string
	db       string
	port     int

	log = logging.Logger("boostd-data")
)

func init() {
	logging.SetLogLevel("*", "debug")

	flag.StringVar(&db, "db", "", "db type for boostd-data (couchbase or ldb)")
	flag.IntVar(&port, "port", 8089, "")
	flag.StringVar(&repopath, "repopath", "", "path for repo")
}

func main() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	bdsvc := svc.NewLevelDB(repopath)
	_, err := bdsvc.Start(ctx, port)
	if err != nil {
		log.Fatal(err)
	}

	// setup a signal handler to cancel the context
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGTERM, syscall.SIGINT)
	select {
	case <-interrupt:
		cancel()
		log.Debugw("got os signal interrupt")
	}
}
