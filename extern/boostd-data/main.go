package main

import (
	"context"
	"flag"
	"github.com/filecoin-project/boostd-data/svc"
	logging "github.com/ipfs/go-log/v2"
	"os"
	"os/signal"
	"syscall"
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

	ctx, cancel := context.WithCancel(context.Background())
	bdsvc, err := svc.NewLevelDB(repopath)
	if err != nil {
		log.Fatal(err)
	}
	_, err = bdsvc.Start(ctx)
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
