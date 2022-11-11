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
	port     int

	log = logging.Logger("boostd-data")
)

func init() {
	logging.SetLogLevel("*", "debug")

	flag.StringVar(&db, "db", "", "db type for piece directory (couchbase or ldb)")
	flag.StringVar(&repopath, "repopath", "", "path for repo")
	flag.IntVar(&port, "port", 8042, "port to run the service on")
}

func main() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	bdsvc, err := svc.NewLevelDB(repopath)
	if err != nil {
		log.Fatal(err)
	}
	err = bdsvc.Start(ctx, port)
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
