package main

import (
	"context"
	"flag"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/filecoin-project/boost/cmd/boostd-data/svc"
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

	done := make(chan struct{})

	srv := svc.New(db, repopath)
	addr := "localhost:8089"
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	log.Infow("server is listening", "addr", "localhost:8089")

	go func() {
		err = srv.Serve(ln)
		if err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}

		done <- struct{}{}
	}()

	// setup a signal handler to cancel the context
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGTERM, syscall.SIGINT)
	select {
	case <-interrupt:
		log.Debugw("got os signal interrupt")
	}

	log.Debug("shutting down server")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		panic(err) // failure/timeout shutting down the server gracefully
	}

	<-done
}
