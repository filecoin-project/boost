package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/filecoin-project/boost/pkg/devnet"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go devnet.Run(ctx, done)

	// setup a signal handler to cancel the context
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGTERM, syscall.SIGINT)
	select {
	case <-interrupt:
		log.Println("closing as we got interrupt")
		cancel()
	case <-ctx.Done():
	}

	<-done
}
