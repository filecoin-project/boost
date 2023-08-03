package adminserver

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("adminserver")

func Start(ctx context.Context, addr string) (net.Addr, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("setting up listener for admin server: %w", err)
	}

	as := &Service{}

	server := jsonrpc.NewServer()
	server.Register("adminserver", as)
	router := mux.NewRouter()
	router.Handle("/", server)

	srv := &http.Server{Handler: router}
	log.Infow("admin server is listening", "addr", ln.Addr())

	done := make(chan struct{})
	go func() {
		err = srv.Serve(ln)
		if err != nil && err != http.ErrServerClosed {
			log.Errorf("exiting local index directory server: %s", err)
		}

		done <- struct{}{}
	}()

	go func() {
		<-ctx.Done()
		log.Debug("shutting down admin server")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Errorf("shutting down admin server: %s", err)
		}

		<-done
	}()

	return ln.Addr(), nil
}

type HealthzResponse struct {
	Healthy bool
}

type ConfigzResponse struct {
	RuntimeConfigHere string
}

type Service struct {
}

func (s *Service) Healthz(ctx context.Context) (*HealthzResponse, error) {
	return &HealthzResponse{Healthy: true}, nil
}

func (s *Service) Configz(ctx context.Context) (*ConfigzResponse, error) {
	return &ConfigzResponse{RuntimeConfigHere: "tomlpastehere"}, nil
}
