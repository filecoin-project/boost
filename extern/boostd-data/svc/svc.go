package svc

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/filecoin-project/boost/extern/boostd-data/ldb"
	"github.com/filecoin-project/boost/extern/boostd-data/metrics"
	"github.com/filecoin-project/boost/extern/boostd-data/svc/types"
	"github.com/filecoin-project/boost/extern/boostd-data/yugabyte"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
)

var (
	log = logging.Logger("piecedir")
)

type Service struct {
	Impl types.ServiceImpl
}

func NewYugabyte(settings yugabyte.DBSettings, migrator *yugabyte.Migrator, storeOpts ...yugabyte.StoreOpt) *Service {
	return &Service{Impl: yugabyte.NewStore(settings, migrator, storeOpts...)}
}

func NewLevelDB(repoPath string) (*Service, error) {
	if repoPath != "" { // an empty repo path is used for testing
		var err error
		repoPath, err = MakeLevelDBDir(repoPath)
		if err != nil {
			return nil, err
		}
	}

	return &Service{Impl: ldb.NewStore(repoPath)}, nil
}

func MakeLevelDBDir(repoPath string) (string, error) {
	repoPath = path.Join(repoPath, "lid", "leveldb")
	if err := os.MkdirAll(repoPath, os.ModePerm); err != nil {
		return "", fmt.Errorf("creating leveldb repo directory %s: %w", repoPath, err)
	}
	return repoPath, nil
}

func (s *Service) Start(ctx context.Context, addr string) (net.Addr, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("setting up listener for local index directory service: %w", err)
	}

	err = s.Impl.Start(ctx)
	if err != nil {
		return nil, fmt.Errorf("starting local index directory service: %w", err)
	}

	server := jsonrpc.NewServer()
	server.Register("boostddata", s.Impl)
	router := mux.NewRouter()
	router.Handle("/", server)
	router.Handle("/metrics", metrics.Exporter("boostd_data")) // metrics

	srv := &http.Server{Handler: router}
	log.Infow("local index directory server is listening", "addr", ln.Addr())

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
		log.Debug("shutting down server")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Errorf("shutting down local index directory server: %s", err)
		}

		<-done
	}()

	return ln.Addr(), nil
}
