package svc

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/filecoin-project/boostd-data/couchbase"
	"github.com/filecoin-project/boostd-data/ldb"
	"github.com/filecoin-project/boostd-data/svc/types"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
)

var (
	log = logging.Logger("piecedir")
)

type Service struct {
	impl types.ServiceImpl
}

func NewCouchbase(settings couchbase.DBSettings) *Service {
	return &Service{impl: couchbase.NewStore(settings)}
}

func NewLevelDB(repoPath string) (*Service, error) {
	if repoPath != "" { // an empty repo path is used for testing
		var err error
		repoPath, err = MakeLevelDBDir(repoPath)
		if err != nil {
			return nil, err
		}
	}

	return &Service{impl: ldb.NewStore(repoPath)}, nil
}

func MakeLevelDBDir(repoPath string) (string, error) {
	repoPath = path.Join(repoPath, "piece-directory", "leveldb")
	if err := os.MkdirAll(repoPath, os.ModePerm); err != nil {
		return "", fmt.Errorf("creating leveldb repo directory %s: %w", repoPath, err)
	}
	return repoPath, nil
}

func (s *Service) Start(ctx context.Context) (string, error) {
	addr := "localhost:0"
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", fmt.Errorf("setting up listener for boostd-data service: %w", err)
	}

	err = s.impl.Start(ctx)
	if err != nil {
		return "", fmt.Errorf("starting boostd-data service: %w", err)
	}

	server := jsonrpc.NewServer()
	server.Register("boostddata", s.impl)
	router := mux.NewRouter()
	router.Handle("/", server)

	srv := &http.Server{Handler: router}
	log.Infow("boost-data server is listening", "addr", ln.Addr())

	done := make(chan struct{})
	go func() {
		err = srv.Serve(ln)
		if err != nil && err != http.ErrServerClosed {
			log.Errorf("exiting boost-data server: %s", err)
		}

		done <- struct{}{}
	}()

	go func() {
		<-ctx.Done()
		log.Debug("shutting down server")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Errorf("shutting down boost-data server: %s", err)
		}

		<-done
	}()

	return ln.Addr().String(), nil
}
