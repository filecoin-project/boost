package bstoreserver

import (
	"context"
	"fmt"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"net"
	"net/http"
)

var log = logging.Logger("bstoresvr")

const BasePathBlock = "/rpc/block/"

type BstoreHttpServer struct {
	port   int
	ibs    dtypes.IndexBackedBlockstore
	ctx    context.Context
	cancel context.CancelFunc
	server *http.Server
}

func NewBlockstoreHttpServer(ibs dtypes.IndexBackedBlockstore) *BstoreHttpServer {
	return &BstoreHttpServer{port: 8555, ibs: ibs}
}

func (s BstoreHttpServer) Start(ctx context.Context) error {
	log.Info("starting bstore server")

	s.ctx, s.cancel = context.WithCancel(ctx)

	listenAddr := fmt.Sprintf(":%d", s.port)
	handler := http.NewServeMux()
	handler.HandleFunc(BasePathBlock, s.handleBlock)
	s.server = &http.Server{
		Addr:    listenAddr,
		Handler: handler,
		// This context will be the parent of the context associated with all
		// incoming requests
		BaseContext: func(listener net.Listener) context.Context {
			return s.ctx
		},
	}

	go func() {
		if err := s.server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("http.ListenAndServe(): %v", err)
		}
	}()

	return nil
}

func (s BstoreHttpServer) Stop(ctx context.Context) error {
	log.Info("stopping bstore server")
	s.cancel()
	return s.server.Close()
}

func (s BstoreHttpServer) handleBlock(w http.ResponseWriter, r *http.Request) {
	prefixLen := len(BasePathBlock)
	if len(r.URL.Path) <= prefixLen {
		msg := fmt.Sprintf("path '%s' is missing payload CID", r.URL.Path)
		writeError(w, r, http.StatusBadRequest, msg)
		return
	}

	cidstr := r.URL.Path[prefixLen:]
	c, err := cid.Parse(cidstr)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, fmt.Errorf("parsing block cid: %w", err).Error())
		return
	}

	blk, err := s.ibs.Get(s.ctx, c)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, fmt.Errorf("getting block: %w", err).Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(blk.RawData())
}

func writeError(w http.ResponseWriter, r *http.Request, status int, msg string) {
	w.WriteHeader(status)
	w.Write([]byte("Error: " + msg)) //nolint:errcheck
	//alog("%s\tGET %s\n%s",
	//	color.New(color.FgRed).Sprintf("%d", status), r.URL, msg)
}
