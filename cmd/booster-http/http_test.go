package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"testing"

	mocks_booster_http "github.com/filecoin-project/boost/cmd/booster-http/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestNewHttpServer(t *testing.T) {

	// Create a new mock Http server
	ctrl := gomock.NewController(t)
	httpServer := NewHttpServer("", 7777, false, mocks_booster_http.NewMockHttpServerApi(ctrl))
	httpServer.Start(context.Background())

	// Check that server is up
	resp, err := http.Get("http://127.0.0.1:7777/")
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)

	// Stop the server
	err = httpServer.Stop()
	require.NoError(t, err)
}

func TestHttpGzipResponse(t *testing.T) {

	// Create a new mock Http server with custom functions
	ctrl := gomock.NewController(t)
	httpServer := NewHttpServer("", 7777, false, mocks_booster_http.NewMockHttpServerApi(ctrl))
	httpServer.ctx, httpServer.cancel = context.WithCancel(context.Background())
	listenAddr := fmt.Sprintf(":%d", httpServer.port)
	handler := http.NewServeMux()
	handler.HandleFunc(httpServer.pieceBasePath(), testHandlePieceRequest)
	handler.HandleFunc("/", httpServer.handleIndex)
	handler.HandleFunc("/index.html", httpServer.handleIndex)
	httpServer.server = &http.Server{
		Addr:    listenAddr,
		Handler: handler,
		// This context will be the parent of the context associated with all
		// incoming requests
		BaseContext: func(listener net.Listener) context.Context {
			return httpServer.ctx
		},
	}

	go func() {
		if err := httpServer.server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("http.ListenAndServe(): %w", err)
		}
	}()

	// Create a client
	client := new(http.Client)
	request, err := http.NewRequest("GET", "http://127.0.0.1:7777/piece?payloadCid=bafyCid&format=piece", nil)
	request.Header.Add("Accept-Encoding", "gzip")

	response, err := client.Do(request)
	require.NoError(t, err)
	require.Equal(t, "gzip", response.Header.Get("Content-Encoding"))
	defer response.Body.Close()

	// Stop the server
	err = httpServer.Stop()
	require.NoError(t, err)
}

func testHandlePieceRequest(w http.ResponseWriter, r *http.Request) {
	f, _ := os.Create("data")
	for i := 0; i < 1000; i++ {
		_, _ = f.WriteString("Test file")
	}
	defer f.Close()
	defer os.Remove("data")
	serveContent(w, r, f, "application/piece")
}
