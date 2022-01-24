package testutil

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func HttpTestFileServer(t *testing.T, dir string) (*httptest.Server, error) {
	// start server with data to send
	fileSystem := &SlowFileOpener{Dir: dir}
	handler := http.FileServer(fileSystem)
	svr := httptest.NewServer(handler)
	return svr, nil
}

func HttpTestUnstartedFileServer(t *testing.T, dir string) (*httptest.Server, error) {
	handler := http.FileServer(http.Dir(dir))
	svr := httptest.NewUnstartedServer(handler)
	return svr, nil
}
