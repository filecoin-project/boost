package gql

import (
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

const DummyDealsDir = "/tmp/dummy"
const DummyDealsPrefix = "dummy"

var DummyDealsBase = fmt.Sprintf("http://localhost:%d/"+DummyDealsPrefix, httpPort)

func serveDummyDeals() error {
	// Serve dummy-deals
	dpath := "/" + DummyDealsPrefix + "/"
	if err := os.MkdirAll(DummyDealsDir, 0755); err != nil {
		return fmt.Errorf("failed to mk directory %s for dummy deals: %w", DummyDealsDir, err)
	}
	fileSystem := &slowFileOpener{dir: DummyDealsDir}
	http.Handle(dpath, http.StripPrefix(dpath, http.FileServer(fileSystem)))
	return nil
}

const slowChunkSize = 256
const slowChunkDelay = time.Millisecond

type slowReader struct {
	*os.File
}

func (r *slowReader) Read(p []byte) (int, error) {
	bz := make([]byte, slowChunkSize)
	time.Sleep(slowChunkDelay)
	n, err := r.File.Read(bz)
	copy(p, bz)
	return n, err
}

type slowFileOpener struct {
	dir string
}

func (s *slowFileOpener) Open(name string) (http.File, error) {
	if filepath.Separator != '/' && strings.ContainsRune(name, filepath.Separator) {
		return nil, errors.New("http: invalid character in file path")
	}
	dir := s.dir
	if dir == "" {
		dir = "."
	}
	fullName := filepath.Join(dir, filepath.FromSlash(path.Clean("/"+name)))
	f, err := os.Open(fullName)
	if err != nil {
		return nil, mapDirOpenError(err, fullName)
	}
	return &slowReader{File: f}, nil
}

func mapDirOpenError(originalErr error, name string) error {
	if os.IsNotExist(originalErr) || os.IsPermission(originalErr) {
		return originalErr
	}

	parts := strings.Split(name, string(filepath.Separator))
	for i := range parts {
		if parts[i] == "" {
			continue
		}
		fi, err := os.Stat(strings.Join(parts[:i+1], string(filepath.Separator)))
		if err != nil {
			return originalErr
		}
		if !fi.IsDir() {
			return fs.ErrNotExist
		}
	}
	return originalErr
}
