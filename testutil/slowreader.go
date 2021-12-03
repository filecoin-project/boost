package testutil

import (
	"errors"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

const slowChunkSize = 256
const slowChunkDelay = 1 * time.Millisecond

type SlowReader struct {
	*os.File
}

func (r *SlowReader) Read(p []byte) (int, error) {
	bz := make([]byte, slowChunkSize)
	time.Sleep(slowChunkDelay)
	n, err := r.File.Read(bz)
	copy(p, bz)
	return n, err
}

type SlowFileOpener struct {
	Dir string
}

func (s *SlowFileOpener) Open(name string) (http.File, error) {
	if filepath.Separator != '/' && strings.ContainsRune(name, filepath.Separator) {
		return nil, errors.New("http: invalid character in file path")
	}
	dir := s.Dir
	if dir == "" {
		dir = "."
	}
	fullName := filepath.Join(dir, filepath.FromSlash(path.Clean("/"+name)))
	f, err := os.Open(fullName)
	if err != nil {
		return nil, mapDirOpenError(err, fullName)
	}
	return &SlowReader{File: f}, nil
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
