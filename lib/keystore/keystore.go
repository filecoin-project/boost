package keystore

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/filecoin-project/lotus/chain/types"
	"github.com/whyrusleeping/base32"
)

type DiskKeyStore struct {
	path string
}

func OpenOrInitKeystore(p string) (*DiskKeyStore, error) {
	if _, err := os.Stat(p); err == nil {
		return &DiskKeyStore{p}, nil
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	if err := os.Mkdir(p, 0700); err != nil {
		return nil, err
	}

	return &DiskKeyStore{p}, nil
}

var kstrPermissionMsg = "permissions of key: '%s' are too relaxed, " +
	"required: 0600, got: %#o"

// List lists all the keys stored in the KeyStore
func (fsr *DiskKeyStore) List() ([]string, error) {

	dir, err := os.Open(fsr.path)
	if err != nil {
		return nil, fmt.Errorf("opening dir to list keystore: %w", err)
	}
	defer dir.Close() //nolint:errcheck
	files, err := dir.Readdir(-1)
	if err != nil {
		return nil, fmt.Errorf("reading keystore dir: %w", err)
	}
	keys := make([]string, 0, len(files))
	for _, f := range files {
		if f.Mode()&0077 != 0 {
			return nil, fmt.Errorf(kstrPermissionMsg, f.Name(), f.Mode())
		}
		name, err := base32.RawStdEncoding.DecodeString(f.Name())
		if err != nil {
			return nil, fmt.Errorf("decoding key: '%s': %w", f.Name(), err)
		}
		keys = append(keys, string(name))
	}
	return keys, nil
}

// Get gets a key out of keystore and returns types.KeyInfo coresponding to named key
func (fsr *DiskKeyStore) Get(name string) (types.KeyInfo, error) {

	encName := base32.RawStdEncoding.EncodeToString([]byte(name))
	keyPath := filepath.Join(fsr.path, encName)

	fstat, err := os.Stat(keyPath)
	if os.IsNotExist(err) {
		return types.KeyInfo{}, fmt.Errorf("opening key '%s': %w", name, types.ErrKeyInfoNotFound)
	} else if err != nil {
		return types.KeyInfo{}, fmt.Errorf("opening key '%s': %w", name, err)
	}

	if fstat.Mode()&0077 != 0 {
		return types.KeyInfo{}, fmt.Errorf(kstrPermissionMsg, name, fstat.Mode())
	}

	file, err := os.Open(keyPath)
	if err != nil {
		return types.KeyInfo{}, fmt.Errorf("opening key '%s': %w", name, err)
	}
	defer file.Close() //nolint: errcheck // read only op

	data, err := io.ReadAll(file)
	if err != nil {
		return types.KeyInfo{}, fmt.Errorf("reading key '%s': %w", name, err)
	}

	var res types.KeyInfo
	err = json.Unmarshal(data, &res)
	if err != nil {
		return types.KeyInfo{}, fmt.Errorf("decoding key '%s': %w", name, err)
	}

	return res, nil
}

// Put saves key info under given name
func (fsr *DiskKeyStore) Put(name string, info types.KeyInfo) error {

	encName := base32.RawStdEncoding.EncodeToString([]byte(name))
	keyPath := filepath.Join(fsr.path, encName)

	_, err := os.Stat(keyPath)
	if err == nil {
		return fmt.Errorf("checking key before put '%s': %w", name, types.ErrKeyExists)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("checking key before put '%s': %w", name, err)
	}

	keyData, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("encoding key '%s': %w", name, err)
	}

	err = os.WriteFile(keyPath, keyData, 0600)
	if err != nil {
		return fmt.Errorf("writing key '%s': %w", name, err)
	}
	return nil
}

func (fsr *DiskKeyStore) Delete(name string) error {

	encName := base32.RawStdEncoding.EncodeToString([]byte(name))
	keyPath := filepath.Join(fsr.path, encName)

	_, err := os.Stat(keyPath)
	if os.IsNotExist(err) {
		return fmt.Errorf("checking key before delete '%s': %w", name, types.ErrKeyInfoNotFound)
	} else if err != nil {
		return fmt.Errorf("checking key before delete '%s': %w", name, err)
	}

	err = os.Remove(keyPath)
	if err != nil {
		return fmt.Errorf("deleting key '%s': %w", name, err)
	}
	return nil
}
