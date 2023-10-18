package yugabyte

import (
	"errors"
	"fmt"
	"github.com/filecoin-project/boost/extern/boostd-data/svc/types"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/yugabyte/gocql"
	"strings"
)

func normalizePieceCidError(pieceCid cid.Cid, err error) error {
	if err == nil {
		return nil
	}
	if isNotFoundErr(err) {
		return fmt.Errorf("piece %s: %w", pieceCid, types.ErrNotFound)
	}
	return err
}

func normalizeMultihashError(m mh.Multihash, err error) error {
	if err == nil {
		return nil
	}
	if isNotFoundErr(err) {
		return fmt.Errorf("multihash %s: %w", m, types.ErrNotFound)
	}
	return err
}

func isNotFoundErr(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, gocql.ErrNotFound) {
		return true
	}

	// Unfortunately it seems like the Cassandra driver doesn't always return
	// a specific not found error type, so we need to rely on string parsing
	return strings.Contains(strings.ToLower(err.Error()), "not found")
}
