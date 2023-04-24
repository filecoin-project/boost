package yugabyte

import (
	"fmt"
	"github.com/filecoin-project/boostd-data/svc/types"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"strings"
)

func normalizePieceCidError(pieceCid cid.Cid, err error) error {
	if err == nil {
		return nil
	}
	if isNotFoundErr(err) {
		return fmt.Errorf("piece %s: %s", pieceCid, types.ErrNotFound)
	}
	return err
}

func normalizeMultihashError(m mh.Multihash, err error) error {
	if err == nil {
		return nil
	}
	if isNotFoundErr(err) {
		return fmt.Errorf("multihash %s: %s", m, types.ErrNotFound)
	}
	return err
}

func isNotFoundErr(err error) bool {
	// TODO: is there some other way to know if the error is a not found error?
	// Check yugabyte cassandra driver docs
	return strings.Contains(err.Error(), "not found")
}
