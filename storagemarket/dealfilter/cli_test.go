package dealfilter

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-state-types/network"

	"github.com/filecoin-project/boost/retrievalmarket/types/legacyretrievaltypes"
	"github.com/filecoin-project/boost/storagemarket/types"
)

// TestCliStorageDealFilterDocument pins the document an external filter actually
// receives: the JSON is the whole contract, so it is read back out of a real
// invocation rather than off the Go struct.
func TestCliStorageDealFilterDocument(t *testing.T) {
	accept, reason, err := CliStorageDealFilter(
		// NetworkVersion is what a filter sorting FIL+ traffic branches on past
		// nv29; without it the script cannot tell the chain moving from the client
		// dropping datacap. One process, because the document arrives on stdin.
		`awk '/"FormatVersion": "2.3.0"/{f=1} /"NetworkVersion": 29/{v=1} /"DealType": "storage"/{t=1} END{exit !(f&&v&&t)}'`,
	)(context.Background(), DealFilterParams{
		DealParams:     types.DealParams{},
		NetworkVersion: network.Version29,
	})

	require.NoError(t, err)
	require.True(t, accept, "reason: %s", reason)
}

// TestCliRetrievalDealFilterVersionNotBumped: the retrieval document did not
// change, so it must not claim to have.
func TestCliRetrievalDealFilterVersionNotBumped(t *testing.T) {
	accept, reason, err := CliRetrievalDealFilter(
		`awk '/"FormatVersion": "2.2.0"/{f=1} /"NetworkVersion"/{v=1} END{exit !(f&&!v)}'`,
	)(context.Background(), legacyretrievaltypes.ProviderDealState{})

	require.NoError(t, err)
	require.True(t, accept, "reason: %s", reason)
}

// TestCliStorageDealFilterRejectsMissingNetworkVersion covers the guard on the
// way into the external process: a zero version reads to a filter as a very old
// chain, so it must not reach the script at all.
func TestCliStorageDealFilterRejectsMissingNetworkVersion(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "ran")

	accept, reason, err := CliStorageDealFilter("touch "+marker)(context.Background(), DealFilterParams{})

	require.Error(t, err)
	require.False(t, accept)
	require.Contains(t, reason, "network version missing")
	require.NoFileExists(t, marker, "the external filter must not run with no network version")
}
