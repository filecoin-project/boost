package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-state-types/network"

	"github.com/filecoin-project/boost/storagemarket"
)

// TestCheckDirectDealSupported pins the boundary and the wording. The last
// version that still has datacap is nv28, everything from nv29 on is refused,
// and the refusal is the same sentence the provider gives in Accept - a
// client-side check that explains the rejection differently from the server
// just moves the confusion rather than removing it.
func TestCheckDirectDealSupported(t *testing.T) {
	tests := []struct {
		name      string
		nv        network.Version
		supported bool
	}{
		{name: "nv27", nv: network.Version27, supported: true},
		{name: "nv28, the last version with datacap", nv: network.Version28, supported: true},
		{name: "nv29, the version FIP-0118 lands in", nv: network.Version29, supported: false},
		{name: "a version past nv29", nv: network.Version29 + 1, supported: false},
		{name: "VersionMax", nv: network.VersionMax, supported: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := checkDirectDealSupported(tc.nv)
			if tc.supported {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.EqualError(t, err, storagemarket.DirectDealRejectionAtNv29,
				"the client should turn the deal away with the provider's own reason, not a paraphrase")
		})
	}
}
