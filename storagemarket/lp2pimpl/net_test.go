package lp2pimpl

import (
	"bytes"
	"testing"

	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestDealParamsMissingFields verifies that when the client sends a v1.2.0
// DealParams struct, the server unmarshalls it into a v1.2.1 DealParams struct
// with all missing boolean fields set to false.
func TestDealParamsMissingFields(t *testing.T) {
	label, err := market.NewLabelFromString("label")
	require.NoError(t, err)
	dpv120 := types.DealParamsV120{
		DealUUID:  uuid.New(),
		IsOffline: false,
		ClientDealProposal: market.ClientDealProposal{
			Proposal: market.DealProposal{
				PieceCID:             testutil.GenerateCid(),
				Client:               address.TestAddress,
				Provider:             address.TestAddress2,
				Label:                label,
				StoragePricePerEpoch: abi.NewTokenAmount(1),
				ProviderCollateral:   abi.NewTokenAmount(2),
				ClientCollateral:     abi.NewTokenAmount(3),
			},
			ClientSignature: crypto.Signature{
				Type: crypto.SigTypeSecp256k1,
				Data: []byte("sig"),
			},
		},
		DealDataRoot: testutil.GenerateCid(),
		Transfer: types.Transfer{
			Type:     "http",
			ClientID: "1234",
			Params:   []byte("params"),
			Size:     5678,
		},
	}

	var buff bytes.Buffer
	err = dpv120.MarshalCBOR(&buff)
	require.NoError(t, err)

	var dpv121 types.DealParams
	err = dpv121.UnmarshalCBOR(&buff)
	require.NoError(t, err)

	// Expect all fields present in both v1.2.0 and v1.2.1 to match
	require.Equal(t, dpv120.DealUUID, dpv121.DealUUID)
	require.Equal(t, dpv120.IsOffline, dpv121.IsOffline)
	require.Equal(t, dpv120.ClientDealProposal, dpv121.ClientDealProposal)
	require.Equal(t, dpv120.DealDataRoot, dpv121.DealDataRoot)
	require.Equal(t, dpv120.Transfer, dpv121.Transfer)

	// Expect all boolean fields not present in v1.2.0 to be false
	// in v1.2.1
	require.False(t, dpv121.SkipIPNIAnnounce)
	require.False(t, dpv121.RemoveUnsealedCopy)
}
