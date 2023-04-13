package filters_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/filecoin-project/boost/cmd/lib/filters"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestConfigFilter(t *testing.T) {
	peer1, err := peer.Decode("Qma9T5YraSnpRDZqRR4krcSJabThc8nwZuJV3LercPHufi")
	require.NoError(t, err)
	peer2, err := peer.Decode("QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N")
	require.NoError(t, err)
	peer3, err := peer.Decode("QmcfgsJsMtx6qJb74akCw1M24X1zFwgGo11h1cuhwQjtJP")
	require.NoError(t, err)
	c, err := cid.Parse("QmWATWQ7fVPP2EFGu71UkfnqhYXDYH566qy47CnJDgvs8u")
	require.NoError(t, err)
	testCases := []struct {
		name               string
		response           string
		expectedParseError error
		fulfillPeer1       bool
		fulfillPeer2       bool
		fulfillPeer3       bool
	}{
		{
			name:         "default behavior",
			response:     `{}`,
			fulfillPeer1: true,
			fulfillPeer2: true,
			fulfillPeer3: true,
		},
		{
			name: "working allow list",
			response: `{
				"AllowDenyList": {
						"Type": "allowlist", 
						"PeerIDs": ["Qma9T5YraSnpRDZqRR4krcSJabThc8nwZuJV3LercPHufi", "QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N"]
				}
			}`,
			fulfillPeer1: true,
			fulfillPeer2: true,
			fulfillPeer3: false,
		},
		{
			name: "working deny list",
			response: `{
				"AllowDenyList": {
						"Type": "denylist", 
						"PeerIDs": ["Qma9T5YraSnpRDZqRR4krcSJabThc8nwZuJV3LercPHufi", "QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N"]
				}
			}`,
			fulfillPeer1: false,
			fulfillPeer2: false,
			fulfillPeer3: true,
		},
		{
			name: "maintenance mode",
			response: `{
				"UnderMaintenance": true
			}`,
			fulfillPeer1: false,
			fulfillPeer2: false,
			fulfillPeer3: false,
		},
		{
			name: "improperly formatted json",
			response: `s{
				"AllowDenyList": {
						"Type": "denylist", 
						"PeerIDs": ["Qma9T5YraSnpRDZqRR4krcSJabThc8nwZuJV3LercPHufi", "QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N"]
				}
			}`,
			expectedParseError: errors.New("parsing response: invalid character 's' looking for beginning of value"),
		},
		{
			name: "improper list type",
			response: `{
				"AllowDenyList": {
						"Type": "blacklist", 
						"PeerIDs": ["Qma9T5YraSnpRDZqRR4krcSJabThc8nwZuJV3LercPHufi", "QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N"]
				}
			}`,
			expectedParseError: errors.New("parsing response: 'Type' must be either 'allowlist' or 'denylist'"),
		},
		{
			name: "improper peer id",
			response: `{
				"AllowDenyList": {
						"Type": "denylist", 
						"PeerIDs": ["apples", "QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N"]
				}
			}`,
			expectedParseError: errors.New("parsing response: failed to parse peer ID: invalid cid: selected encoding not supported"),
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {

			pf := filters.NewConfigFilter()
			err := pf.ParseUpdate(strings.NewReader(testCase.response))
			if testCase.expectedParseError == nil {
				require.NoError(t, err)
				fulfilled, err := pf.FulfillRequest(peer1, c)
				require.NoError(t, err)
				require.Equal(t, testCase.fulfillPeer1, fulfilled)
				fulfilled, err = pf.FulfillRequest(peer2, c)
				require.NoError(t, err)
				require.Equal(t, testCase.fulfillPeer2, fulfilled)
				fulfilled, err = pf.FulfillRequest(peer3, c)
				require.NoError(t, err)
				require.Equal(t, testCase.fulfillPeer3, fulfilled)
			} else {
				require.EqualError(t, err, testCase.expectedParseError.Error())
			}
		})
	}
}
