package filters_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/filecoin-project/boost/cmd/booster-bitswap/filters"
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestPeerFilter(t *testing.T) {
	peer1, err := peer.Decode("Qma9T5YraSnpRDZqRR4krcSJabThc8nwZuJV3LercPHufi")
	require.NoError(t, err)
	peer2, err := peer.Decode("QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N")
	require.NoError(t, err)
	peer3, err := peer.Decode("QmcfgsJsMtx6qJb74akCw1M24X1zFwgGo11h1cuhwQjtJP")
	require.NoError(t, err)
	c, err := cid.Parse("QmWATWQ7fVPP2EFGu71UkfnqhYXDYH566qy47CnJDgvs8u")
	require.NoError(t, err)
	megabyte := uint64(1 << 20)
	totalRequestsInProgress := uint64(10)
	requestsInProgressForPeer1 := uint64(3)
	requestsInProgressForPeer2 := uint64(4)
	requestsInProgressForPeer3 := uint64(5)
	avgBandwidthPerSecond := 11 * megabyte
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
			name: "working bandwidth limit",
			response: `{
				"StorageProviderLimits": {
					"Bitswap": {
						"MaxBandwidth": "10mb"
					}
				}
			}`,
			fulfillPeer1: false,
			fulfillPeer2: false,
			fulfillPeer3: false,
		},
		{
			name: "working simultaneous request limit",
			response: `{
				"StorageProviderLimits": {
					"Bitswap": {
						"SimultaneousRequests": 10
					}
				}
			}`,
			fulfillPeer1: false,
			fulfillPeer2: false,
			fulfillPeer3: false,
		},
		{
			name: "working simultaneous request per peer limit",
			response: `{
				"StorageProviderLimits": {
					"Bitswap": {
						"SimultaneousRequestsPerPeer": 4
					}
				}
			}`,
			fulfillPeer1: true,
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
			expectedParseError: errors.New("parsing response: failed to parse peer ID: selected encoding not supported"),
		},
		{
			name: "bandwidth not parsable",
			response: `{
				"StorageProviderLimits": {
					"Bitswap": {
						"MaxBandwidth": "10mg"
					}
				}
			}`,
			expectedParseError: errors.New("parsing response: parsing 'MaxBandwidth': unhandled size name: mg"),
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			tbm := &testBandwidthMeasure{avgBandwidthPerSecond}
			pf := filters.NewRemoteConfigFilter(tbm)
			err := pf.ParseUpdate(strings.NewReader(testCase.response))
			if testCase.expectedParseError == nil {
				require.NoError(t, err)
				fulfilled, err := pf.FulfillRequest(peer1, c, filters.ServerState{
					TotalRequestsInProgress:   totalRequestsInProgress,
					RequestsInProgressForPeer: requestsInProgressForPeer1,
				})
				require.NoError(t, err)
				require.Equal(t, testCase.fulfillPeer1, fulfilled)
				fulfilled, err = pf.FulfillRequest(peer2, c, filters.ServerState{
					TotalRequestsInProgress:   totalRequestsInProgress,
					RequestsInProgressForPeer: requestsInProgressForPeer2,
				})
				require.NoError(t, err)
				require.Equal(t, testCase.fulfillPeer2, fulfilled)
				fulfilled, err = pf.FulfillRequest(peer3, c, filters.ServerState{
					TotalRequestsInProgress:   totalRequestsInProgress,
					RequestsInProgressForPeer: requestsInProgressForPeer3,
				})
				require.NoError(t, err)
				require.Equal(t, testCase.fulfillPeer3, fulfilled)
			} else {
				require.EqualError(t, err, testCase.expectedParseError.Error())
			}
		})
	}
}

type testBandwidthMeasure struct {
	avgBytesPerSecond uint64
}

func (tbm *testBandwidthMeasure) AvgBytesPerSecond() uint64 {
	return tbm.avgBytesPerSecond
}
