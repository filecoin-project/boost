package httptransport

import (
	"testing"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestParseUrl(t *testing.T) {
	tests := []struct {
		name        string
		url         string
		expect      *transportUrl
		expectError bool
	}{{
		name: "http url",
		url:  "http://www.test.com/path",
		expect: &transportUrl{
			url:    "http://www.test.com/path",
			scheme: "http",
		},
	}, {
		name: "https url",
		url:  "https://www.test.com/path",
		expect: &transportUrl{
			url:    "https://www.test.com/path",
			scheme: "https",
		},
	}, {
		name:        "bad url",
		url:         "badurl",
		expectError: true,
	}, {
		name: "ip4 libp2p url",
		url:  "libp2p:///ip4/104.131.131.82/tcp/4001/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
		expect: &transportUrl{
			scheme:    libp2pScheme,
			url:       libp2pScheme + "://QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
			peerID:    peerMustDecode(t, "QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"),
			multiaddr: multiAddrMustParse(t, "/ip4/104.131.131.82/tcp/4001"),
		},
	}, {
		name: "dns libp2p url",
		url:  "libp2p:///dnsaddr/bootstrap.libp2p.io/ipfs/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
		expect: &transportUrl{
			scheme:    libp2pScheme,
			url:       libp2pScheme + "://QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
			peerID:    peerMustDecode(t, "QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb"),
			multiaddr: multiAddrMustParse(t, "/dnsaddr/bootstrap.libp2p.io"),
		},
	}, {
		name:        "libp2p url no peer ID",
		url:         "libp2p:///ip4/104.131.131.82/tcp/4001",
		expectError: true,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseUrl(tt.url)
			if tt.expectError {
				require.Error(t, err)
				return
			} else {
				require.NoError(t, err)
			}
			if tt.expect.multiaddr != nil {
				require.Equal(t, tt.expect.multiaddr.String(), got.multiaddr.String())
			}
			require.Equal(t, tt.expect, got)
		})
	}
}

func peerMustDecode(t *testing.T, s string) peer.ID {
	pid, err := peer.Decode(s)
	require.NoError(t, err)
	return pid
}

func multiAddrMustParse(t *testing.T, s string) multiaddr.Multiaddr {
	ma, err := multiaddr.NewMultiaddr(s)
	require.NoError(t, err)
	return ma
}
