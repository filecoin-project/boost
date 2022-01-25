package httptransport

import (
	"testing"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestParseUrl(t *testing.T) {
	tests := []struct {
		name   string
		url    string
		expect *transportUrl
	}{{
		name: "http url",
		url:  "http://www.test.com/path",
		expect: &transportUrl{
			URL:    "http://www.test.com/path",
			Scheme: "http",
		},
	}, {
		name: "https url",
		url:  "https://www.test.com/path",
		expect: &transportUrl{
			URL:    "https://www.test.com/path",
			Scheme: "https",
		},
	}, {
		name: "ip4 libp2p url",
		url:  "libp2p:///ip4/104.131.131.82/tcp/4001/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
		expect: &transportUrl{
			Scheme:    libp2pScheme,
			Host:      "104.131.131.82:4001",
			URL:       libp2pScheme + "://QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
			PeerID:    peer.ID("QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"),
			Multiaddr: MultiAddrMustParse(t, "/ip4/104.131.131.82/tcp/4001"),
		},
	}, {
		name: "dns libp2p url",
		url:  "libp2p:///dnsaddr/bootstrap.libp2p.io/ipfs/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
		expect: &transportUrl{
			Scheme:    libp2pScheme,
			Host:      "bootstrap.libp2p.io",
			URL:       libp2pScheme + "://QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
			PeerID:    peer.ID("QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb"),
			Multiaddr: MultiAddrMustParse(t, "/dnsaddr/bootstrap.libp2p.io"),
		},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseUrl(tt.url)
			require.NoError(t, err)
			if tt.expect.Multiaddr != nil {
				require.Equal(t, tt.expect.Multiaddr.String(), got.Multiaddr.String())
			}
			require.Equal(t, tt.expect, got)
		})
	}
}

func MultiAddrMustParse(t *testing.T, s string) multiaddr.Multiaddr {
	ma, err := multiaddr.NewMultiaddr(s)
	require.NoError(t, err)
	return ma
}
