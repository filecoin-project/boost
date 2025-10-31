package types

import (
	"encoding/json"
	"testing"

	"github.com/filecoin-project/boost/transport/types"
	"github.com/stretchr/testify/require"
)

func TestTransferHost(t *testing.T) {
	testCases := []struct {
		name     string
		xferType string
		url      string
		expected string
	}{{
		name:     "http",
		xferType: "http",
		url:      "http://foo.bar:1234",
		expected: "foo.bar:1234",
	}, {
		name:     "libp2p http",
		xferType: "libp2p",
		url:      "libp2p:///ip4/1.2.3.4/tcp/5678/p2p/Qma9T5YraSnpRDZqRR4krcSJabThc8nwZuJV3LercPHufi",
		expected: "1.2.3.4:5678",
	}, {
		name:     "libp2p quic",
		xferType: "libp2p",
		url:      "libp2p:///ip4/1.2.3.4/udp/5678/quic/p2p/Qma9T5YraSnpRDZqRR4krcSJabThc8nwZuJV3LercPHufi",
		expected: "1.2.3.4:5678",
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := types.HttpRequest{URL: tc.url}
			res, err := json.Marshal(req)
			require.NoError(t, err)

			xfer := Transfer{
				Type:   tc.xferType,
				Params: res,
			}
			h, err := xfer.Host()
			require.NoError(t, err)

			require.Equal(t, tc.expected, h)
		})
	}
}
