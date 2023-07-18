package main

import (
	"testing"

	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestMultiaddrToNative(t *testing.T) {
	testCases := []struct {
		name     string
		proto    string
		ma       string
		expected string
	}{{
		name:     "http",
		proto:    "http",
		ma:       "/dns/foo.com/http",
		expected: "http://foo.com",
	}, {
		name:     "http IP 4 address",
		proto:    "http",
		ma:       "/ip4/192.168.0.1/tcp/80/http",
		expected: "http://192.168.0.1:80",
	}, {
		name:     "https",
		proto:    "https",
		ma:       "/dns/foo.com/tcp/443/https",
		expected: "https://foo.com:443",
	}, {
		name:     "unknown protocol",
		proto:    "fancynewproto",
		ma:       "/dns/foo.com/tcp/80/http",
		expected: "",
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ma, err := multiaddr.NewMultiaddr(tc.ma)
			require.NoError(t, err)
			res := multiaddrToNative(tc.proto, ma)
			require.Equal(t, tc.expected, res)
		})
	}
}
