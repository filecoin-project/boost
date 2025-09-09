package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestToHttpMultiaddr(t *testing.T) {
	tcs := []struct {
		hostname  string
		port      int
		expected  string
		expectErr bool
	}{{
		hostname: "192.168.1.1",
		port:     1234,
		expected: "/ip4/192.168.1.1/tcp/1234/http",
	}, {
		hostname: "2001:db8::68",
		port:     1234,
		expected: "/ip6/2001:db8::68/tcp/1234/http",
	}, {
		hostname: "example.com",
		port:     1234,
		expected: "/dns/example.com/tcp/1234/http",
	}, {
		hostname:  "",
		port:      1234,
		expected:  "",
		expectErr: true,
	}}

	for _, tc := range tcs {
		t.Run("", func(t *testing.T) {
			ma, err := ToHttpMultiaddr(tc.hostname, tc.port)
			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, ma.String())
			}
		})
	}
}
