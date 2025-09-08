package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAgentVersion(t *testing.T) {
	testCases := map[string]string{
		"lotus-1.20.0+gzbt+git.cddeab21d.dirty": "lotus-1.20.0+gzbt",
		"boost-1.6.1+git.4931e18":               "boost-1.6.1",
		"boost-1.6.2-rc1+git.0b5cf47.dirty":     "boost-1.6.2-rc1",
		"lotus-1.20.0":                          "lotus-1.20.0",
	}
	for agentVersion, exp := range testCases {
		agentVersionShort := shortAgentVersion(agentVersion)
		require.Equal(t, exp, agentVersionShort)
	}
}
