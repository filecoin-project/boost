//go:build debug || 2k
// +build debug 2k

package build

import (
	"github.com/filecoin-project/go-address"
)

func init() {
	SetAddressNetwork(address.Testnet)
	BuildType = BuildDebug
}
