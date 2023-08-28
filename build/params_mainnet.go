//go:build !calibnet && !debug && !2k
// +build !calibnet,!debug,!2k

package build

import (
	"github.com/filecoin-project/go-address"
)

func init() {
	SetAddressNetwork(address.Mainnet)
	BuildType = BuildMainnet
}
