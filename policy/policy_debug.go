//go:build debug
// +build debug

package policy

import "github.com/filecoin-project/go-state-types/abi"

var (
	MaxProviderCollateral = abi.NewTokenAmount(0)
)
