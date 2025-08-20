package bitswap

import (
	"github.com/ipfs/boxo/bitswap/network/bsnet"
	"github.com/libp2p/go-libp2p/core/protocol"
)

var Protocols = []protocol.ID{
	bsnet.ProtocolBitswap,
	bsnet.ProtocolBitswapNoVers,
	bsnet.ProtocolBitswapOneOne,
	bsnet.ProtocolBitswapOneZero,
}

var ProtocolStrings = []string{}

func init() {
	for _, p := range Protocols {
		ProtocolStrings = append(ProtocolStrings, string(p))
	}
}
