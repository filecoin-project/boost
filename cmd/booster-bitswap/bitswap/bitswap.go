package bitswap

import (
	"github.com/ipfs/boxo/bitswap/network"
	"github.com/libp2p/go-libp2p/core/protocol"
)

var Protocols = []protocol.ID{
	network.ProtocolBitswap,
	network.ProtocolBitswapNoVers,
	network.ProtocolBitswapOneOne,
	network.ProtocolBitswapOneZero,
}

var ProtocolStrings = []string{}

func init() {
	for _, p := range Protocols {
		ProtocolStrings = append(ProtocolStrings, string(p))
	}
}
