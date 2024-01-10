package safe

import (
	"runtime/debug"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/network"
)

var log = logging.Logger("safe")

func Handle(h network.StreamHandler) network.StreamHandler {
	defer func() {
		if r := recover(); r != nil {
			log.Error("panic occurred", "stack", debug.Stack())
		}
	}()

	return h
}
