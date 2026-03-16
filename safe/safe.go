package safe

import (
	"runtime/debug"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/network"
)

var log = logging.Logger("safe")

func Handle(h network.StreamHandler) network.StreamHandler {
	return func(s network.Stream) {
		defer func() {
			if r := recover(); r != nil {
				log.Errorw("panic in stream handler", "panic", r, "stack", string(debug.Stack()))
				if s != nil {
					_ = s.Reset()
				}
			}
		}()
		h(s)
	}
}
