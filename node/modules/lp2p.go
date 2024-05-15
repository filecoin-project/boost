package modules

import (
	"github.com/filecoin-project/boost/build"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/lotus/node/modules/lp2p"
	"github.com/libp2p/go-libp2p"
)

func UserAgent(cfg *config.Boost) func() (opts lp2p.Libp2pOpts, err error) {

	opt := libp2p.UserAgent("boost-" + build.UserVersion())

	if cfg.Curio.Enabled {
		opt = libp2p.UserAgent("boost-curio-" + build.UserVersion())
	}

	return func() (opts lp2p.Libp2pOpts, err error) {
		opts.Opts = append(opts.Opts, opt)
		return
	}
}
