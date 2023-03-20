package modules

import (
	"github.com/filecoin-project/boost/build"
	"github.com/filecoin-project/lotus/node/modules/lp2p"
	"github.com/libp2p/go-libp2p"
)

func simpleOpt(opt libp2p.Option) func() (opts lp2p.Libp2pOpts, err error) {
	return func() (opts lp2p.Libp2pOpts, err error) {
		opts.Opts = append(opts.Opts, opt)
		return
	}
}

var UserAgent = simpleOpt(libp2p.UserAgent("boost-" + build.UserVersion()))
