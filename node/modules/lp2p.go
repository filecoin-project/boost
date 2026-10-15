package modules

import (
	"github.com/libp2p/go-libp2p"

	"github.com/filecoin-project/boost/build"

	"github.com/filecoin-project/lotus/node/modules/lp2p"
)

func UserAgent() (opts lp2p.Libp2pOpts, err error) {
	opts.Opts = append(opts.Opts, libp2p.UserAgent("boost-"+build.UserVersion()))
	return opts, nil
}
