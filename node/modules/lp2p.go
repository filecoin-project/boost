package modules

import (
	"context"
	"strings"

	"github.com/filecoin-project/boost/build"
	"github.com/filecoin-project/boost/storagemarket/sealingpipeline"
	"github.com/filecoin-project/lotus/node/modules/lp2p"
	"github.com/libp2p/go-libp2p"
)

func UserAgent(sps sealingpipeline.API) (opts lp2p.Libp2pOpts, err error) {
	opt := libp2p.UserAgent("boost-" + build.UserVersion())

	v, err := sps.Version(context.Background())
	if err != nil {
		return opts, err
	}

	if strings.Contains(v.String(), "curio") {
		opt = libp2p.UserAgent("boost-curio-" + build.UserVersion())
	}

	opts.Opts = append(opts.Opts, opt)
	return opts, nil
}
