package cli

import (
	"github.com/filecoin-project/boost/cli/ctxutil"
	cliutil "github.com/filecoin-project/boost/cli/util"
	lcliutil "github.com/filecoin-project/lotus/cli/util"
)

var GetBoostAPI = cliutil.GetBoostAPI
var GetFullNodeAPI = lcliutil.GetFullNodeAPI

var ReqContext = ctxutil.ReqContext
var DaemonContext = ctxutil.DaemonContext
