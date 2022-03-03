package cli

import (
	cliutil "github.com/filecoin-project/boost/cli/util"
	lcliutil "github.com/filecoin-project/lotus/cli/util"
)

var GetBoostAPI = cliutil.GetBoostAPI
var GetFullNodeAPI = lcliutil.GetFullNodeAPI

var ReqContext = cliutil.ReqContext
var DaemonContext = cliutil.DaemonContext
