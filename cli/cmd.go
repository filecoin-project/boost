package cli

import (
	cliutil "github.com/filecoin-project/boost/cli/util"
	scliutil "github.com/filecoin-project/boost/extern/boostd-data/shared/cliutil"
	lcliutil "github.com/filecoin-project/lotus/cli/util"
)

var GetBoostAPI = cliutil.GetBoostAPI
var GetFullNodeAPI = lcliutil.GetFullNodeAPI

var ReqContext = scliutil.ReqContext
var DaemonContext = scliutil.DaemonContext
