package framework

import logging "github.com/ipfs/go-log/v2"

func SetLogLevel() {
	_ = logging.SetLogLevel("boosttest", "DEBUG")
	_ = logging.SetLogLevel("devnet", "DEBUG")
	_ = logging.SetLogLevel("boost", "DEBUG")
	_ = logging.SetLogLevel("provider", "DEBUG")
	_ = logging.SetLogLevel("http-transfer", "DEBUG")
	_ = logging.SetLogLevel("boost-provider", "DEBUG")
	_ = logging.SetLogLevel("storagemanager", "DEBUG")
	_ = logging.SetLogLevel("storageadapter", "DEBUG")
	_ = logging.SetLogLevel("messagepool", "WARN")
	_ = logging.SetLogLevel("consensus-common", "WARN")
	_ = logging.SetLogLevel("fxlog", "WARN")
}
