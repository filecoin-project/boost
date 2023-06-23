package cmd

import "github.com/urfave/cli/v2"

var FlagRepo = &cli.StringFlag{
	Name:    "repo",
	Usage:   "repo directory for Boost client",
	Value:   "~/.boost-client",
	EnvVars: []string{"BOOST_CLIENT_REPO"},
}

var FlagJson = &cli.BoolFlag{
	Name:  "json",
	Usage: "output results in json format",
	Value: false,
}
