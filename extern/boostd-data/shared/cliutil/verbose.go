package cliutil

import "github.com/urfave/cli/v2"

// IsVerbose is a global var signalling if the CLI is running in
// verbose mode or not (default: false).
var IsVerbose bool

// FlagVerbose enables verbose mode, which shows verbose information about
// operations invoked in the CLI. It should be included as a flag on the
// top-level command (e.g. boost -v).
var FlagVerbose = &cli.BoolFlag{
	Name:        "v",
	Usage:       "enables verbose mode, outputs more logging",
	Destination: &IsVerbose,
}

// IsVeryVerbose is a global var signalling if the CLI is running in very
// verbose mode or not (default: false).
var IsVeryVerbose bool

// FlagVeryVerbose enables very verbose mode, which is useful when debugging
// the CLI itself. It should be included as a flag on the top-level command
// (e.g. boost -vv).
var FlagVeryVerbose = &cli.BoolFlag{
	Name:        "vv",
	Usage:       "enables very verbose mode, useful for debugging the CLI",
	Destination: &IsVeryVerbose,
}
