package main

import "github.com/urfave/cli/v2"

var flagMiner = &cli.StringFlag{
	Name:    "miner",
	Aliases: []string{"m"},
}

var flagMinerRequired = &cli.StringFlag{
	Name:     flagMiner.Name,
	Aliases:  flagMiner.Aliases,
	Required: true,
}

var flagMiners = &cli.StringSliceFlag{
	Name:    "miners",
	Aliases: []string{"miner", "m"},
}

var flagMinersRequired = &cli.StringSliceFlag{
	Name:     flagMiners.Name,
	Aliases:  flagMiners.Aliases,
	Required: true,
}

var flagVerified = &cli.BoolFlag{
	Name: "verified",
}

var removeUnsealed = &cli.BoolFlag{
	Name: "remove-unsealed",
}

var flagOutput = &cli.StringFlag{
	Name:    "output",
	Aliases: []string{"o"},
}

var flagNetwork = &cli.StringFlag{
	Name:        "network",
	Aliases:     []string{"n"},
	Usage:       "which network to retrieve from [fil|ipfs|auto]",
	DefaultText: NetworkAuto,
	Value:       NetworkAuto,
}

var flagCar = &cli.BoolFlag{
	Name: "car",
}

var flagDealUUID = &cli.StringFlag{
	Name: "deal-uuid",
}

const (
	NetworkFIL  = "fil"
	NetworkIPFS = "ipfs"
	NetworkAuto = "auto"
)

var flagDmPathSel = &cli.StringFlag{
	Name:  "datamodel-path-selector",
	Usage: "a rudimentary (DM-level-only) text-path selector, allowing for sub-selection within a deal",
}
