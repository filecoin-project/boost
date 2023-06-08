package main

import (
	"fmt"
	"path/filepath"

	"github.com/filecoin-project/boost/node/config"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
)

var configCmd = &cli.Command{
	Name:  "config",
	Usage: "Display Boost config",
	Subcommands: []*cli.Command{
		configDefaultCmd,
		configUpdateCmd,
	},
}

var configDefaultCmd = &cli.Command{
	Name:  "default",
	Usage: "Print config file defaults",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "no-comment",
			Usage: "don't include comments in the output",
		},
	},
	Action: func(cctx *cli.Context) error {

		cb, err := config.ConfigUpdate(config.DefaultBoost(), nil, !cctx.Bool("no-comment"), false)
		if err != nil {
			return err
		}

		fmt.Println(string(cb))

		return nil
	},
}

var configUpdateCmd = &cli.Command{
	Name:  "updated",
	Usage: "Print config file with updates (changes from the default config file)",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "no-comment",
			Usage: "don't include commented out default values in the output",
		},
		&cli.BoolFlag{
			Name:  "diff",
			Usage: "only display values different from default",
		},
	},
	Action: func(cctx *cli.Context) error {
		path, err := homedir.Expand(cctx.String(FlagBoostRepo))
		if err != nil {
			return err
		}

		configPath := filepath.Join(path, "config.toml")

		cfgNode, err := config.FromFile(configPath, config.DefaultBoost())
		if err != nil {
			return err
		}

		output, err := config.ConfigUpdate(cfgNode, config.DefaultBoost(), !cctx.Bool("no-comment"), cctx.Bool("diff"))
		if err != nil {
			return err
		}

		fmt.Print(string(output))
		return nil
	},
}
