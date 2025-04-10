package main

import (
	"os"

	"github.com/filecoin-project/boost/build"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("migrate-curio")

const (
	FlagBoostRepo = "boost-repo"
)

var FlagRepo = &cli.StringFlag{
	Name:    FlagBoostRepo,
	EnvVars: []string{"BOOST_PATH"},
	Usage:   "boost repo path",
	Value:   "~/.boost",
}

var IsVeryVerbose bool

var FlagVeryVerbose = &cli.BoolFlag{
	Name:        "vv",
	Usage:       "enables very verbose mode, useful for debugging the CLI",
	Destination: &IsVeryVerbose,
}

func main() {
	app := &cli.App{
		Name:                 "migrate-curio",
		Usage:                "Migrate boost to Curio",
		EnableBashCompletion: true,
		Version:              build.UserVersion(),
		Flags: []cli.Flag{
			FlagRepo,
			FlagVeryVerbose,
			&cli.StringFlag{
				Name:    "db-host",
				EnvVars: []string{"CURIO_DB_HOST", "CURIO_HARMONYDB_HOSTS"},
				Usage:   "Command separated list of hostnames for yugabyte cluster",
				Value:   "127.0.0.1",
			},
			&cli.StringFlag{
				Name:    "db-name",
				EnvVars: []string{"CURIO_DB_NAME", "CURIO_HARMONYDB_NAME"},
				Value:   "yugabyte",
			},
			&cli.StringFlag{
				Name:    "db-user",
				EnvVars: []string{"CURIO_DB_USER", "CURIO_HARMONYDB_USERNAME"},
				Value:   "yugabyte",
			},
			&cli.StringFlag{
				Name:    "db-password",
				EnvVars: []string{"CURIO_DB_PASSWORD", "CURIO_HARMONYDB_PASSWORD"},
				Value:   "yugabyte",
			},
			&cli.StringFlag{
				Name:    "db-port",
				EnvVars: []string{"CURIO_DB_PORT", "CURIO_HARMONYDB_PORT"},
				Value:   "5433",
			},
		},
		Commands: []*cli.Command{
			migrateCmd,
			cleanupLIDCmd,
		},
	}
	app.Setup()

	if err := app.Run(os.Args); err != nil {
		os.Stderr.WriteString("Error: " + err.Error() + "\n")
	}
}

func before(cctx *cli.Context) error {
	_ = logging.SetLogLevel("migrate-curio", "INFO")

	if IsVeryVerbose {
		_ = logging.SetLogLevel("migrate-curio", "DEBUG")
	}

	return nil
}
