package main

import (
	"math/rand"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/filecoin-project/boost/extern/boostd-data/shared/cliutil"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("bench")

var metricsEnabled bool

func main() {
	app := &cli.App{
		Name:                 "bench",
		Usage:                "Benchmark LID databases",
		EnableBashCompletion: true,
		Flags: []cli.Flag{
			cliutil.FlagVeryVerbose,
			&cli.BoolFlag{
				Name:        "metrics",
				Usage:       "enables / disables influxdb metrics",
				Value:       true,
				Destination: &metricsEnabled,
			},
		},
		Commands: []*cli.Command{
			cassandraCmd,
			//foundationCmd,
			postgresCmd,
		},
	}
	app.Setup()

	rand.Seed(time.Now().UnixNano())
	if err := app.Run(os.Args); err != nil {
		log.Errorf("Error: %s", err.Error())
		os.Exit(1)
	}

	if metricsEnabled {
		time.Sleep(11 * time.Second) // 10+1 because of influxdb reporter
	}
}

func before(cctx *cli.Context) error {
	_ = logging.SetLogLevel("bench", "info")

	if cliutil.IsVeryVerbose {
		_ = logging.SetLogLevel("bench", "debug")
	}

	if metricsEnabled {
		metricsSetup()
	}

	return nil
}

func metricsSetup() {
	metrics.Enabled = true

	hostname, _ := os.Hostname()
	tags := make(map[string]string)
	tags["host"] = hostname

	endpoint := "http://10.14.1.64:8086"
	username := "admin"
	password := "admin"
	database := "metrics"
	namespace := ""

	go InfluxDBWithTags(metrics.DefaultRegistry, 10*time.Second, endpoint, database, username, password, namespace, tags)
	log.Info("setting up influxdb exporter")
}
