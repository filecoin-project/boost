package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"strings"

	"github.com/filecoin-project/boost/api"
	bclient "github.com/filecoin-project/boost/api/client"
	cliutil "github.com/filecoin-project/boost/cli/util"
	"github.com/filecoin-project/boost/cmd/booster-bitswap/remoteblockstore"
	"github.com/filecoin-project/go-jsonrpc"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/storage/sealer"
	"github.com/urfave/cli/v2"
)

var runCmd = &cli.Command{
	Name:   "run",
	Usage:  "Start a booster-bitswap process",
	Before: before,
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "pprof",
			Usage: "run pprof web server on localhost:6070",
		},
		&cli.UintFlag{
			Name:  "port",
			Usage: "the port to listen for bitswap requests on",
			Value: 8888,
		},
		&cli.StringFlag{
			Name:     "api-boost",
			Usage:    "the endpoint for the boost API",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "api-fullnode",
			Usage:    "the endpoint for the full node API",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "api-sealer",
			Usage:    "the endpoint for the sealer API",
			Required: true,
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.Bool("pprof") {
			go func() {
				err := http.ListenAndServe("localhost:6070", nil)
				if err != nil {
					log.Error(err)
				}
			}()
		}

		// Connect to the Boost API
		ctx := lcli.ReqContext(cctx)
		boostAPIInfo := cctx.String("api-boost")
		bapi, bcloser, err := getBoostAPI(ctx, boostAPIInfo)
		if err != nil {
			return fmt.Errorf("getting boost API: %w", err)
		}
		defer bcloser()

		remoteStore := remoteblockstore.NewRemoteBlockstore(bapi)
		// Create the server API
		server := NewBitswapServer(cctx.String("base-path"), cctx.Int("port"), remoteStore)

		// Start the server
		log.Infof("Starting booster-http node on port %d with base path '%s'",
			cctx.Int("port"), cctx.String("base-path"))
		err = server.Start(ctx)
		if err != nil {
			return err
		}
		// Monitor for shutdown.
		<-ctx.Done()

		log.Info("Shutting down...")

		err = server.Stop()
		if err != nil {
			return err
		}
		log.Info("Graceful shutdown successful")

		// Sync all loggers.
		_ = log.Sync() //nolint:errcheck

		return nil
	},
}

func storageAuthWithURL(apiInfo string) (sealer.StorageAuth, error) {
	s := strings.Split(apiInfo, ":")
	if len(s) != 2 {
		return nil, errors.New("unexpected format of `apiInfo`")
	}
	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+s[0])
	return sealer.StorageAuth(headers), nil
}

func getBoostAPI(ctx context.Context, ai string) (api.Boost, jsonrpc.ClientCloser, error) {
	ai = strings.TrimPrefix(strings.TrimSpace(ai), "BOOST_API_INFO=")
	info := cliutil.ParseApiInfo(ai)
	addr, err := info.DialArgs("v0")
	if err != nil {
		return nil, nil, fmt.Errorf("could not get DialArgs: %w", err)
	}

	log.Infof("Using boost API at %s", addr)
	api, closer, err := bclient.NewBoostRPCV0(ctx, addr, info.AuthHeader())
	if err != nil {
		return nil, nil, fmt.Errorf("creating full node service API: %w", err)
	}

	return api, closer, nil
}
