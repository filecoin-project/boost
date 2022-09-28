package main

import (
	"context"
	"fmt"
	"github.com/filecoin-project/boost/node/bstoreserver"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"io/ioutil"
	"net/http"
	"net/http/httptrace"
	_ "net/http/pprof"
	"strings"
	"time"

	"github.com/filecoin-project/boost/api"
	bclient "github.com/filecoin-project/boost/api/client"
	cliutil "github.com/filecoin-project/boost/cli/util"
	"github.com/filecoin-project/boost/cmd/booster-bitswap/remoteblockstore"
	"github.com/filecoin-project/boost/tracing"
	"github.com/filecoin-project/go-jsonrpc"
	lcli "github.com/filecoin-project/lotus/cli"
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
		&cli.BoolFlag{
			Name:  "tracing",
			Usage: "enables tracing of booster-bitswap calls",
			Value: false,
		},
		&cli.StringFlag{
			Name:  "tracing-endpoint",
			Usage: "the endpoint for the tracing exporter",
			Value: "http://tempo:14268/api/traces",
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

		ctx := lcli.ReqContext(cctx)

		// Instantiate the tracer and exporter
		if cctx.Bool("tracing") {
			tracingStopper, err := tracing.New("booster-bitswap", cctx.String("tracing-endpoint"))
			if err != nil {
				return fmt.Errorf("failed to instantiate tracer: %w", err)
			}
			log.Info("Tracing exporter enabled")

			defer func() {
				_ = tracingStopper(ctx)
			}()
		}

		// Connect to the Boost API
		boostAPIInfo := cctx.String("api-boost")
		bapi, bcloser, err := getBoostAPI(ctx, boostAPIInfo)
		if err != nil {
			return fmt.Errorf("getting boost API: %w", err)
		}
		defer bcloser()

		remoteStore := remoteblockstore.NewRemoteBlockstore(bapi)
		bs := &httpGetStore{Blockstore: remoteStore}
		// Create the server API
		port := cctx.Int("port")
		server := NewBitswapServer(port, bs)

		// Start the server
		log.Infof("Starting booster-bitswap node on port %d", port)
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

type httpGetStore struct {
	blockstore.Blockstore
}

func (s *httpGetStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	start := time.Now()
	url := "http://localhost:8555" + bstoreserver.BasePathBlock + c.String()
	req, err := http.NewRequest("GET", url, nil)
	req = req.WithContext(ctx)
	var timeToFirstByte time.Duration
	trace := &httptrace.ClientTrace{
		GotFirstResponseByte: func() {
			timeToFirstByte = time.Since(start)
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send http req: %w", err)
	}
	defer resp.Body.Close() // nolint
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http req failed: code: %d, status: %s", resp.StatusCode, resp.Status)
	}

	startRead := time.Now()
	data, err := ioutil.ReadAll(resp.Body)
	log.Debugw("http get block", "cid", c, "error", err,
		"duration-ms", time.Since(start).Milliseconds(),
		"time-to-first-byte-ms", timeToFirstByte.Milliseconds(),
		"read-ms", time.Since(startRead).Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("failed to read block: %w", err)
	}
	return blocks.NewBlockWithCid(data, c)
}
