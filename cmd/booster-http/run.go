package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"strings"

	"github.com/filecoin-project/boost/api"
	bclient "github.com/filecoin-project/boost/api/client"
	cliutil "github.com/filecoin-project/boost/cli/util"
	"github.com/filecoin-project/boost/cmd/lib"
	"github.com/filecoin-project/boost/tracing"
	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/markets/dagstore"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
)

var runCmd = &cli.Command{
	Name:   "run",
	Usage:  "Start a booster-http process",
	Before: before,
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "pprof",
			Usage: "run pprof web server on localhost:6070",
		},
		&cli.StringFlag{
			Name:  "base-path",
			Usage: "the base path at which to run the web server",
			Value: "",
		},
		&cli.UintFlag{
			Name:  "port",
			Usage: "the port the web server listens on",
			Value: 7777,
		},
		&cli.BoolFlag{
			Name:  "allow-indexing",
			Usage: "allow booster-http to build an index for a CAR file on the fly if necessary (requires doing an extra pass over the CAR file)",
			Value: false,
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
			Name:     "api-storage",
			Usage:    "the endpoint for the storage node API",
			Required: true,
		},
		&cli.BoolFlag{
			Name:  "tracing",
			Usage: "enables tracing of booster-http calls",
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

		// Connect to the Boost API
		ctx := lcli.ReqContext(cctx)
		boostApiInfo := cctx.String("api-boost")
		bapi, bcloser, err := getBoostApi(ctx, boostApiInfo)
		if err != nil {
			return fmt.Errorf("getting boost API: %w", err)
		}
		defer bcloser()

		// Connect to the full node API
		fnApiInfo := cctx.String("api-fullnode")
		fullnodeApi, ncloser, err := lib.GetFullNodeApi(ctx, fnApiInfo, log)
		if err != nil {
			return fmt.Errorf("getting full node API: %w", err)
		}
		defer ncloser()

		// Connect to the storage API
		storageApiInfo := cctx.String("api-storage")
		if err != nil {
			return fmt.Errorf("parsing storage API endpoint: %w", err)
		}

		// Instantiate the tracer and exporter
		enableTracing := cctx.Bool("tracing")
		var tracingStopper func(context.Context) error
		if enableTracing {
			tracingStopper, err = tracing.New("booster-http", cctx.String("tracing-endpoint"))
			if err != nil {
				return fmt.Errorf("failed to instantiate tracer: %w", err)
			}
			log.Info("Tracing exporter enabled")
		}

		// Create the sector accessor
		sa, storageCloser, err := lib.CreateSectorAccessor(ctx, storageApiInfo, fullnodeApi, log)
		if err != nil {
			return err
		}
		defer storageCloser()

		allowIndexing := cctx.Bool("allow-indexing")
		// Create the server API
		sapi := serverApi{ctx: ctx, bapi: bapi, sa: sa}
		server := NewHttpServer(
			cctx.String("base-path"),
			cctx.Int("port"),
			allowIndexing,
			sapi,
		)

		// Start the server
		log.Infof("Starting booster-http node on port %d with base path '%s'",
			cctx.Int("port"), cctx.String("base-path"))
		var indexingStr string
		if allowIndexing {
			indexingStr = "Enabled"
		} else {
			indexingStr = "Disabled"
		}
		log.Info("On-the-fly indexing of CAR files is " + indexingStr)
		server.Start(ctx)

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

		if enableTracing {
			err = tracingStopper(ctx)
			if err != nil {
				return err
			}
		}

		return nil
	},
}

type serverApi struct {
	ctx  context.Context
	bapi api.Boost
	sa   dagstore.SectorAccessor
}

var _ HttpServerApi = (*serverApi)(nil)

func (s serverApi) GetPieceInfo(pieceCID cid.Cid) (*piecestore.PieceInfo, error) {
	return s.bapi.PiecesGetPieceInfo(s.ctx, pieceCID)
}

func (s serverApi) IsUnsealed(ctx context.Context, sectorID abi.SectorNumber, offset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (bool, error) {
	return s.sa.IsUnsealed(ctx, sectorID, offset, length)
}

func (s serverApi) UnsealSectorAt(ctx context.Context, sectorID abi.SectorNumber, offset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (mount.Reader, error) {
	return s.sa.UnsealSectorAt(ctx, sectorID, offset, length)
}

func getBoostApi(ctx context.Context, ai string) (api.Boost, jsonrpc.ClientCloser, error) {
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
