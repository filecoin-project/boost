package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"

	"github.com/filecoin-project/boost/cmd/lib"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/boost/tracing"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/dagstore/mount"
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
		&cli.StringFlag{
			Name:     "api-piece-directory",
			Usage:    "the endpoint for the piece directory API",
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

		// Connect to the piece directory service
		ctx := lcli.ReqContext(cctx)
		pdClient := piecedirectory.NewStore()
		defer pdClient.Close(ctx)
		err := pdClient.Dial(ctx, cctx.String("api-piece-directory"))
		if err != nil {
			return fmt.Errorf("connecting to piece directory service: %w", err)
		}

		// Connect to the full node API
		fnApiInfo := cctx.String("api-fullnode")
		fullnodeApi, ncloser, err := lib.GetFullNodeApi(ctx, fnApiInfo, log)
		if err != nil {
			return fmt.Errorf("getting full node API: %w", err)
		}
		defer ncloser()

		// Connect to the storage API and create a sector accessor
		storageApiInfo := cctx.String("api-storage")
		sa, storageCloser, err := lib.CreateSectorAccessor(ctx, storageApiInfo, fullnodeApi, log)
		if err != nil {
			return err
		}
		defer storageCloser()

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

		// Create the server API
		pr := &piecedirectory.SectorAccessorAsPieceReader{SectorAccessor: sa}
		piecedirectory := piecedirectory.NewPieceDirectory(pdClient, pr, cctx.Int("add-index-throttle"))
		sapi := serverApi{ctx: ctx, piecedirectory: piecedirectory, sa: sa}
		server := NewHttpServer(
			cctx.String("base-path"),
			cctx.Int("port"),
			sapi,
		)

		// Start the server
		log.Infof("Starting booster-http node on port %d with base path '%s'",
			cctx.Int("port"), cctx.String("base-path"))
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
	ctx            context.Context
	piecedirectory *piecedirectory.PieceDirectory
	sa             dagstore.SectorAccessor
}

var _ HttpServerApi = (*serverApi)(nil)

func (s serverApi) GetPieceDeals(ctx context.Context, pieceCID cid.Cid) ([]model.DealInfo, error) {
	return s.piecedirectory.GetPieceDeals(ctx, pieceCID)
}

func (s serverApi) IsUnsealed(ctx context.Context, sectorID abi.SectorNumber, offset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (bool, error) {
	return s.sa.IsUnsealed(ctx, sectorID, offset, length)
}

func (s serverApi) UnsealSectorAt(ctx context.Context, sectorID abi.SectorNumber, offset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (mount.Reader, error) {
	return s.sa.UnsealSectorAt(ctx, sectorID, offset, length)
}

func (s serverApi) GetBlockByCid(ctx context.Context, blockCid cid.Cid) ([]byte, error) {
	return s.piecedirectory.BlockstoreGet(ctx, blockCid)
}
