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
	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/api/v0api"
	"github.com/filecoin-project/lotus/api/v1api"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/markets/dagstore"
	"github.com/filecoin-project/lotus/markets/sectoraccessor"
	lotus_modules "github.com/filecoin-project/lotus/node/modules"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/node/repo"
	"github.com/filecoin-project/lotus/storage/paths"
	"github.com/filecoin-project/lotus/storage/sealer"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
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
		boostApiInfo := cctx.String("api-boost")
		bapi, bcloser, err := getBoostApi(ctx, boostApiInfo)
		if err != nil {
			return fmt.Errorf("getting boost API: %w", err)
		}
		defer bcloser()

		// Connect to the full node API
		fnApiInfo := cctx.String("api-fullnode")
		fullnodeApi, ncloser, err := getFullNodeApi(ctx, fnApiInfo)
		if err != nil {
			return fmt.Errorf("getting full node API: %w", err)
		}
		defer ncloser()

		// Connect to the sealing API
		sealingApiInfo := cctx.String("api-sealer")
		sauth, err := storageAuthWithURL(sealingApiInfo)
		if err != nil {
			return fmt.Errorf("parsing sealing API endpoint: %w", err)
		}
		sealingService, sealerCloser, err := getMinerApi(ctx, sealingApiInfo)
		if err != nil {
			return fmt.Errorf("getting miner API: %w", err)
		}
		defer sealerCloser()

		maddr, err := sealingService.ActorAddress(ctx)
		if err != nil {
			return fmt.Errorf("getting miner actor address: %w", err)
		}
		log.Infof("Miner address: %s", maddr)

		// Use an in-memory repo because we don't need any functions
		// of a real repo, we just need to supply something that satisfies
		// the LocalStorage interface to the store
		memRepo := repo.NewMemory(nil)
		lr, err := memRepo.Lock(repo.StorageMiner)
		if err != nil {
			return fmt.Errorf("locking mem repo: %w", err)
		}
		defer lr.Close()

		// Create the store interface
		var urls []string
		lstor, err := paths.NewLocal(ctx, lr, sealingService, urls)
		if err != nil {
			return fmt.Errorf("creating new local store: %w", err)
		}
		storage := lotus_modules.RemoteStorage(lstor, sealingService, sauth, sealer.Config{
			// TODO: Not sure if I need this, or any of the other fields in this struct
			ParallelFetchLimit: 1,
		})
		// Create the piece provider and sector accessors
		pp := sealer.NewPieceProvider(storage, sealingService, sealingService)
		sa := sectoraccessor.NewSectorAccessor(dtypes.MinerAddress(maddr), sealingService, pp, fullnodeApi)
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

type serverApi struct {
	ctx  context.Context
	bapi api.Boost
	sa   dagstore.SectorAccessor
}

var _ HttpServerApi = (*serverApi)(nil)

func (s serverApi) PiecesContainingMultihash(mh multihash.Multihash) ([]cid.Cid, error) {
	return s.bapi.BoostDagstorePiecesContainingMultihash(s.ctx, mh)
}

func (s serverApi) GetMaxPieceOffset(pieceCid cid.Cid) (uint64, error) {
	return s.bapi.PiecesGetMaxOffset(s.ctx, pieceCid)
}

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

func getFullNodeApi(ctx context.Context, ai string) (v1api.FullNode, jsonrpc.ClientCloser, error) {
	ai = strings.TrimPrefix(strings.TrimSpace(ai), "FULLNODE_API_INFO=")
	info := cliutil.ParseApiInfo(ai)
	addr, err := info.DialArgs("v1")
	if err != nil {
		return nil, nil, fmt.Errorf("could not get DialArgs: %w", err)
	}

	log.Infof("Using full node API at %s", addr)
	api, closer, err := client.NewFullNodeRPCV1(ctx, addr, info.AuthHeader())
	if err != nil {
		return nil, nil, fmt.Errorf("creating full node service API: %w", err)
	}

	v, err := api.Version(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("checking full node service API version: %w", err)
	}

	if !v.APIVersion.EqMajorMinor(lapi.FullAPIVersion1) {
		return nil, nil, fmt.Errorf("full node service API version didn't match (expected %s, remote %s)", lapi.FullAPIVersion1, v.APIVersion)
	}

	return api, closer, nil
}

func getMinerApi(ctx context.Context, ai string) (v0api.StorageMiner, jsonrpc.ClientCloser, error) {
	ai = strings.TrimPrefix(strings.TrimSpace(ai), "MINER_API_INFO=")
	info := cliutil.ParseApiInfo(ai)
	addr, err := info.DialArgs("v0")
	if err != nil {
		return nil, nil, fmt.Errorf("could not get DialArgs: %w", err)
	}

	log.Infof("Using sealing API at %s", addr)
	api, closer, err := client.NewStorageMinerRPCV0(ctx, addr, info.AuthHeader())
	if err != nil {
		return nil, nil, fmt.Errorf("creating miner service API: %w", err)
	}

	v, err := api.Version(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("checking miner service API version: %w", err)
	}

	if !v.APIVersion.EqMajorMinor(lapi.MinerAPIVersion0) {
		return nil, nil, fmt.Errorf("miner service API version didn't match (expected %s, remote %s)", lapi.MinerAPIVersion0, v.APIVersion)
	}

	return api, closer, nil
}
