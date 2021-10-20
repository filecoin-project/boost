package modules

import (
	"context"

	"github.com/filecoin-project/lotus/storage/sectorblocks"

	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/node/modules/helpers"

	lapi "github.com/filecoin-project/lotus/api"
	lclient "github.com/filecoin-project/lotus/api/client"
	cliutil "github.com/filecoin-project/lotus/cli/util"
)

type MinerSealingService lapi.StorageMiner
type MinerStorageService lapi.StorageMiner

var _ sectorblocks.SectorBuilder = *new(MinerSealingService)

func connectMinerService(apiInfo string) func(mctx helpers.MetricsCtx, lc fx.Lifecycle) (lapi.StorageMiner, error) {
	return func(mctx helpers.MetricsCtx, lc fx.Lifecycle) (lapi.StorageMiner, error) {
		ctx := helpers.LifecycleCtx(mctx, lc)
		info := cliutil.ParseApiInfo(apiInfo)
		addr, err := info.DialArgs("v0")
		if err != nil {
			return nil, xerrors.Errorf("could not get DialArgs: %w", err)
		}

		log.Infof("Checking (svc) api version of %s", addr)

		mapi, closer, err := lclient.NewStorageMinerRPCV0(ctx, addr, info.AuthHeader())
		if err != nil {
			return nil, err
		}
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				v, err := mapi.Version(ctx)
				if err != nil {
					return xerrors.Errorf("checking version: %w", err)
				}

				if !v.APIVersion.EqMajorMinor(lapi.MinerAPIVersion0) {
					return xerrors.Errorf("remote service API version didn't match (expected %s, remote %s)", api.MinerAPIVersion0, v.APIVersion)
				}

				return nil
			},
			OnStop: func(context.Context) error {
				closer()
				return nil
			}})

		return mapi, nil
	}
}

func ConnectSealingService(apiInfo string) func(mctx helpers.MetricsCtx, lc fx.Lifecycle) (MinerSealingService, error) {
	return func(mctx helpers.MetricsCtx, lc fx.Lifecycle) (MinerSealingService, error) {
		log.Info("Connecting sealing service to miner")
		return connectMinerService(apiInfo)(mctx, lc)
	}
}

func ConnectStorageService(apiInfo string) func(mctx helpers.MetricsCtx, lc fx.Lifecycle) (MinerStorageService, error) {
	return func(mctx helpers.MetricsCtx, lc fx.Lifecycle) (MinerStorageService, error) {
		log.Info("Connecting storage service to miner")
		return connectMinerService(apiInfo)(mctx, lc)
	}
}
