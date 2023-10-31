package server

import (
	"context"
	"errors"
	"time"

	"github.com/filecoin-project/boost/datatransfer"
	dtimpl "github.com/filecoin-project/boost/datatransfer/impl"
	marketevents "github.com/filecoin-project/boost/markets/loggers"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/node/repo"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"go.uber.org/fx"
)

type ProviderDataTransfer datatransfer.Manager

// NewProviderDataTransfer returns a data transfer manager
func NewProviderDataTransfer(lc fx.Lifecycle, net dtypes.ProviderTransferNetwork, transport dtypes.ProviderTransport, ds lotus_dtypes.MetadataDS, r repo.LockedRepo) (ProviderDataTransfer, error) {
	dtDs := namespace.Wrap(ds, datastore.NewKey("/datatransfer/provider/transfers"))

	dt, err := dtimpl.NewDataTransfer(dtDs, net, transport)
	if err != nil {
		return nil, err
	}

	dt.OnReady(marketevents.ReadyLogger("provider data transfer"))
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			dt.SubscribeToEvents(marketevents.DataTransferLogger)
			return dt.Start(ctx)
		},
		OnStop: func(ctx context.Context) error {
			errc := make(chan error)

			go func() {
				errc <- dt.Stop(ctx)
			}()

			select {
			case err := <-errc:
				return err
			case <-time.After(5 * time.Second):
				return errors.New("couldnt stop datatransfer.Manager in 5 seconds. forcing an App.Stop")
			}
		},
	})
	return dt, nil
}
