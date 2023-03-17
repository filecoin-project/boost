package modules

import (
	"context"

	marketevents "github.com/filecoin-project/boost/markets/loggers"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/go-address"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
	"go.uber.org/fx"
)

// NewProviderPieceStore creates a statestore for storing metadata about pieces
// shared by the storage and retrieval providers
func NewProviderPieceStore(lc fx.Lifecycle, pm *piecedirectory.PieceDirectory, maddr lotus_dtypes.MinerAddress) (lotus_dtypes.ProviderPieceStore, error) {
	ps := NewPieceStore(pm, address.Address(maddr))
	ps.OnReady(marketevents.ReadyLogger("piecestore"))
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return ps.Start(ctx)
		},
	})
	return ps, nil
}
