package modules

import (
	"context"

	"github.com/filecoin-project/boost/piecemeta"
	"github.com/filecoin-project/go-address"
	marketevents "github.com/filecoin-project/lotus/markets/loggers"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
	"go.uber.org/fx"
)

// NewProviderPieceStore creates a statestore for storing metadata about pieces
// shared by the storage and retrieval providers
func NewProviderPieceStore(lc fx.Lifecycle, pm *piecemeta.PieceMeta, maddr lotus_dtypes.MinerAddress) (lotus_dtypes.ProviderPieceStore, error) {
	ps := NewPieceStore(pm, address.Address(maddr))
	ps.OnReady(marketevents.ReadyLogger("piecestore"))
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return ps.Start(ctx)
		},
	})
	return ps, nil
}
