package gql

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/boost/gql/types"
	"github.com/graph-gophers/graphql-go"
)

type storageAskResolver struct {
	Price         types.Uint64
	VerifiedPrice types.Uint64
	MinPieceSize  types.Uint64
	MaxPieceSize  types.Uint64
	ExpiryEpoch   types.Uint64
	ExpiryTime    graphql.Time
}

func (r *resolver) StorageAsk(ctx context.Context) (*storageAskResolver, error) {
	head, err := r.fullNode.ChainHead(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting chain head: %w", err)
	}

	signedAsk := r.legacyProv.GetAsk()
	ask := signedAsk.Ask
	expTimeEpochs := ask.Expiry - head.Height()
	expTime := time.Now().Add(time.Duration(expTimeEpochs) * time.Second * 30)

	return &storageAskResolver{
		Price:         types.Uint64(ask.Price.Uint64()),
		VerifiedPrice: types.Uint64(ask.VerifiedPrice.Uint64()),
		MinPieceSize:  types.Uint64(ask.MinPieceSize),
		MaxPieceSize:  types.Uint64(ask.MaxPieceSize),
		ExpiryEpoch:   types.Uint64(ask.Expiry),
		ExpiryTime:    graphql.Time{Time: expTime},
	}, nil
}
