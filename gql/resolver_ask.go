package gql

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/boost-gfm/storagemarket"
	"github.com/filecoin-project/boost/gql/types"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/build"
	"github.com/graph-gophers/graphql-go"
)

type storageAskResolver struct {
	Price         types.BigInt
	VerifiedPrice types.BigInt
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
	expTime := time.Now().Add(time.Duration(expTimeEpochs) * time.Duration(build.BlockDelaySecs) * time.Second)

	return &storageAskResolver{
		Price:         types.BigInt{Int: ask.Price},
		VerifiedPrice: types.BigInt{Int: ask.VerifiedPrice},
		MinPieceSize:  types.Uint64(ask.MinPieceSize),
		MaxPieceSize:  types.Uint64(ask.MaxPieceSize),
		ExpiryEpoch:   types.Uint64(ask.Expiry),
		ExpiryTime:    graphql.Time{Time: expTime},
	}, nil
}

type storageAskUpdate struct {
	Price         *types.BigInt
	VerifiedPrice *types.BigInt
	MinPieceSize  *types.Uint64
	MaxPieceSize  *types.Uint64
}

func (r *resolver) StorageAskUpdate(args struct{ Update storageAskUpdate }) (bool, error) {
	signedAsk := r.legacyProv.GetAsk()
	ask := signedAsk.Ask

	dur := 87660 * time.Hour // 10 years
	duration := abi.ChainEpoch(dur.Seconds() / float64(build.BlockDelaySecs))

	price := ask.Price
	verifiedPrice := ask.VerifiedPrice
	var opts []storagemarket.StorageAskOption

	update := args.Update
	if update.Price != nil {
		price = (*update.Price).Int
	}
	if update.VerifiedPrice != nil {
		verifiedPrice = (*update.VerifiedPrice).Int
	}
	if update.MinPieceSize != nil {
		opts = append(opts, storagemarket.MinPieceSize(abi.PaddedPieceSize(*update.MinPieceSize)))
	}
	if update.MaxPieceSize != nil {
		opts = append(opts, storagemarket.MaxPieceSize(abi.PaddedPieceSize(*update.MaxPieceSize)))
	}

	err := r.legacyProv.SetAsk(price, verifiedPrice, duration, opts...)
	if err != nil {
		return false, fmt.Errorf("setting ask: %w", err)
	}

	return true, nil
}
