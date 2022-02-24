package gql

import (
	"context"
	"fmt"

	gqltypes "github.com/filecoin-project/boost/gql/types"
	"github.com/filecoin-project/lotus/build"
)

type epochInfo struct {
	Epoch           gqltypes.Uint64
	SecondsPerEpoch int32
}

func (r *resolver) Epoch(ctx context.Context) (*epochInfo, error) {
	head, err := r.fullNode.ChainHead(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting chain head: %w", err)
	}

	return &epochInfo{
		Epoch:           gqltypes.Uint64(head.Height()),
		SecondsPerEpoch: int32(build.BlockDelaySecs),
	}, nil
}
