package gql

import (
	"context"
	"fmt"

	gqltypes "github.com/filecoin-project/boost/gql/types"
)

func (r *resolver) Epoch(ctx context.Context) (gqltypes.Uint64, error) {
	head, err := r.fullNode.ChainHead(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting chain head: %w", err)
	}

	return gqltypes.Uint64(head.Height()), nil
}
