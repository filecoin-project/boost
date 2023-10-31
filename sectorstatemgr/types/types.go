package types

import (
	"context"

	"github.com/filecoin-project/lotus/storage/sealer/storiface"
)

type StorageAPI interface {
	StorageRedeclareLocal(context.Context, *storiface.ID, bool) error
	StorageList(context.Context) (map[storiface.ID][]storiface.Decl, error)
}
