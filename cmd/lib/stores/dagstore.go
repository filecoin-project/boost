package stores

import (
	"io"

	bstore "github.com/ipfs/boxo/blockstore"
)

type ClosableBlockstore interface {
	bstore.Blockstore
	io.Closer
}
