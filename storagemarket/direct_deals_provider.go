package storagemarket

import (
	"context"
	"fmt"
	"os"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/go-address"
	"github.com/ipfs/go-cid"
)

//var log = logging.Logger("direct-deals-providers")

type DirectDealsProvider struct {
}

func NewDirectDealsProvider() *DirectDealsProvider {
	return &DirectDealsProvider{}
}

func (ddp *DirectDealsProvider) Import(ctx context.Context, piececid cid.Cid, filepath string, deleteAfterImport bool, allocationid string, clientaddr address.Address) (*api.ProviderDealRejectionInfo, error) {
	log.Infow("received direct data import", "piececid", piececid, "filepath", filepath, "clientaddr", clientaddr, "allocationid", allocationid)

	fi, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open filepath: %w", err)
	}
	defer fi.Close() //nolint:errcheck

	return nil, nil
}
