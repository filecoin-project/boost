package gql

import (
	"context"

	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
)

// query: storage: [Storage]
func (r *resolver) Storage(ctx context.Context) (*storageResolver, error) {
	tagged, err := r.storageMgr.TotalTagged(ctx)
	if err != nil {
		return nil, err
	}

	free, err := r.storageMgr.Free(ctx)
	if err != nil {
		return nil, err
	}

	activeDeals, err := r.dealsDB.ListActive(ctx)
	if err != nil {
		return nil, err
	}

	transferred := uint64(0)
	for _, deal := range activeDeals {
		if deal.Checkpoint < dealcheckpoints.Transferred {
			transferred += r.provider.NBytesReceived(deal.DealUuid)
		} else {
			transferred += deal.Transfer.Size
		}
	}

	staged := uint64(0)
	return &storageResolver{
		Staged:      float64(staged),
		Transferred: float64(transferred),
		Pending:     float64(tagged - transferred),
		Free:        float64(free),
		MountPoint:  r.storageMgr.StagingAreaDirPath,
	}, nil
}
