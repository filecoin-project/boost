package gql

import (
	"context"

	gqltypes "github.com/filecoin-project/boost/gql/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
)

type storageResolver struct {
	Staged      gqltypes.Uint64
	Transferred gqltypes.Uint64
	Pending     gqltypes.Uint64
	Free        gqltypes.Uint64
	MountPoint  string
}

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
	staged := uint64(0)
	sealing := uint64(0)

	for _, deal := range activeDeals {
		if deal.IsOffline {
			continue
		}

		if deal.Checkpoint < dealcheckpoints.Transferred {
			transferred += r.provider.NBytesReceived(deal.DealUuid)
		} else if deal.Checkpoint < dealcheckpoints.AddedPiece {
			staged += deal.Transfer.Size
		} else {
			sealing += deal.Transfer.Size
		}
	}

	log.Debugw("storage values", "tagged", tagged, "transferred", transferred, "staged", staged, "sealing", sealing)

	return &storageResolver{
		Staged:      gqltypes.Uint64(staged),
		Transferred: gqltypes.Uint64(transferred),
		Pending:     gqltypes.Uint64(tagged - transferred - staged),
		Free:        gqltypes.Uint64(free),
		MountPoint:  r.storageMgr.StagingAreaDirPath,
	}, nil
}
