package gql

import "context"

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

	staged := uint64(0)
	transferred := uint64(0)
	return &storageResolver{
		Staged:      staged,
		Transferred: transferred,
		Pending:     tagged - transferred,
		Free:        free,
		MountPoint:  r.storageMgr.StagingAreaDirPath,
	}, nil
}
