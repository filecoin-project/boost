package gql

import (
	"context"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
)

// query: sealingpipeline: [SealingPipeline]
func (r *resolver) SealingPipeline(ctx context.Context) (*sealingPipelineResolver, error) {
	committing := int32(0)
	precommitting := int32(0)
	waitSeed := int32(0)

	//res, err := r.spApi.WorkerJobs(ctx)
	//if err != nil {
	//return nil, err
	//}

	//_ = res

	//spew.Dump(res)

	summary, err := r.spApi.SectorsSummary(ctx)
	if err != nil {
		return nil, err
	}

	wdSectors, err := r.spApi.SectorsListInStates(ctx, []api.SectorState{"WaitDeals"})
	if err != nil {
		return nil, err
	}

	taken := uint64(0)
	deals := []abi.DealID{}
	for _, s := range wdSectors {
		wdSectorStatus, err := r.spApi.SectorsStatus(ctx, s, false)
		if err != nil {
			return nil, err
		}

		for _, p := range wdSectorStatus.Pieces {
			taken += uint64(p.Piece.Size)
		}

		deals = append(deals, wdSectorStatus.Deals...)
	}

	log.Debugw("sealing pipeline", "waitdeals", summary["WaitDeals"], "taken", taken, "deals", deals, "pc1", summary["PreCommit1"], "pc2", summary["PreCommit2"], "precommitwait", summary["PreCommitWait"], "waitseed", summary["WaitSeed"], "committing", summary["Committing"], "commitwait", summary["CommitWait"], "proving", summary["Proving"])

	return &sealingPipelineResolver{
		Committing:    committing,
		WaitSeed:      waitSeed,
		PreCommitting: precommitting,
	}, nil
}

type sealingPipelineResolver struct {
	Committing    int32
	WaitSeed      int32
	PreCommitting int32
}
