package util

import (
	"context"

	"github.com/filecoin-project/go-commp-utils/writer"
	"github.com/filecoin-project/go-fil-markets/shared"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-car"
	"golang.org/x/xerrors"
)

const DefaultMaxTraversalLinks = 2 << 29

func CommP(ctx context.Context, bs bstore.Blockstore, root cid.Cid) (cid.Cid, abi.UnpaddedPieceSize, error) {
	// do a CARv1 traversal with the DFS selector.
	sc := car.NewSelectiveCar(ctx, bs, []car.Dag{{Root: root, Selector: shared.AllSelector()}}, car.MaxTraversalLinks(DefaultMaxTraversalLinks))
	//sc := car.NewSelectiveCar(ctx, bs, []car.Dag{{Root: root, Selector: selectorparse.CommonSelector_ExploreAllRecursively}}, car.MaxTraversalLinks(DefaultMaxTraversalLinks))
	prepared, err := sc.Prepare()
	if err != nil {
		return cid.Undef, 0, xerrors.Errorf("failed to prepare CAR: %w", err)
	}

	// write out the deterministic CARv1 payload to the CommP writer and calculate the CommP.
	commpWriter := &writer.Writer{}
	err = prepared.Dump(commpWriter)
	if err != nil {
		return cid.Undef, 0, xerrors.Errorf("failed to write CARv1 to commP writer: %w", err)
	}
	dataCIDSize, err := commpWriter.Sum()
	if err != nil {
		return cid.Undef, 0, xerrors.Errorf("commpWriter.Sum failed: %w", err)
	}

	return dataCIDSize.PieceCID, dataCIDSize.PieceSize.Unpadded(), nil
}
