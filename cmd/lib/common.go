package lib

import (
	"context"
	"errors"
	"fmt"

	"github.com/filecoin-project/boost-gfm/piecestore"
	piecestoreimpl "github.com/filecoin-project/boost-gfm/piecestore/impl"
	"github.com/filecoin-project/boost-gfm/storagemarket"
	vfsm "github.com/filecoin-project/go-ds-versioning/pkg/fsm"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-statemachine/fsm"
	"github.com/filecoin-project/lotus/lib/backupds"
	"github.com/filecoin-project/lotus/node/repo"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
)

func OpenDataStore(path string) (*backupds.Datastore, error) {
	ctx := context.Background()

	rpo, err := repo.NewFS(path)
	if err != nil {
		return nil, fmt.Errorf("could not open repo %s: %w", path, err)
	}

	exists, err := rpo.Exists()
	if err != nil {
		return nil, fmt.Errorf("checking repo %s exists: %w", path, err)
	}
	if !exists {
		return nil, fmt.Errorf("repo does not exist: %s", path)
	}

	lr, err := rpo.Lock(repo.StorageMiner)
	if err != nil {
		return nil, fmt.Errorf("locking repo %s: %w", path, err)
	}

	mds, err := lr.Datastore(ctx, "/metadata")
	if err != nil {
		return nil, err
	}

	bds, err := backupds.Wrap(mds, "")
	if err != nil {
		return nil, fmt.Errorf("opening backupds: %w", err)
	}

	return bds, nil
}

func GetPropCidByChainDealID(ctx context.Context, ds *backupds.Datastore) (map[abi.DealID]cid.Cid, error) {
	deals, err := getLegacyDealsFSM(ctx, ds)
	if err != nil {
		return nil, err
	}

	// Build a mapping of chain deal ID to proposal CID
	var list []storagemarket.MinerDeal
	if err := deals.List(&list); err != nil {
		return nil, err
	}

	byChainDealID := make(map[abi.DealID]cid.Cid, len(list))
	for _, d := range list {
		if d.DealID != 0 {
			byChainDealID[d.DealID] = d.ProposalCid
		}
	}

	return byChainDealID, nil
}

func OpenPieceStore(ctx context.Context, ds *backupds.Datastore) (piecestore.PieceStore, error) {
	// Open the piece store
	ps, err := piecestoreimpl.NewPieceStore(namespace.Wrap(ds, datastore.NewKey("/storagemarket")))
	if err != nil {
		return nil, fmt.Errorf("creating piece store from datastore : %w", err)
	}

	// Wait for the piece store to be ready
	ch := make(chan error, 1)
	ps.OnReady(func(e error) {
		ch <- e
	})

	err = ps.Start(ctx)
	if err != nil {
		return nil, fmt.Errorf("starting piece store: %w", err)
	}

	select {
	case err = <-ch:
		if err != nil {
			return nil, fmt.Errorf("waiting for piece store to be ready: %w", err)
		}
	case <-ctx.Done():
		return nil, errors.New("cancelled while waiting for piece store to be ready")
	}

	return ps, nil
}

func getLegacyDealsFSM(ctx context.Context, ds *backupds.Datastore) (fsm.Group, error) {
	// Get the deals FSM
	provDS := namespace.Wrap(ds, datastore.NewKey("/deals/provider"))
	deals, migrate, err := vfsm.NewVersionedFSM(provDS, fsm.Parameters{
		StateType:     storagemarket.MinerDeal{},
		StateKeyField: "State",
	}, nil, "2")
	if err != nil {
		return nil, fmt.Errorf("reading legacy deals from datastore: %w", err)
	}

	err = migrate(ctx)
	if err != nil {
		return nil, fmt.Errorf("running provider fsm migration script: %w", err)
	}

	return deals, err
}
