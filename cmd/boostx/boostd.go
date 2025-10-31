package main

import (
	"fmt"

	"github.com/filecoin-project/boost/node/repo"
	"github.com/filecoin-project/go-address"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	"github.com/ipfs/go-datastore"
	"github.com/urfave/cli/v2"
)

const metadataNamespace = "/metadata"

var boostdCmd = &cli.Command{
	Name:  "boostd",
	Usage: "boostd utilities",
	Subcommands: []*cli.Command{
		minerAddressCmd,
	},
}

var minerAddressCmd = &cli.Command{
	Name:        "miner-address",
	Usage:       "boostx boostd miner-address",
	Description: "Fetch the miner address from the datastore. Boostd process must be stopped before running this command.",
	Action: func(cctx *cli.Context) error {

		boostRepoPath := cctx.String(FlagBoostRepo)

		r, err := lotus_repo.NewFS(boostRepoPath)
		if err != nil {
			return err
		}
		ok, err := r.Exists()
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("repo at '%s' is not initialized", cctx.String(FlagBoostRepo))
		}

		lr, err := r.LockRO(repo.Boost)
		if err != nil {
			return fmt.Errorf("locking repo: %w. Please stop the boostd process to proceed", err)
		}
		defer func() {
			_ = lr.Close()
		}()

		mds, err := lr.Datastore(cctx.Context, metadataNamespace)
		if err != nil {
			return fmt.Errorf("getting metadata datastore: %w", err)
		}

		var minerAddrDSKey = datastore.NewKey("miner-address")

		addr, err := mds.Get(cctx.Context, minerAddrDSKey)
		if err != nil {
			return fmt.Errorf("getting miner address from legacy datastore: %w", err)
		}

		minerAddr, err := address.NewFromBytes(addr)
		if err != nil {
			return fmt.Errorf("parsing miner address from legacy datastore: %w", err)
		}

		fmt.Printf("Miner Address from datastore: %s\n", minerAddr.String())

		return nil
	},
}
