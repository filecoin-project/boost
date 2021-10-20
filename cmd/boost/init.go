package main

import (
	"context"

	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/boost/node/repo"

	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v0api"
	lcli "github.com/filecoin-project/lotus/cli"
)

var initCmd = &cli.Command{
	Name:   "init",
	Usage:  "Initialize a lotus miner repo",
	Flags:  []cli.Flag{},
	Before: before,
	Action: func(cctx *cli.Context) error {
		log.Info("Initializing boost repo")

		ctx := lcli.ReqContext(cctx)

		log.Debug("Trying to connect to full node RPC")

		if err := checkV1ApiSupport(ctx, cctx); err != nil {
			return err
		}

		api, closer, err := lcli.GetFullNodeAPIV1(cctx)
		if err != nil {
			return err
		}
		defer closer()

		log.Debug("Checking full node sync status")

		if err := lcli.SyncWait(ctx, &v0api.WrapperV1Full{FullNode: api}, false); err != nil {
			return xerrors.Errorf("sync wait: %w", err)
		}

		repoPath := cctx.String(FlagBoostRepo)
		log.Debugw("Checking if repo exists", "path", repoPath)

		r, err := repo.NewFS(repoPath)
		if err != nil {
			return err
		}

		ok, err := r.Exists()
		if err != nil {
			return err
		}
		if ok {
			return xerrors.Errorf("repo at '%s' is already initialized", cctx.String(FlagBoostRepo))
		}

		log.Debug("Checking full node version")

		v, err := api.Version(ctx)
		if err != nil {
			return err
		}

		if !v.APIVersion.EqMajorMinor(lapi.FullAPIVersion1) {
			return xerrors.Errorf("Remote API version didn't match (expected %s, remote %s)", lapi.FullAPIVersion1, v.APIVersion)
		}

		if err := r.Init(repo.Boost); err != nil {
			return err
		}

		{
			lr, err := r.Lock(repo.Boost)
			if err != nil {
				return err
			}

			if err := lr.Close(); err != nil {
				return err
			}
		}

		log.Info("Boost repo successfully created, you can now start boost with 'boost run'")

		return nil
	},
}

// checkV1ApiSupport uses v0 api version to signal support for v1 API
// trying to query the v1 api on older lotus versions would get a 404, which can happen for any number of other reasons
func checkV1ApiSupport(ctx context.Context, cctx *cli.Context) error {
	// check v0 api version to make sure it supports v1 api
	api0, closer, err := lcli.GetFullNodeAPI(cctx)
	if err != nil {
		return err
	}

	v, err := api0.Version(ctx)
	closer()

	if err != nil {
		return err
	}

	if !v.APIVersion.EqMajorMinor(lapi.FullAPIVersion0) {
		return xerrors.Errorf("Remote API version didn't match (expected %s, remote %s)", lapi.FullAPIVersion0, v.APIVersion)
	}

	return nil
}
