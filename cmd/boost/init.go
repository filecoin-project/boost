package main

import (
	"context"
	"strings"

	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/boost/node/repo"

	cliutil "github.com/filecoin-project/boost/cli/util"
	"github.com/filecoin-project/boost/node/config"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/api/v0api"
	lcli "github.com/filecoin-project/lotus/cli"
)

var initCmd = &cli.Command{
	Name:  "init",
	Usage: "Initialize a lotus miner repo",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "api-sector-index",
			Usage: "sector Index API info (lotus-miner auth api-info --perm=admin)",
		},
	},
	Before: before,
	Action: func(cctx *cli.Context) error {
		log.Info("Initializing boost repo")

		if !cctx.IsSet("api-sector-index") {
			return xerrors.Errorf("--api-sector-index is required")
		}

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

			var cerr error
			err = lr.SetConfig(func(raw interface{}) {
				rcfg, ok := raw.(*config.Boost)
				if !ok {
					cerr = xerrors.New("expected boost config")
					return
				}

				ai, err := checkApiInfo(ctx, cctx.String("api-sector-index"))
				if err != nil {
					cerr = xerrors.Errorf("checking sector index API: %w", err)
					return
				}
				rcfg.SectorIndexApiInfo = ai

			})
			if cerr != nil {
				return cerr
			}
			if err != nil {
				return xerrors.Errorf("setting config: %w", err)
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

func checkApiInfo(ctx context.Context, ai string) (string, error) {
	ai = strings.TrimPrefix(strings.TrimSpace(ai), "MINER_API_INFO=")
	info := cliutil.ParseApiInfo(ai)
	addr, err := info.DialArgs("v0")
	if err != nil {
		return "", xerrors.Errorf("could not get DialArgs: %w", err)
	}

	log.Infof("Checking api version of %s", addr)

	api, closer, err := client.NewStorageMinerRPCV0(ctx, addr, info.AuthHeader())
	if err != nil {
		return "", err
	}
	defer closer()

	v, err := api.Version(ctx)
	if err != nil {
		return "", xerrors.Errorf("checking version: %w", err)
	}

	if !v.APIVersion.EqMajorMinor(lapi.MinerAPIVersion0) {
		return "", xerrors.Errorf("remote service API version didn't match (expected %s, remote %s)", lapi.MinerAPIVersion0, v.APIVersion)
	}

	return ai, nil
}
