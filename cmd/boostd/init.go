package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"

	cliutil "github.com/filecoin-project/boost/cli/util"
	scliutil "github.com/filecoin-project/boost/extern/boostd-data/shared/cliutil"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/node/repo"
	"github.com/filecoin-project/go-address"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/api/v0api"
	lcli "github.com/filecoin-project/lotus/cli"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	"github.com/ipfs/go-datastore"
	"github.com/urfave/cli/v2"
)

const metadataNamespace = "/metadata"

var minerApiFlags = []cli.Flag{
	&cli.StringFlag{
		Name:     "api-sealer",
		Usage:    "miner/sealer API info (lotus-miner auth api-info --perm=admin)",
		Required: true,
	},
	&cli.StringFlag{
		Name:     "api-sector-index",
		Usage:    "miner sector Index API info (lotus-miner auth api-info --perm=admin)",
		Required: true,
	},
}

var initCmd = &cli.Command{
	Name:  "init",
	Usage: "Initialize a boost repository",
	Flags: append(minerApiFlags, []cli.Flag{
		&cli.StringFlag{
			Name:     "wallet-publish-storage-deals",
			Usage:    "wallet to be used for PublishStorageDeals messages",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "wallet-deal-collateral",
			Usage:    "wallet to be used for deal collateral",
			Required: true,
		},
		&cli.Int64Flag{
			Name:     "max-staging-deals-bytes",
			Usage:    "max size for staging area in bytes",
			Required: true,
		},
	}...),
	Before: before,
	Action: func(cctx *cli.Context) error {
		ctx := scliutil.ReqContext(cctx)

		bp, err := initBoost(ctx, cctx, nil)
		if err != nil {
			return err
		}

		lr, err := bp.repo.Lock(repo.Boost)
		if err != nil {
			return err
		}
		defer lr.Close()

		ds, err := lr.Datastore(context.Background(), metadataNamespace)
		if err != nil {
			return err
		}

		fmt.Println("Creating boost config")
		var cerr error
		err = lr.SetConfig(func(raw interface{}) {
			rcfg, ok := raw.(*config.Boost)
			if !ok {
				cerr = errors.New("expected boost config")
				return
			}

			rcfg.ConfigVersion = config.CurrentVersion
			cerr = setMinerApiConfig(cctx, rcfg, true)
			if cerr != nil {
				return
			}
			setCommonConfig(cctx, rcfg, bp)
		})
		if cerr != nil {
			return cerr
		}
		if err != nil {
			return fmt.Errorf("setting config: %w", err)
		}

		// Add comments to config
		c, err := lr.Config()
		if err != nil {
			return fmt.Errorf("getting config: %w", err)
		}
		curCfg, ok := c.(*config.Boost)
		if !ok {
			return fmt.Errorf("parsing config from boost repo")
		}
		newCfg, err := config.ConfigUpdate(curCfg, config.DefaultBoost(), true, false)
		if err != nil {
			return err
		}
		err = os.WriteFile(path.Join(lr.Path(), "config.toml"), newCfg, 0644)
		if err != nil {
			return fmt.Errorf("writing config file %s: %w", string(newCfg), err)
		}

		// Add the miner address to the metadata datastore
		fmt.Printf("Adding miner address %s to datastore\n", bp.minerActor)
		err = addMinerAddressToDatastore(ds, bp.minerActor)
		if err != nil {
			return err
		}

		// Create an empty storage.json file
		fmt.Println("Creating empty storage.json file")
		err = os.WriteFile(path.Join(lr.Path(), "storage.json"), []byte("{}"), 0666)
		if err != nil {
			return fmt.Errorf("creating storage.json file: %w", err)
		}

		fmt.Println("Boost repo successfully created at " + lr.Path())
		fmt.Println("You can now start boost with 'boostd -vv run'")

		return nil
	},
}

type boostParams struct {
	repo       *lotus_repo.FsRepo
	minerActor address.Address
	walletPSD  address.Address
	walletCP   address.Address
}

func initBoost(ctx context.Context, cctx *cli.Context, marketsRepo lotus_repo.LockedRepo) (*boostParams, error) {
	fmt.Println("Initializing boost repo")

	walletPSD, err := address.NewFromString(cctx.String("wallet-publish-storage-deals"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse wallet-publish-storage-deals: %s; err: %w", cctx.String("wallet-publish-storage-deals"), err)
	}

	walletCP, err := address.NewFromString(cctx.String("wallet-deal-collateral"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse wallet-deal-collateral: %s; err: %w", cctx.String("wallet-deal-collateral"), err)
	}

	if cctx.Int64("max-staging-deals-bytes") <= 0 {
		return nil, fmt.Errorf("max size for staging deals area must be > 0 bytes")
	}

	fmt.Println("Trying to connect to full node RPC")
	if err := checkV1ApiSupport(ctx, cctx); err != nil {
		return nil, err
	}

	api, closer, err := lcli.GetFullNodeAPIV1(cctx)
	if err != nil {
		if strings.Contains(err.Error(), "could not get API info") {
			err = fmt.Errorf("%w\nDo you need to set the environment variable FULLNODE_API_INFO?", err)
		}
		return nil, err
	}
	defer closer()

	var minerActor address.Address
	if marketsRepo == nil {
		// If this is not a migration from an existing repo, just query the
		// miner directly for the actor address
		smApi, smCloser, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			if strings.Contains(err.Error(), "could not get API info") {
				err = fmt.Errorf("%w\nDo you need to set the environment variable MINER_API_INFO?", err)
			}
			return nil, err
		}
		defer smCloser()

		minerActor, err = smApi.ActorAddress(ctx)
		if err != nil {
			return nil, fmt.Errorf("getting miner actor address: %w", err)
		}
	} else {
		// This is a migration from an existing repo, so get the miner address
		// from the repo datastore
		ds, err := marketsRepo.Datastore(context.Background(), metadataNamespace)
		if err != nil {
			return nil, fmt.Errorf("getting legacy repo datastore: %w", err)
		}
		minerActor, err = getMinerAddressFromDatastore(ds)
		if err != nil {
			return nil, fmt.Errorf("getting miner actor address: %w", err)
		}
	}

	fmt.Println("Checking full node sync status")

	if err := lcli.SyncWait(ctx, &v0api.WrapperV1Full{FullNode: api}, false); err != nil {
		return nil, fmt.Errorf("sync wait: %w", err)
	}

	repoPath := cctx.String(FlagBoostRepo)
	fmt.Printf("Checking if repo exists at %s\n", repoPath)

	r, err := lotus_repo.NewFS(repoPath)
	if err != nil {
		return nil, err
	}

	ok, err := r.Exists()
	if err != nil {
		return nil, err
	}
	if ok {
		return nil, fmt.Errorf("repo at '%s' is already initialized", repoPath)
	}

	fmt.Println("Checking full node version")

	v, err := api.Version(ctx)
	if err != nil {
		return nil, err
	}

	if !v.APIVersion.EqMajorMinor(lapi.FullAPIVersion1) {
		msg := fmt.Sprintf("Remote API version didn't match (expected %s, remote %s)",
			lapi.FullAPIVersion1, v.APIVersion)
		return nil, fmt.Errorf(msg + ". Boost and Lotus Daemon must have the same API version")
	}

	fmt.Println("Creating boost repo")
	if err := r.Init(repo.Boost); err != nil {
		return nil, err
	}

	return &boostParams{
		repo:       r,
		minerActor: minerActor,
		walletPSD:  walletPSD,
		walletCP:   walletCP,
	}, nil
}

func setMinerApiConfig(cctx *cli.Context, rcfg *config.Boost, dialCheck bool) error {
	ctx := cctx.Context
	asi, err := checkApiInfo(ctx, cctx.String("api-sector-index"), dialCheck)
	if err != nil {
		return fmt.Errorf("checking sector index API: %w", err)
	}
	fmt.Printf("Sector index api info: %s\n", asi)
	rcfg.SectorIndexApiInfo = asi

	ai, err := checkApiInfo(ctx, cctx.String("api-sealer"), dialCheck)
	if err != nil {
		return fmt.Errorf("checking sealer API: %w", err)
	}

	fmt.Printf("Sealer api info: %s\n", ai)
	rcfg.SealerApiInfo = ai

	return nil
}

func setCommonConfig(cctx *cli.Context, rcfg *config.Boost, bp *boostParams) {
	rcfg.Dealmaking.MaxStagingDealsBytes = cctx.Int64("max-staging-deals-bytes")
	rcfg.Wallets.Miner = bp.minerActor.String()
	rcfg.Wallets.DealCollateral = bp.walletCP.String()
	rcfg.Wallets.PublishStorageDeals = bp.walletPSD.String()
}

var minerAddrDSKey = datastore.NewKey("miner-address")

func getMinerAddressFromDatastore(ds datastore.Batching) (address.Address, error) {
	addr, err := ds.Get(context.Background(), minerAddrDSKey)
	if err != nil {
		return address.Address{}, fmt.Errorf("getting miner address from legacy datastore: %w", err)
	}

	minerAddr, err := address.NewFromBytes(addr)
	if err != nil {
		return address.Address{}, fmt.Errorf("parsing miner address from legacy datastore: %w", err)
	}

	return minerAddr, nil
}

func addMinerAddressToDatastore(ds datastore.Batching, minerActor address.Address) error {
	return ds.Put(context.Background(), minerAddrDSKey, minerActor.Bytes())
}

// checkV1ApiSupport uses v0 api version to signal support for v1 API
// trying to query the v1 api on older lotus versions would get a 404, which can happen for any number of other reasons
func checkV1ApiSupport(ctx context.Context, cctx *cli.Context) error {
	// check v0 api version to make sure it supports v1 api
	api0, closer, err := lcli.GetFullNodeAPI(cctx)
	if err != nil {
		if strings.Contains(err.Error(), "could not get API info") {
			err = fmt.Errorf("%w\nDo you need to set the environment variable FULLNODE_API_INFO?", err)
		}
		return err
	}

	v, err := api0.Version(ctx)
	closer()

	if err != nil {
		return err
	}

	if !v.APIVersion.EqMajorMinor(lapi.FullAPIVersion0) {
		return fmt.Errorf("Remote API version didn't match (expected %s, remote %s)", lapi.FullAPIVersion0, v.APIVersion)
	}

	return nil
}

func checkApiInfo(ctx context.Context, ai string, dialCheck bool) (string, error) {
	ai = strings.TrimPrefix(strings.TrimSpace(ai), "MINER_API_INFO=")
	info := cliutil.ParseApiInfo(ai)
	addr, err := info.DialArgs("v0")
	if err != nil {
		return "", fmt.Errorf("could not get DialArgs: %w", err)
	}

	if !dialCheck {
		return ai, nil
	}

	fmt.Printf("Checking miner api version of %s\n", addr)
	api, closer, err := client.NewStorageMinerRPCV0(ctx, addr, info.AuthHeader())
	if err != nil {
		return "", err
	}
	defer closer()

	v, err := api.Version(ctx)
	if err != nil {
		return "", fmt.Errorf("checking version: %w", err)
	}

	if !v.APIVersion.EqMajorMinor(lapi.MinerAPIVersion0) {
		return "", fmt.Errorf("remote service API version didn't match (expected %s, remote %s)", lapi.MinerAPIVersion0, v.APIVersion)
	}

	return ai, nil
}
