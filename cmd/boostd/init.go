package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/chzyer/readline"
	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/boost/cli/ctxutil"
	cliutil "github.com/filecoin-project/boost/cli/util"
	"github.com/filecoin-project/boost/node"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/util"
	"github.com/filecoin-project/go-address"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/api/v0api"
	lcli "github.com/filecoin-project/lotus/cli"
	lotus_config "github.com/filecoin-project/lotus/node/config"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	"github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"github.com/urfave/cli/v2"
)

const metadataNamespace = "/metadata"

var initCmd = &cli.Command{
	Name:  "init",
	Usage: "Initialize a boost repository",
	Flags: []cli.Flag{
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
		&cli.StringFlag{
			Name:     "wallet-publish-storage-deals",
			Usage:    "wallet to be used for PublishStorageDeals messages",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "wallet-collateral-pledge",
			Usage:    "wallet to be used for pledging collateral",
			Required: true,
		},
		&cli.Int64Flag{
			Name:     "max-staging-deals-bytes",
			Usage:    "max size for staging area in bytes",
			Required: true,
		},
	},
	Before: before,
	Action: func(cctx *cli.Context) error {
		ctx := ctxutil.ReqContext(cctx)

		bp, err := initBoost(ctx, cctx)
		if err != nil {
			return err
		}

		lr, err := bp.repo.Lock(node.Boost)
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
				cerr = fmt.Errorf("expected boost config")
				return
			}

			asi, err := checkApiInfo(ctx, cctx.String("api-sector-index"))
			if err != nil {
				cerr = fmt.Errorf("checking sector index API: %w", err)
				return
			}
			fmt.Printf("Sector index api info: %s\n", asi)
			rcfg.SectorIndexApiInfo = asi

			ai, err := checkApiInfo(ctx, cctx.String("api-sealer"))
			if err != nil {
				cerr = fmt.Errorf("checking sealer API: %w", err)
				return
			}
			fmt.Printf("Sealer api info: %s\n", ai)
			rcfg.SealerApiInfo = ai

			setCommonConfig(cctx, rcfg, bp)
		})
		if cerr != nil {
			return cerr
		}
		if err != nil {
			return fmt.Errorf("setting config: %w", err)
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

		fmt.Println("Boost repo successfully created, you can now start boost with 'boostd -vv run'")

		return nil
	},
}

var migrateCmd = &cli.Command{
	Name:  "migrate",
	Usage: "Migrate from an existing markets repo to Boost",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "import-markets-repo",
			Usage:    "initialize boost from an existing markets repo",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "wallet-publish-storage-deals",
			Usage:    "wallet to be used for PublishStorageDeals messages",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "wallet-collateral-pledge",
			Usage:    "wallet to be used for pledging collateral",
			Required: true,
		},
		&cli.Int64Flag{
			Name:     "max-staging-deals-bytes",
			Usage:    "max size for staging area in bytes",
			Required: true,
		},
	},
	Before: before,
	Action: func(cctx *cli.Context) error {
		ctx := ctxutil.ReqContext(cctx)

		// Open markets repo
		mktsRepoPath := cctx.String("import-markets-repo")
		fmt.Printf("Getting markets repo '%s'\n", mktsRepoPath)
		mktsRepo, err := getMarketsRepo(mktsRepoPath)
		if err != nil {
			return err
		}
		defer mktsRepo.Close() //nolint:errcheck

		// Initialize boost repo
		bp, err := initBoost(ctx, cctx)
		if err != nil {
			return err
		}

		boostRepo, err := bp.repo.Lock(node.Boost)
		if err != nil {
			return err
		}
		defer boostRepo.Close()

		ds, err := boostRepo.Datastore(context.Background(), metadataNamespace)
		if err != nil {
			return err
		}

		// Migrate datastore keys
		fmt.Println("Migrating datastore keys")
		err = migrateMarketsDatastore(ctx, ds, mktsRepo)
		if err != nil {
			return err
		}

		// Migrate keystore
		fmt.Println("Migrating markets keystore")
		err = migrateMarketsKeystore(mktsRepo, boostRepo)
		if err != nil {
			return err
		}

		// Migrate config
		fmt.Println("Migrating markets config")
		err = migrateMarketsConfig(cctx, mktsRepo, boostRepo, bp)
		if err != nil {
			return err
		}

		// Add the miner address to the metadata datastore
		fmt.Printf("Adding miner address %s to datastore\n", bp.minerActor)
		err = addMinerAddressToDatastore(ds, bp.minerActor)
		if err != nil {
			return err
		}

		// Copy the storage.json file if there is one, otherwise create an empty one
		err = migrateStorageJson(mktsRepo.Path(), boostRepo.Path())
		if err != nil {
			return err
		}

		// Migrate DAG store
		err = migrateDirectory(ctx, mktsRepo.Path(), boostRepo.Path(), "dagstore")
		if err != nil {
			return err
		}

		fmt.Println("Boost repo successfully created, you can now start boost with 'boostd -vv run'")

		return nil
	},
}

func migrateStorageJson(mktsRepoPath string, boostRepoPath string) error {
	mktsFilePath := path.Join(mktsRepoPath, "storage.json")
	boostFilePath := path.Join(boostRepoPath, "storage.json")

	// Read storage.json in the markets repo
	bz, err := os.ReadFile(mktsFilePath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("reading %s: %w", mktsFilePath, err)
		}

		// There is no storage.json in the markets repo, so create an empty one
		// in the Boost repo
		fmt.Println("Creating storage.json file")
		bz = []byte("{}")
	} else {
		fmt.Println("Migrating storage.json file")
	}

	// Write storage.json in the boost repo
	err = os.WriteFile(boostFilePath, bz, 0666)
	if err != nil {
		return fmt.Errorf("writing %s: %w", boostFilePath, err)
	}

	return nil
}

func migrateDirectory(ctx context.Context, mktsRepoPath string, boostRepoPath string, subdir string) error {
	mktsSubdirPath := path.Join(mktsRepoPath, subdir)
	boostSubdirPath := path.Join(boostRepoPath, subdir)
	dirInfo, err := os.Lstat(mktsSubdirPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("reading %s path %s", subdir, mktsSubdirPath)
	}

	// If it's a sym-link just copy the sym-link
	if dirInfo.Mode()&os.ModeSymlink == os.ModeSymlink {
		fmt.Printf("copying sym-link %s to %s\n", mktsSubdirPath, boostSubdirPath)
		cmd := exec.Command("cp", "-a", mktsSubdirPath, boostSubdirPath)
		err = cmd.Run()
		if err != nil {
			return fmt.Errorf("Copying sym-link %s %s to %s: %w", subdir, mktsSubdirPath, boostSubdirPath, err)
		}
		return nil
	}

	if !dirInfo.IsDir() {
		return fmt.Errorf("expected %s to be a directory but it's not", mktsSubdirPath)
	}

	dirSizeBytes, err := util.DirSize(mktsSubdirPath)
	if err != nil {
		return fmt.Errorf("getting size of %s: %w", mktsSubdirPath, err)
	}

	humanSize := humanize.Bytes(uint64(dirSizeBytes))
	fmt.Printf("%s directory size: %s\n", subdir, humanSize)

	// If the directory is small enough, just copy it
	if dirSizeBytes < 1024 {
		fmt.Printf("Copying %s to %s\n", mktsSubdirPath, boostSubdirPath)
		cmd := exec.Command("cp", "-r", mktsSubdirPath, boostSubdirPath)
		err = cmd.Run()
		if err != nil {
			return fmt.Errorf("Copying %s directory %s to %s: %w", subdir, mktsSubdirPath, boostSubdirPath, err)
		}
		return nil
	}

	cs := readline.NewCancelableStdin(os.Stdin)
	go func() {
		<-ctx.Done()
		cs.Close() // nolint:errcheck
	}()
	rl := bufio.NewReader(cs)
	for {
		fmt.Printf("%s directory size is %s. Copy [c] / Move [m] / Ignore [i]:\n", subdir, humanSize)

		line, _, err := rl.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return fmt.Errorf("boost initialize canceled: %w", err)
			}

			return fmt.Errorf("reading input: %w", err)
		}

		switch string(line) {
		case "c", "y":
			fmt.Printf("Copying %s to %s\n", mktsSubdirPath, boostSubdirPath)
			cmd := exec.Command("cp", "-r", mktsSubdirPath, boostSubdirPath)
			err = cmd.Run()
			if err != nil {
				return fmt.Errorf("Copying %s directory %s to %s: %w", subdir, mktsSubdirPath, boostSubdirPath, err)
			}
			return nil
		case "m":
			fmt.Printf("Moving %s to %s\n", mktsSubdirPath, boostSubdirPath)
			cmd := exec.Command("mv", mktsSubdirPath, boostSubdirPath)
			err = cmd.Run()
			if err != nil {
				return fmt.Errorf("Moving %s directory %s to %s: %w", subdir, mktsSubdirPath, boostSubdirPath, err)
			}
			return nil
		case "i":
			fmt.Printf("Not copying %s directory from markets to boost\n", subdir)
			return nil
		}
	}
}

func migrateMarketsConfig(cctx *cli.Context, mktsRepo lotus_repo.LockedRepo, boostRepo lotus_repo.LockedRepo, bp *boostParams) error {
	var cerr error
	err := boostRepo.SetConfig(func(raw interface{}) {
		rcfg, ok := raw.(*config.Boost)
		if !ok {
			cerr = fmt.Errorf("expected boost config")
			return
		}

		rawMktsCfg, err := mktsRepo.Config()
		if err != nil {
			cerr = fmt.Errorf("getting markets repo config: %w", err)
			return
		}
		mktsCfg, ok := rawMktsCfg.(*lotus_config.StorageMiner)
		if !ok {
			cerr = fmt.Errorf("expected legacy markets config, got %T", rawMktsCfg)
			return
		}

		rcfg.Common.API = mktsCfg.Common.API
		rcfg.Common.Backup = mktsCfg.Common.Backup
		rcfg.Common.Libp2p = mktsCfg.Common.Libp2p
		rcfg.Storage = mktsCfg.Storage
		rcfg.SealerApiInfo = mktsCfg.Subsystems.SealerApiInfo
		rcfg.SectorIndexApiInfo = mktsCfg.Subsystems.SectorIndexApiInfo
		rcfg.Dealmaking = boostDealMakingCfg(mktsCfg)
		rcfg.LotusDealmaking = mktsCfg.Dealmaking
		rcfg.LotusFees = mktsCfg.Fees
		rcfg.DAGStore = mktsCfg.DAGStore
		rcfg.IndexProvider = mktsCfg.IndexProvider
		rcfg.IndexProvider.Enable = true // Enable index provider in Boost by default

		setCommonConfig(cctx, rcfg, bp)
	})
	if cerr != nil {
		return cerr
	}
	if err != nil {
		return fmt.Errorf("setting config: %w", err)
	}

	return nil
}

type boostParams struct {
	repo       *lotus_repo.FsRepo
	minerActor address.Address
	walletPSD  address.Address
	walletCP   address.Address
}

func initBoost(ctx context.Context, cctx *cli.Context) (*boostParams, error) {
	fmt.Println("Initializing boost repo")

	walletPSD, err := address.NewFromString(cctx.String("wallet-publish-storage-deals"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse wallet-publish-storage-deals: %s; err: %w", cctx.String("wallet-publish-storage-deals"), err)
	}

	walletCP, err := address.NewFromString(cctx.String("wallet-collateral-pledge"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse wallet-collateral-pledge: %s; err: %w", cctx.String("wallet-collateral-pledge"), err)
	}

	if walletPSD.String() == walletCP.String() {
		return nil, fmt.Errorf("wallets for PublishStorageDeals and pledging collateral must be different")
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

	smApi, smCloser, err := lcli.GetStorageMinerAPI(cctx)
	if err != nil {
		if strings.Contains(err.Error(), "could not get API info") {
			err = fmt.Errorf("%w\nDo you need to set the environment variable MINER_API_INFO?", err)
		}
		return nil, err
	}
	defer smCloser()

	minerActor, err := smApi.ActorAddress(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting miner actor address: %w", err)
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
	if err := r.Init(node.Boost); err != nil {
		return nil, err
	}

	return &boostParams{
		repo:       r,
		minerActor: minerActor,
		walletPSD:  walletPSD,
		walletCP:   walletCP,
	}, nil
}

func setCommonConfig(cctx *cli.Context, rcfg *config.Boost, bp *boostParams) {
	rcfg.Dealmaking.MaxStagingDealsBytes = cctx.Int64("max-staging-deals-bytes")
	rcfg.Wallets.Miner = bp.minerActor.String()
	rcfg.Wallets.PledgeCollateral = bp.walletCP.String()
	rcfg.Wallets.PublishStorageDeals = bp.walletPSD.String()
}

func addMinerAddressToDatastore(ds datastore.Batching, minerActor address.Address) error {
	return ds.Put(context.Background(), datastore.NewKey("miner-address"), minerActor.Bytes())
}

func boostDealMakingCfg(mktsCfg *lotus_config.StorageMiner) config.DealmakingConfig {
	ldm := mktsCfg.Dealmaking
	return config.DealmakingConfig{
		ConsiderOnlineStorageDeals:        ldm.ConsiderOnlineStorageDeals,
		ConsiderOfflineStorageDeals:       ldm.ConsiderOfflineStorageDeals,
		ConsiderOnlineRetrievalDeals:      ldm.ConsiderOnlineRetrievalDeals,
		ConsiderOfflineRetrievalDeals:     ldm.ConsiderOfflineRetrievalDeals,
		ConsiderVerifiedStorageDeals:      ldm.ConsiderVerifiedStorageDeals,
		ConsiderUnverifiedStorageDeals:    ldm.ConsiderUnverifiedStorageDeals,
		PieceCidBlocklist:                 ldm.PieceCidBlocklist,
		ExpectedSealDuration:              config.Duration(ldm.ExpectedSealDuration),
		MaxDealStartDelay:                 config.Duration(ldm.MaxDealStartDelay),
		PublishMsgPeriod:                  config.Duration(ldm.PublishMsgPeriod),
		PublishMsgMaxDealsPerMsg:          ldm.MaxDealsPerPublishMsg,
		PublishMsgMaxFee:                  mktsCfg.Fees.MaxPublishDealsFee,
		MaxProviderCollateralMultiplier:   ldm.MaxProviderCollateralMultiplier,
		MaxStagingDealsBytes:              ldm.MaxStagingDealsBytes,
		SimultaneousTransfersForStorage:   ldm.SimultaneousTransfersForStorage,
		SimultaneousTransfersForRetrieval: ldm.SimultaneousTransfersForRetrieval,
		StartEpochSealingBuffer:           ldm.StartEpochSealingBuffer,
		Filter:                            ldm.Filter,
		RetrievalFilter:                   ldm.RetrievalFilter,
		RetrievalPricing:                  ldm.RetrievalPricing,
	}
}

func getMarketsRepo(repoPath string) (lotus_repo.LockedRepo, error) {
	// Open the repo at the repo path
	mktsRepo, err := lotus_repo.NewFS(repoPath)
	if err != nil {
		return nil, fmt.Errorf("opening legacy markets repo %s: %w", repoPath, err)
	}

	// Make sure the repo exists
	exists, err := mktsRepo.Exists()
	if err != nil {
		return nil, fmt.Errorf("checking legacy markets repo %s exists: %w", repoPath, err)
	}
	if !exists {
		return nil, fmt.Errorf("legacy markets repo %s does not exist", repoPath)
	}

	// Lock the repo
	lr, err := mktsRepo.LockRO(lotus_repo.StorageMiner)
	if err != nil {
		return nil, fmt.Errorf("locking legacy markets repo %s: %w", repoPath, err)
	}
	return lr, nil
}

func migrateMarketsDatastore(ctx context.Context, boostDS datastore.Batching, mktsRepo lotus_repo.LockedRepo) error {
	// Open the metadata datastore on the repo
	mktsDS, err := mktsRepo.Datastore(ctx, metadataNamespace)
	if err != nil {
		return fmt.Errorf("opening datastore %s on legacy markets repo %s: %w",
			metadataNamespace, mktsRepo.Path(), err)
	}

	// Import the key / values from the markets metadata datastore
	prefixes := []string{
		// Storage deals
		"/deals/provider",
		// Retrieval deals
		"/retrievals/provider",
		// Piece store
		"/storagemarket",
	}
	for _, prefix := range prefixes {
		err := importPrefix(ctx, prefix, mktsDS, boostDS)
		if err != nil {
			return err
		}
	}

	return nil
}

func importPrefix(ctx context.Context, prefix string, mktsDS datastore.Batching, boostDS datastore.Batching) error {
	fmt.Printf("Importing all legacy markets datastore keys under %s\n", prefix)

	q, err := mktsDS.Query(ctx, dsq.Query{
		Prefix: prefix,
	})
	if err != nil {
		return fmt.Errorf("legacy markets datastore query: %w", err)
	}
	defer q.Close() //nolint:errcheck

	// Import keys in batches
	totalCount := 0
	batchSize := 1024
	results := q.Next()
	for {
		batch, err := boostDS.Batch(ctx)
		if err != nil {
			return fmt.Errorf("creating boost datastore batch: %w", err)
		}

		complete := false
		count := 0
		for ; count < batchSize; count++ {
			res, ok := <-results
			if !ok {
				complete = true
				break
			}

			err := batch.Put(ctx, datastore.NewKey(res.Key), res.Value)
			if err != nil {
				return fmt.Errorf("putting %s to Boost datastore: %w", res.Key, err)
			}
		}

		fmt.Printf("Importing %d legacy markets datastore keys\n", count)
		err = batch.Commit(ctx)
		if err != nil {
			return fmt.Errorf("saving %d datastore keys to Boost datastore: %w", count, err)
		}

		totalCount += count
		if complete {
			fmt.Printf("Imported %d legacy markets datastore keys under %s\n", totalCount, prefix)
			return nil
		}
	}
}

func migrateMarketsKeystore(mktsRepo lotus_repo.LockedRepo, boostRepo lotus_repo.LockedRepo) error {
	boostKS, err := boostRepo.KeyStore()
	if err != nil {
		return err
	}

	mktsKS, err := mktsRepo.KeyStore()
	if err != nil {
		return err
	}

	keys, err := mktsKS.List()
	if err != nil {
		return err
	}

	for _, k := range keys {
		ki, err := mktsKS.Get(k)
		if err != nil {
			return err
		}
		err = boostKS.Put(k, ki)
		if err != nil {
			return err
		}
	}

	return nil
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

func checkApiInfo(ctx context.Context, ai string) (string, error) {
	ai = strings.TrimPrefix(strings.TrimSpace(ai), "MINER_API_INFO=")
	info := cliutil.ParseApiInfo(ai)
	addr, err := info.DialArgs("v0")
	if err != nil {
		return "", fmt.Errorf("could not get DialArgs: %w", err)
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
