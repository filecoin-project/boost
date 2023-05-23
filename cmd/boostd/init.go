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
	"github.com/filecoin-project/boost/api"
	cliutil "github.com/filecoin-project/boost/cli/util"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/node/impl/backupmgr"
	"github.com/filecoin-project/boost/node/repo"
	"github.com/filecoin-project/boost/util"
	scliutil "github.com/filecoin-project/boostd-data/shared/cliutil"
	"github.com/filecoin-project/go-address"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/api/v0api"
	lcli "github.com/filecoin-project/lotus/cli"
	lotus_config "github.com/filecoin-project/lotus/node/config"
	lotus_modules "github.com/filecoin-project/lotus/node/modules"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	"github.com/gbrlsnchs/jwt/v3"
	"github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
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

var migrateFlags = []cli.Flag{
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
}

var migrateMarketsCmd = &cli.Command{
	Name:  "migrate-markets",
	Usage: "Migrate from an existing split markets (MRA) repo to Boost",
	Flags: append([]cli.Flag{
		&cli.StringFlag{
			Name:     "import-markets-repo",
			Usage:    "initialize boost from an existing split markets (MRA) repo",
			Required: true,
		}},
		migrateFlags...,
	),
	Before: before,
	Action: func(cctx *cli.Context) error {
		return migrate(cctx, false, cctx.String("import-markets-repo"))
	},
}

var migrateMonolithCmd = &cli.Command{
	Name:  "migrate-monolith",
	Usage: "Migrate from an existing monolith lotus-miner repo to Boost",
	Flags: append([]cli.Flag{
		&cli.StringFlag{
			Name:     "import-miner-repo",
			Usage:    "initialize boost from an existing monolith lotus-miner repo",
			Required: true,
		}},
		append(minerApiFlags, migrateFlags...)...,
	),
	Before: before,
	Action: func(cctx *cli.Context) error {
		return migrate(cctx, true, cctx.String("import-miner-repo"))
	},
}

func migrate(cctx *cli.Context, fromMonolith bool, mktsRepoPath string) error {
	ctx := scliutil.ReqContext(cctx)

	// Open markets repo
	fmt.Printf("Opening repo '%s'\n", mktsRepoPath)
	mktsRepo, err := getMarketsRepo(mktsRepoPath)
	if err != nil {
		return err
	}
	defer mktsRepo.Close() //nolint:errcheck

	// Initialize boost repo
	bp, err := initBoost(ctx, cctx, mktsRepo)
	if err != nil {
		return err
	}

	boostRepo, err := bp.repo.Lock(repo.Boost)
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
	fmt.Println("Migrating keystore")
	err = backupmgr.CopyKeysBetweenRepos(mktsRepo, boostRepo)
	if err != nil {
		return err
	}

	// Migrate config
	fmt.Println("Migrating markets config")
	err = migrateMarketsConfig(cctx, mktsRepo, boostRepo, bp, fromMonolith)
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

	// Create an auth token
	err = createAuthToken(bp.repo, boostRepo)
	if err != nil {
		return err
	}

	// Migrate DAG store
	err = migrateDAGStore(ctx, mktsRepo, boostRepo)
	if err != nil {
		return err
	}

	fmt.Println("Boost repo successfully created at " + boostRepo.Path())
	fmt.Println("You can now start boost with 'boostd -vv run'")

	return nil
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

func createAuthToken(boostRepo *lotus_repo.FsRepo, boostRepoLocked lotus_repo.LockedRepo) error {
	ks, err := boostRepoLocked.KeyStore()
	if err != nil {
		return fmt.Errorf("getting boost keystore: %w", err)
	}

	// Set up the API secret key in the keystore
	_, err = lotus_modules.APISecret(ks, boostRepoLocked)
	if err != nil {
		return fmt.Errorf("generating API token: %w", err)
	}

	// Get the API token from the repo
	_, err = boostRepo.APIToken()
	if err == nil {
		// If the token already exists, nothing more to do
		return nil
	}

	// Check if the error was because the token has not been created (expected)
	// or for some other reason
	if !errors.Is(err, lotus_repo.ErrNoAPIEndpoint) {
		return fmt.Errorf("getting API token for newly created boost repo: %w", err)
	}

	// The token does not exist, so create a new token
	p := lotus_modules.JwtPayload{
		Allow: api.AllPermissions,
	}

	// Get the API secret key
	key, err := ks.Get(lotus_modules.JWTSecretName)
	if err != nil {
		// This should never happen because it gets created by the APISecret
		// function above
		return fmt.Errorf("getting key %s from keystore to generate API token: %w",
			lotus_modules.JWTSecretName, err)
	}

	// Create the API token
	cliToken, err := jwt.Sign(&p, jwt.NewHS256(key.PrivateKey))
	if err != nil {
		return fmt.Errorf("signing JSW payload for API token: %w", err)
	}

	// Save the API token in the repo
	err = boostRepoLocked.SetAPIToken(cliToken)
	if err != nil {
		return fmt.Errorf("setting boost API token: %w", err)
	}

	return nil
}

func migrateDAGStore(ctx context.Context, mktsRepo lotus_repo.LockedRepo, boostRepo lotus_repo.LockedRepo) error {

	subdir := "dagstore"

	mktsSubdirPath := path.Join(mktsRepo.Path(), subdir)
	boostSubdirPath := path.Join(boostRepo.Path(), subdir)

	rawMktsCfg, err := mktsRepo.Config()
	if err != nil {
		return fmt.Errorf("getting markets repo config: %w", err)
	}
	mktsCfg, ok := rawMktsCfg.(*lotus_config.StorageMiner)
	if !ok {
		return fmt.Errorf("expected legacy markets config, got %T", rawMktsCfg)
	}

	if len(mktsCfg.DAGStore.RootDir) > 0 {
		fmt.Println("Not migrating the dagstore as a custom dagstore path is set. Please manually move or copy the dagstore to $BOOST_PATH/dagstore")
		return nil
	}

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

func migrateMarketsConfig(cctx *cli.Context, mktsRepo lotus_repo.LockedRepo, boostRepo lotus_repo.LockedRepo, bp *boostParams, fromMonolith bool) error {
	var cerr error
	err := boostRepo.SetConfig(func(raw interface{}) {
		rcfg, ok := raw.(*config.Boost)
		if !ok {
			cerr = errors.New("expected boost config")
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

		if !fromMonolith {
			// When migrating from a split markets process, copy across the API
			// listen address because we're going to replace the split markets
			// process with boost.
			// (When migrating from a monolith leave the defaults which are
			// different from lotus miner, so they won't clash).
			rcfg.Common.API = mktsCfg.Common.API
		}
		rcfg.Common.Backup = mktsCfg.Common.Backup
		rcfg.Common.Libp2p = mktsCfg.Common.Libp2p
		rcfg.Storage = config.StorageConfig{ParallelFetchLimit: mktsCfg.Storage.ParallelFetchLimit}
		setBoostDealMakingCfg(&rcfg.Dealmaking, mktsCfg)
		rcfg.LotusDealmaking = mktsCfg.Dealmaking
		rcfg.LotusFees = config.FeeConfig{
			MaxPublishDealsFee:     mktsCfg.Fees.MaxPublishDealsFee,
			MaxMarketBalanceAddFee: mktsCfg.Fees.MaxMarketBalanceAddFee,
		}
		rcfg.DAGStore = mktsCfg.DAGStore
		// Clear the DAG store root dir config, because the DAG store is no longer configurable in Boost
		// (it is always at <repo path>/dagstore
		rcfg.DAGStore.RootDir = ""
		rcfg.IndexProvider = config.IndexProviderConfig{
			Enable:               mktsCfg.IndexProvider.Enable,
			EntriesCacheCapacity: mktsCfg.IndexProvider.EntriesCacheCapacity,
			EntriesChunkSize:     mktsCfg.IndexProvider.EntriesChunkSize,
			TopicName:            mktsCfg.IndexProvider.TopicName,
			PurgeCacheOnStart:    mktsCfg.IndexProvider.PurgeCacheOnStart,
		}
		rcfg.IndexProvider.Enable = true // Enable index provider in Boost by default

		if fromMonolith {
			// If migrating from a monolith miner, read the sealing and
			// indexing endpoints from the command line parameters
			cerr = setMinerApiConfig(cctx, rcfg, false)
			if cerr != nil {
				return
			}
		} else {
			// If migrating from a split markets process, just copy across
			// the sealing and indexing endpoints.
			rcfg.SealerApiInfo = mktsCfg.Subsystems.SealerApiInfo
			rcfg.SectorIndexApiInfo = mktsCfg.Subsystems.SectorIndexApiInfo
		}
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

func setBoostDealMakingCfg(bdm *config.DealmakingConfig, mktsCfg *lotus_config.StorageMiner) {
	ldm := mktsCfg.Dealmaking
	bdm.ConsiderOnlineStorageDeals = ldm.ConsiderOnlineStorageDeals
	bdm.ConsiderOfflineStorageDeals = ldm.ConsiderOfflineStorageDeals
	bdm.ConsiderOnlineRetrievalDeals = ldm.ConsiderOnlineRetrievalDeals
	bdm.ConsiderOfflineRetrievalDeals = ldm.ConsiderOfflineRetrievalDeals
	bdm.ConsiderVerifiedStorageDeals = ldm.ConsiderVerifiedStorageDeals
	bdm.ConsiderUnverifiedStorageDeals = ldm.ConsiderUnverifiedStorageDeals
	bdm.PieceCidBlocklist = ldm.PieceCidBlocklist
	bdm.ExpectedSealDuration = config.Duration(ldm.ExpectedSealDuration)
	bdm.MaxDealStartDelay = config.Duration(ldm.MaxDealStartDelay)
	bdm.MaxProviderCollateralMultiplier = ldm.MaxProviderCollateralMultiplier
	bdm.MaxStagingDealsBytes = ldm.MaxStagingDealsBytes
	bdm.StartEpochSealingBuffer = ldm.StartEpochSealingBuffer
	bdm.Filter = ldm.Filter
	bdm.RetrievalFilter = ldm.RetrievalFilter
	bdm.RetrievalPricing = ldm.RetrievalPricing
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
