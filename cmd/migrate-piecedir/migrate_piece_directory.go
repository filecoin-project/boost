package main

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boostd-data/couchbase"
	"github.com/filecoin-project/boostd-data/ldb"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/boostd-data/svc"
	"github.com/filecoin-project/go-address"
	vfsm "github.com/filecoin-project/go-ds-versioning/pkg/fsm"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	piecestoreimpl "github.com/filecoin-project/go-fil-markets/piecestore/impl"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-statemachine/fsm"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/lib/backupds"
	"github.com/filecoin-project/lotus/node/modules"
	"github.com/filecoin-project/lotus/node/repo"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipld/go-car/v2/index"
	"github.com/mitchellh/go-homedir"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/schollz/progressbar/v3"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

// The methods on the store that are used for migration
type StoreMigrationApi interface {
	Start(ctx context.Context) error
	IsIndexed(ctx context.Context, pieceCid cid.Cid) (bool, error)
	AddIndex(ctx context.Context, pieceCid cid.Cid, records []model.Record) error
	AddDealForPiece(ctx context.Context, pcid cid.Cid, info model.DealInfo) error
	ListPieces(ctx context.Context) ([]cid.Cid, error)
	GetPieceMetadata(ctx context.Context, pieceCid cid.Cid) (model.Metadata, error)
}

var desc = "It is recommended to do the dagstore migration while boost is running. " +
	"The dagstore migration may take several hours. It is safe to stop and restart " +
	"the process. It will continue from where it was stopped.\n" +
	"The pieceinfo migration must be done after boost has been shut down."

func checkMigrateType(migrateType string) error {
	if migrateType != "dagstore" && migrateType != "pieceinfo" {
		return fmt.Errorf("invalid migration type '%s': must be either dagstore or pieceinfo", migrateType)
	}
	return nil
}

var migrateLevelDBCmd = &cli.Command{
	Name:        "leveldb",
	Description: "Migrate boost piece information and dagstore to a leveldb store.\n" + desc,
	Usage:       "migrate-piecedir leveldb dagstore|pieceinfo",
	Before:      before,
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() == 0 {
			return fmt.Errorf("must specify either dagstore or pieceinfo migration")
		}

		// Get the type of migration (dagstore vs pieceinfo)
		migrateType := cctx.Args().Get(0)
		err := checkMigrateType(migrateType)
		if err != nil {
			return err
		}

		// Create the leveldb directory if it doesn't already exist
		repoDir, err := homedir.Expand(cctx.String(FlagBoostRepo))
		if err != nil {
			return err
		}

		repoPath, err := svc.MakeLevelDBDir(repoDir)
		if err != nil {
			return err
		}

		// Create a connection to the leveldb store
		store := ldb.NewStore(repoPath)
		return migrate(cctx, "leveldb", store, migrateType)
	},
}

var migrateCouchDBCmd = &cli.Command{
	Name:        "couchbase",
	Description: "Migrate boost piece information and dagstore to a couchbase store\n" + desc,
	Usage:       "migrate-piecedir couchbase dagstore|pieceinfo",
	Before:      before,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "connect-string",
			Usage:    "couchbase connect string eg 'couchbase://127.0.0.1'",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "username",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "password",
			Required: true,
		},
		&cli.Uint64Flag{
			Name:  "piece-meta-ram-quota-mb",
			Usage: "megabytes of ram allocated to piece metadata couchbase bucket (recommended at least 1024)",
			Value: 1024,
		},
		&cli.Uint64Flag{
			Name:  "mh-pieces-ram-quota-mb",
			Usage: "megabytes of ram allocated to multihash to piece cid couchbase bucket (recommended at least 1024)",
			Value: 1024,
		},
		&cli.Uint64Flag{
			Name:  "piece-offsets-ram-quota-mb",
			Usage: "megabytes of ram allocated to piece offsets couchbase bucket (recommended at least 1024)",
			Value: 1024,
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() == 0 {
			return fmt.Errorf("must specify either dagstore or pieceinfo migration")
		}

		// Get the type of migration (dagstore vs pieceinfo)
		migrateType := cctx.Args().Get(0)
		err := checkMigrateType(migrateType)
		if err != nil {
			return err
		}

		// Create a connection to the couchbase piece directory
		settings := couchbase.DBSettings{
			ConnectString: cctx.String("connect-string"),
			Auth: couchbase.DBSettingsAuth{
				Username: cctx.String("username"),
				Password: cctx.String("password"),
			},
			PieceMetadataBucket: couchbase.DBSettingsBucket{
				RAMQuotaMB: cctx.Uint64("piece-meta-ram-quota-mb"),
			},
			MultihashToPiecesBucket: couchbase.DBSettingsBucket{
				RAMQuotaMB: cctx.Uint64("mh-pieces-ram-quota-mb"),
			},
			PieceOffsetsBucket: couchbase.DBSettingsBucket{
				RAMQuotaMB: cctx.Uint64("piece-offsets-ram-quota-mb"),
			},
		}

		store := couchbase.NewStore(settings)
		return migrate(cctx, "couchbase", store, migrateType)
	},
}

func migrate(cctx *cli.Context, dbType string, store StoreMigrationApi, migrateType string) error {
	ctx := lcli.ReqContext(cctx)
	svcCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	fmt.Println("Setting up connection and creating data structures... this might take a minute...")
	err := store.Start(svcCtx)
	if err != nil {
		return fmt.Errorf("starting "+dbType+" store: %w", err)
	}

	// Create a logger for the migration that outputs to a file in the
	// current working directory
	logPath := "migrate-" + dbType + ".log"
	logger, err := createLogger(logPath)
	if err != nil {
		return err
	}

	repoDir, err := homedir.Expand(cctx.String(FlagBoostRepo))
	if err != nil {
		return err
	}

	fmt.Print("Migrating to " + dbType + " Piece Directory. ")
	fmt.Println("See detailed logs of the migration at")
	fmt.Println(logPath)

	// Create a progress bar
	bar := progressbar.NewOptions(100,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionFullWidth(),
		progressbar.OptionSetPredictTime(true),
		progressbar.OptionSetElapsedTime(false),
		progressbar.OptionShowCount(),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	if migrateType == "dagstore" {
		// Migrate the indices
		bar.Describe("Migrating indices...")
		errCount, err := migrateIndices(ctx, logger, bar, repoDir, store)
		if errCount > 0 {
			msg := fmt.Sprintf("Warning: there were errors migrating %d indices.", errCount)
			msg += " See the log for details:\n" + logPath
			fmt.Fprintf(os.Stderr, "\n"+msg+"\n")
		}
		if err != nil {
			return fmt.Errorf("migrating indices: %w", err)
		}
		fmt.Println()
		return nil
	}

	// Migrate the piece store
	bar.Describe("Migrating piece info...")
	bar.Set(0) //nolint:errcheck
	errCount, err := migratePieceStore(ctx, logger, bar, repoDir, store)
	if errCount > 0 {
		msg := fmt.Sprintf("Warning: there were errors migrating %d piece deal infos.", errCount)
		msg += " See the log for details:\n" + logPath
		fmt.Fprintf(os.Stderr, "\n"+msg+"\n")
	}
	if err != nil {
		return fmt.Errorf("migrating piece store: %w", err)
	}
	fmt.Println()
	return nil
}

func migrateIndices(ctx context.Context, logger *zap.SugaredLogger, bar *progressbar.ProgressBar, repoDir string, store StoreMigrationApi) (int, error) {
	indicesPath := path.Join(repoDir, "dagstore", "index")
	logger.Infof("migrating dagstore indices at %s", indicesPath)

	idxPaths, err := getIndexPaths(indicesPath)
	if err != nil {
		return 0, err
	}

	logger.Infof("starting migration of %d dagstore indices", len(idxPaths))
	bar.ChangeMax(len(idxPaths))

	indicesStart := time.Now()
	var count int
	var errCount int
	var indexTime time.Duration
	for i, ipath := range idxPaths {
		if ctx.Err() != nil {
			return errCount, fmt.Errorf("index migration cancelled")
		}

		start := time.Now()

		indexed, err := migrateIndex(ctx, ipath, store)
		bar.Add(1) //nolint:errcheck
		if err != nil {
			logger.Errorw("migrate index failed", "piece cid", ipath.name, "err", err)
			errCount++
			continue
		}

		if indexed {
			count++
			took := time.Since(start)
			indexTime += took
			logger.Infow("migrated index", "piece cid", ipath.name, "processed", i+1, "total", len(idxPaths),
				"took", took.String(), "average", (indexTime / time.Duration(count)).String())
		} else {
			logger.Infow("index already migrated", "piece cid", ipath.name, "processed", i+1, "total", len(idxPaths))
		}
	}

	logger.Infow("migrated indices", "total", len(idxPaths), "took", time.Since(indicesStart).String())
	return errCount, nil
}

func migrateIndex(ctx context.Context, ipath idxPath, store StoreMigrationApi) (bool, error) {
	pieceCid, err := cid.Parse(ipath.name)
	if err != nil {
		return false, fmt.Errorf("parsing index name %s as cid: %w", ipath.name, err)
	}

	// Check if the index has already been migrated
	isIndexed, err := store.IsIndexed(ctx, pieceCid)
	if err != nil {
		return false, fmt.Errorf("checking if index %s is already migrated: %w", ipath.path, err)
	}
	if isIndexed {
		return false, nil
	}

	// Load the index file
	idx, err := loadIndex(ipath.path)
	if err != nil {
		return false, fmt.Errorf("loading index %s from disk: %w", ipath.path, err)
	}

	itidx, ok := idx.(index.IterableIndex)
	if !ok {
		return false, fmt.Errorf("index %s is not iterable for piece %s", ipath.path, pieceCid)
	}

	// Convert from IterableIndex to an array of records
	records, err := getRecords(itidx)
	if err != nil {
		return false, fmt.Errorf("getting records for index %s: %w", ipath.path, err)
	}

	// Add the index to the store
	addStart := time.Now()
	err = store.AddIndex(ctx, pieceCid, records)
	if err != nil {
		return false, fmt.Errorf("adding index %s to store: %w", ipath.path, err)
	}
	log.Debugw("AddIndex", "took", time.Since(addStart).String())

	return true, nil
}

func migratePieceStore(ctx context.Context, logger *zap.SugaredLogger, bar *progressbar.ProgressBar, repoDir string, store StoreMigrationApi) (int, error) {
	// Open the datastore in the existing repo
	ds, err := openDataStore(repoDir)
	if err != nil {
		return 0, fmt.Errorf("creating piece store from repo %s: %w", repoDir, err)
	}

	// Get the miner address
	maddr, err := modules.MinerAddress(ds)
	if err != nil {
		return 0, fmt.Errorf("getting miner address from repo %s: %w", repoDir, err)
	}

	logger.Infof("migrating piece store deal information to Piece Directory for miner %s", address.Address(maddr).String())
	start := time.Now()

	// Create a mapping of on-chain deal ID to deal proposal cid.
	// This is needed below so that we can map from the legacy piece store
	// info to a legacy deal.
	propCidByChainDealID, err := getPropCidByChainDealID(ctx, ds)
	if err != nil {
		return 0, fmt.Errorf("building chain deal id -> proposal cid map: %w", err)
	}

	ps, err := openPieceStore(ctx, ds)
	if err != nil {
		return 0, fmt.Errorf("opening piece store: %w", err)
	}

	dbPath := path.Join(repoDir, "boost.db?cache=shared")
	sqldb, err := db.SqlDB(dbPath)
	if err != nil {
		return 0, fmt.Errorf("opening boost sqlite db: %w", err)
	}

	qry := "SELECT ID, ChainDealID FROM Deals"
	rows, err := sqldb.QueryContext(ctx, qry)
	if err != nil {
		return 0, fmt.Errorf("executing select on Deals: %w", err)
	}

	boostDeals := make(map[abi.DealID]string)

	for rows.Next() {
		var uuid string
		var chainDealId abi.DealID

		err := rows.Scan(&uuid, &chainDealId)
		if err != nil {
			return 0, fmt.Errorf("executing row scan: %w", err)
		}

		boostDeals[chainDealId] = uuid
	}

	pcids, err := ps.ListPieceInfoKeys()
	if err != nil {
		return 0, fmt.Errorf("getting piece store keys: %w", err)
	}

	// Ensure the same order in case the import is stopped and restarted
	sort.Slice(pcids, func(i, j int) bool {
		return pcids[0].String() < pcids[1].String()
	})

	logger.Infof("starting migration of %d piece infos", len(pcids))
	bar.ChangeMax(len(pcids))

	var indexTime time.Duration
	var count int
	var errorCount int
	for i, pcid := range pcids {
		bar.Add(1) //nolint:errcheck

		pieceStart := time.Now()

		pi, err := ps.GetPieceInfo(pcid)
		if err != nil {
			errorCount++
			logger.Errorw("cant get piece info for piece", "pcid", pcid, "err", err)
			continue
		}

		var addedDeals bool
		for _, d := range pi.Deals {
			// Find the deal corresponding to the deal info's DealID
			proposalCid, okLegacy := propCidByChainDealID[d.DealID]
			uuid, okBoost := boostDeals[d.DealID]

			if !okLegacy && !okBoost {
				logger.Errorw("cant find boost deal or legacy deal for piece",
					"pcid", pcid, "chain-deal-id", d.DealID, "err", err)
				continue
			}

			isLegacy := false
			if uuid == "" {
				uuid = proposalCid.String()
				isLegacy = true
			}

			dealInfo := model.DealInfo{
				DealUuid:    uuid,
				IsLegacy:    isLegacy,
				ChainDealID: d.DealID,
				MinerAddr:   address.Address(maddr),
				SectorID:    d.SectorID,
				PieceOffset: d.Offset,
				PieceLength: d.Length,
			}

			err = store.AddDealForPiece(ctx, pcid, dealInfo)
			if err == nil {
				addedDeals = true
			} else {
				logger.Errorw("cant add deal info for piece", "pcid", pcid, "chain-deal-id", d.DealID, "err", err)
			}
		}

		if addedDeals {
			count++
		} else {
			errorCount++
		}
		took := time.Since(pieceStart)
		indexTime += took
		avgDenom := count
		if avgDenom == 0 {
			avgDenom = 1
		}
		logger.Infow("migrated piece deals", "piece cid", pcid, "processed", i+1, "total", len(pcids),
			"took", took.String(), "average", (indexTime / time.Duration(avgDenom)).String())
	}

	logger.Infow("migrated piece deals", "count", len(pcids), "errors", errorCount, "took", time.Since(start))

	return errorCount, nil
}

func getPropCidByChainDealID(ctx context.Context, ds *backupds.Datastore) (map[abi.DealID]cid.Cid, error) {
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

func openDataStore(path string) (*backupds.Datastore, error) {
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

func getRecords(subject index.Index) ([]model.Record, error) {
	records := make([]model.Record, 0)

	switch idx := subject.(type) {
	case index.IterableIndex:
		err := idx.ForEach(func(m multihash.Multihash, offset uint64) error {

			cid := cid.NewCidV1(cid.Raw, m)

			records = append(records, model.Record{
				Cid: cid,
				OffsetSize: model.OffsetSize{
					Offset: offset,
					Size:   0,
				},
			})

			return nil
		})
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("wanted %v but got %v\n", multicodec.CarMultihashIndexSorted, idx.Codec())
	}
	return records, nil
}

type idxPath struct {
	name string
	path string
}

func getIndexPaths(pathDir string) ([]idxPath, error) {
	files, err := ioutil.ReadDir(pathDir)
	if err != nil {
		return nil, err
	}

	idxPaths := make([]idxPath, 0, len(files))
	for _, f := range files {
		name := f.Name()

		if strings.Contains(name, "full.idx") {
			filepath := pathDir + "/" + name
			name = strings.ReplaceAll(name, ".full.idx", "")

			idxPaths = append(idxPaths, idxPath{
				name: name,
				path: filepath,
			})
		}
	}

	return idxPaths, nil
}

func loadIndex(path string) (index.Index, error) {
	defer func(now time.Time) {
		log.Debugw("loadindex", "took", time.Since(now))
	}(time.Now())

	idxf, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer idxf.Close()

	subject, err := index.ReadFrom(idxf)
	if err != nil {
		return nil, err
	}

	return subject, nil
}

var migrateReverseCmd = &cli.Command{
	Name:  "reverse",
	Usage: "Do a reverse migration from the piece directory back to the legacy format",
	Subcommands: []*cli.Command{
		migrateReverseLeveldbCmd,
		migrateReverseCouchbaseCmd,
	},
}

var migrateReverseLeveldbCmd = &cli.Command{
	Name:   "leveldb",
	Usage:  "Reverse migrate a leveldb piece directory",
	Before: before,
	Action: func(cctx *cli.Context) error {
		return migrateReverse(cctx, "leveldb")
	},
}

var migrateReverseCouchbaseCmd = &cli.Command{
	Name:   "couchbase",
	Usage:  "Reverse migrate a couchbase piece directory",
	Before: before,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "connect-string",
			Usage:    "couchbase connect string eg 'couchbase://127.0.0.1'",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "username",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "password",
			Required: true,
		},
	},
	Action: func(cctx *cli.Context) error {
		return migrateReverse(cctx, "couchbase")
	},
}

func migrateReverse(cctx *cli.Context, dbType string) error {
	// Create a logger for the migration that outputs to a file in the
	// current working directory
	logPath := "reverse-migrate-" + dbType + ".log"
	logger, err := createLogger(logPath)
	if err != nil {
		return err
	}
	fmt.Printf("Performing %s reverse migration with logs at %s\n", dbType, logPath)

	repoDir, err := homedir.Expand(cctx.String(FlagBoostRepo))
	if err != nil {
		return err
	}

	// Get a leveldb / couchbase store
	var store StoreMigrationApi
	if dbType == "leveldb" {
		// Create a connection to the leveldb store
		ldbRepoPath, err := svc.MakeLevelDBDir(repoDir)
		if err != nil {
			return err
		}
		store = ldb.NewStore(ldbRepoPath)
	} else {
		// Create a connection to the couchbase piece directory
		settings := couchbase.DBSettings{
			ConnectString: cctx.String("connect-string"),
			Auth: couchbase.DBSettingsAuth{
				Username: cctx.String("username"),
				Password: cctx.String("password"),
			},
		}

		store = couchbase.NewStore(settings)
	}

	// Perform the reverse migration
	err = migrateDBReverse(cctx, repoDir, dbType, store, logger)
	if err != nil {
		return err
	}

	fmt.Println("Reverse migration complete")
	return nil
}

func migrateDBReverse(cctx *cli.Context, repoDir string, dbType string, pieceDir StoreMigrationApi, logger *zap.SugaredLogger) error {
	ctx := lcli.ReqContext(cctx)
	start := time.Now()

	svcCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	err := pieceDir.Start(svcCtx)
	if err != nil {
		return fmt.Errorf("starting "+dbType+" store: %w", err)
	}

	pcids, err := pieceDir.ListPieces(ctx)
	if err != nil {
		return fmt.Errorf("listing piece directory pieces: %w", err)
	}

	logger.Infof("starting migration of %d piece infos from %s piece directory to piece store", len(pcids), dbType)

	// Open the datastore
	ds, err := openDataStore(repoDir)
	if err != nil {
		return fmt.Errorf("creating datastore from repo %s: %w", repoDir, err)
	}

	// Open the Piece Store
	ps, err := openPieceStore(ctx, ds)
	if err != nil {
		return fmt.Errorf("opening piece store: %w", err)
	}

	// For each piece in the piece directory
	var errorCount int
	for i, pieceCid := range pcids {
		// Reverse migrate the piece
		migrated, err := migrateReversePiece(ctx, pieceCid, pieceDir, ps)
		if err != nil {
			errorCount++
			logger.Errorw("failed to reverse migrate piece", "pieceCid", pieceCid, "index", i, "total", len(pcids), "error", err)
		} else if migrated > 0 {
			logger.Infow("reverse migrated piece", "pieceCid", pieceCid, "index", i, "total", len(pcids), "migrated-deals", migrated)
		} else {
			logger.Infow("no deals to migrate for piece", "pieceCid", pieceCid, "index", i, "total", len(pcids))
		}
	}

	logger.Infow("reverse migration complete", "count", len(pcids), "errors", errorCount, "took", time.Since(start))
	return nil
}

func migrateReversePiece(ctx context.Context, pieceCid cid.Cid, pieceDir StoreMigrationApi, ps piecestore.PieceStore) (int, error) {
	// Get the piece metadata from the piece directory
	pieceDirPieceInfo, err := pieceDir.GetPieceMetadata(ctx, pieceCid)
	if err != nil {
		return 0, fmt.Errorf("getting piece metadata for piece %s", pieceCid)
	}

	// Get the deals from the piece metadata
	var pieceStoreDeals []piecestore.DealInfo
	pieceStorePieceInfo, err := ps.GetPieceInfo(pieceCid)
	if err != nil {
		if !errors.Is(err, retrievalmarket.ErrNotFound) {
			return 0, fmt.Errorf("getting piece info from piece store for piece %s", pieceCid)
		}
	} else {
		pieceStoreDeals = pieceStorePieceInfo.Deals
	}

	// Iterate over each piece directory deal and add it to the piece store
	// if it's not there already
	var migrated int
	for _, pieceDirDeal := range pieceDirPieceInfo.Deals {
		// Check if the piece directory deal is already in the piece store
		var has bool
		for _, pieceStoreDeal := range pieceStoreDeals {
			if pieceStoreDeal.SectorID == pieceDirDeal.SectorID &&
				pieceStoreDeal.Offset == pieceDirDeal.PieceOffset &&
				pieceStoreDeal.Length == pieceDirDeal.PieceLength {

				has = true
			}
		}
		if has {
			continue
		}

		// The piece store doesn't yet have the deal, so add it
		newDealInfo := piecestore.DealInfo{
			DealID:   pieceDirDeal.ChainDealID,
			SectorID: pieceDirDeal.SectorID,
			Offset:   pieceDirDeal.PieceOffset,
			Length:   pieceDirDeal.PieceLength,
		}

		// Note: the second parameter is ignored by the piece store
		// implementation
		err = ps.AddDealForPiece(pieceCid, cid.Undef, newDealInfo)
		if err != nil {
			return 0, fmt.Errorf("adding deal to piece store for piece %s: %w", pieceCid, err)
		}

		migrated++
	}

	return migrated, nil
}

func openPieceStore(ctx context.Context, ds *backupds.Datastore) (piecestore.PieceStore, error) {
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

func createLogger(logPath string) (*zap.SugaredLogger, error) {
	logCfg := zap.NewDevelopmentConfig()
	logCfg.OutputPaths = []string{logPath}
	zl, err := logCfg.Build()
	if err != nil {
		return nil, err
	}
	defer zl.Sync() //nolint:errcheck
	return zl.Sugar(), err
}
