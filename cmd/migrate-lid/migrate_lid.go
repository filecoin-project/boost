package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/boost/cmd/lib"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/extern/boostd-data/ldb"
	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/boost/extern/boostd-data/svc"
	"github.com/filecoin-project/boost/extern/boostd-data/svc/types"
	"github.com/filecoin-project/boost/extern/boostd-data/yugabyte"
	"github.com/filecoin-project/boost/extern/boostd-data/yugabyte/migrations"
	"github.com/filecoin-project/boost/markets/piecestore"
	"github.com/filecoin-project/boost/retrievalmarket/types/legacyretrievaltypes"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/node/modules"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/index"
	"github.com/mitchellh/go-homedir"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/schollz/progressbar/v3"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// The methods on the store that are used for migration
type StoreMigrationApi interface {
	Start(ctx context.Context) error
	IsIndexed(ctx context.Context, pieceCid cid.Cid) (bool, error)
	GetIndex(context.Context, cid.Cid) (<-chan types.IndexRecord, error)
	AddIndex(ctx context.Context, pieceCid cid.Cid, records []model.Record, isCompleteIndex bool) <-chan types.AddIndexProgress
	AddDealForPiece(ctx context.Context, pcid cid.Cid, info model.DealInfo) error
	ListPieces(ctx context.Context) ([]cid.Cid, error)
	GetPieceMetadata(ctx context.Context, pieceCid cid.Cid) (model.Metadata, error)
	GetPieceDeals(context.Context, cid.Cid) ([]model.DealInfo, error)
}

var desc = "It is recommended to do the dagstore migration while boost is running. " +
	"The dagstore migration may take several hours. It is safe to stop and restart " +
	"the process. It will continue from where it was stopped.\n" +
	"The pieceinfo migration must be done after boost has been shut down. " +
	"It takes a few minutes."

func checkMigrateType(migrateType string) error {
	if migrateType != "dagstore" && migrateType != "pieceinfo" {
		return fmt.Errorf("invalid migration type '%s': must be either dagstore or pieceinfo", migrateType)
	}
	return nil
}

var commonFlags = []cli.Flag{
	&cli.StringFlag{
		Name:     "force",
		Usage:    "if the index has already been migrated, overwrite it",
		Required: false,
	},
	&cli.IntFlag{
		Name:     "parallel",
		Usage:    "the number of indexes to be processed in parallel",
		Required: false,
		Value:    4,
	},
}

var migrateLevelDBCmd = &cli.Command{
	Name:        "leveldb",
	Description: "Migrate boost piece information and dagstore to a leveldb store.\n" + desc,
	Usage:       "migrate-lid leveldb dagstore|pieceinfo",
	Before:      before,
	Flags:       commonFlags,
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

var migrateYugabyteDBCmd = &cli.Command{
	Name:        "yugabyte",
	Description: "Migrate boost piece information and dagstore to a yugabyte store\n" + desc,
	Usage:       "migrate-lid yugabyte dagstore|pieceinfo",
	Before:      before,
	Flags: append(commonFlags, []cli.Flag{
		&cli.StringSliceFlag{
			Name:     "hosts",
			Usage:    "yugabyte hosts to connect to over cassandra interface eg '127.0.0.1'",
			Required: true,
		},
		&cli.StringFlag{
			Name:  "username",
			Usage: "yugabyte username to connect to over cassandra interface eg 'cassandra'",
		},
		&cli.StringFlag{
			Name:  "password",
			Usage: "yugabyte password to connect to over cassandra interface eg 'cassandra'",
		},
		&cli.StringFlag{
			Name:     "connect-string",
			Usage:    "postgres connect string eg 'postgresql://postgres:postgres@localhost'",
			Required: true,
		},
		&cli.IntFlag{
			Name:  "insert-parallelism",
			Usage: "the number of threads to use when inserting into the PayloadToPieces index",
			Value: 16,
		},
		&cli.IntFlag{
			Name:     "CQLTimeout",
			Usage:    "client timeout value in seconds for CQL queries",
			Required: false,
			Value:    yugabyte.CqlTimeout,
		},
	}...),
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

		// Create a connection to the yugabyte local index directory
		settings := yugabyte.DBSettings{
			Hosts:                    cctx.StringSlice("hosts"),
			Username:                 cctx.String("username"),
			Password:                 cctx.String("password"),
			ConnectString:            cctx.String("connect-string"),
			PayloadPiecesParallelism: cctx.Int("insert-parallelism"),
			CQLTimeout:               cctx.Int("CQLTimeout"),
		}

		// Note that it doesn't matter what address we pass here: because the
		// table is newly created, it doesn't contain any rows when the
		// migration is run.
		migrator := yugabyte.NewMigrator(settings, address.TestAddress)
		store := yugabyte.NewStore(settings, migrator)
		return migrate(cctx, "yugabyte", store, migrateType)
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

	fmt.Print("Migrating to " + dbType + " Local Index Directory. ")
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
		errCount, err := migrateIndices(ctx, logger, bar, repoDir, store, cctx.Bool("force"), cctx.Int("parallel"))
		if errCount > 0 {
			msg := fmt.Sprintf("Warning: there were errors migrating %d indices.", errCount)
			msg += " See the log for details:\n" + logPath
			fmt.Fprintf(os.Stderr, "\n%s\n", msg)
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
		_, _ = fmt.Fprintf(os.Stderr, "\n%s\n", msg)
	}
	if err != nil {
		return fmt.Errorf("migrating piece store: %w", err)
	}
	fmt.Println()
	return nil
}

type idxTime struct {
	t   time.Duration
	lck sync.Mutex
}

func migrateIndices(ctx context.Context, logger *zap.SugaredLogger, bar *progressbar.ProgressBar, repoDir string, store StoreMigrationApi, force bool, parallel int) (int64, error) {
	indicesPath := path.Join(repoDir, "dagstore", "index")
	logger.Infof("migrating dagstore indices at %s", indicesPath)

	idxPaths, err := getIndexPaths(indicesPath)
	if err != nil {
		return 0, err
	}

	logger.Infof("starting migration of %d dagstore indices", len(idxPaths))
	bar.ChangeMax(len(idxPaths))

	indicesStart := time.Now()
	var count int64
	var errCount int64
	var indexTime idxTime
	var processed int64

	queue := make(chan idxPath, len(idxPaths))
	for _, ipath := range idxPaths {
		queue <- ipath
	}
	close(queue)

	var eg errgroup.Group
	for i := 0; i < parallel; i++ {
		eg.Go(func() error {
			for ctx.Err() == nil {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case p, ok := <-queue:
					if !ok {
						// Finished adding all the queued items, exit the thread
						return nil
					}
					start := time.Now()

					indexed, perr := migrateIndexWithTimeout(ctx, p, store, force)
					bar.Add(1) //nolint:errcheck

					took := time.Since(start)
					indexTime.lck.Lock()
					indexTime.t += took
					indexTime.lck.Unlock()

					if perr != nil {
						logger.Errorw("migrate index failed", "piece cid", p.name, "took", took.String(), "err", perr)
						atomic.AddInt64(&errCount, 1)
					}

					if indexed {
						atomic.AddInt64(&count, 1)
						atomic.AddInt64(&processed, 1)
						logger.Infow("migrated index", "piece cid", p.name, "processed", atomic.LoadInt64(&processed), "total", len(idxPaths),
							"took", took.String(), "average", (indexTime.t / time.Duration(atomic.LoadInt64(&count))).String())

					} else {
						atomic.AddInt64(&processed, 1)
						logger.Infow("index already migrated", "piece cid", p.name, "processed", atomic.LoadInt64(&processed), "total", len(idxPaths))
					}
				}
			}
			return ctx.Err()
		})
	}

	err = eg.Wait()
	logger.Errorw("waiting for indexing threads to finish", err)

	logger.Infow("migrated indices", "total", len(idxPaths), "took", time.Since(indicesStart).String())
	return atomic.LoadInt64(&errCount), nil
}

type migrateIndexResult struct {
	Indexed bool
	Error   error
}

func migrateIndexWithTimeout(ctx context.Context, ipath idxPath, store StoreMigrationApi, force bool) (bool, error) {
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 60*time.Second)
	defer timeoutCancel()

	return execMigrateIndexWithTimeout(timeoutCtx, ipath, store, force)
}

func execMigrateIndexWithTimeout(ctx context.Context, ipath idxPath, store StoreMigrationApi, force bool) (bool, error) {
	result := make(chan migrateIndexResult, 1)
	go func() {
		result <- doMigrateIndex(ctx, ipath, store, force)
	}()
	select {
	case <-ctx.Done():
		return false, errors.New("index migration timed out after 60 seconds")
	case result := <-result:
		return result.Indexed, result.Error
	}
}

func doMigrateIndex(ctx context.Context, ipath idxPath, store StoreMigrationApi, force bool) migrateIndexResult {
	indexed, err := migrateIndex(ctx, ipath, store, force)
	return migrateIndexResult{
		Indexed: indexed,
		Error:   err,
	}
}

func migrateIndex(ctx context.Context, ipath idxPath, store StoreMigrationApi, force bool) (bool, error) {
	pieceCid, err := cid.Parse(ipath.name)
	if err != nil {
		return false, fmt.Errorf("parsing index name %s as cid: %w", ipath.name, err)
	}

	if !force {
		// Check if the index has already been migrated
		isIndexed, err := store.IsIndexed(ctx, pieceCid)
		if err != nil {
			return false, fmt.Errorf("checking if index %s is already migrated: %w", ipath.path, err)
		}
		if isIndexed {
			return false, nil
		}
	}

	// Load the index file
	readStart := time.Now()
	idx, err := loadIndex(ipath.path)
	if err != nil {
		return false, fmt.Errorf("loading index %s from disk: %w", ipath.path, err)
	}
	log.Debugw("ReadIndex", "took", time.Since(readStart).String())

	itidx, ok := idx.(index.IterableIndex)
	if !ok {
		return false, fmt.Errorf("index %s is not iterable for piece %s", ipath.path, pieceCid)
	}

	// Convert from IterableIndex to an array of records
	convStart := time.Now()
	records, err := getRecords(itidx)
	if err != nil {
		return false, fmt.Errorf("getting records for index %s: %w", ipath.path, err)
	}
	log.Debugw("ConvertIndex", "took", time.Since(convStart).String())

	// Add the index to the store
	addStart := time.Now()
	respch := store.AddIndex(ctx, pieceCid, records, false)
	for resp := range respch {
		if resp.Err != "" {
			return false, fmt.Errorf("adding index %s to store: %s", ipath.path, resp.Err)
		}
	}
	log.Debugw("AddIndex", "took", time.Since(addStart).String())

	return true, nil
}

func migratePieceStore(ctx context.Context, logger *zap.SugaredLogger, bar *progressbar.ProgressBar, repoDir string, store StoreMigrationApi) (int, error) {
	// Open the datastore in the existing repo
	ds, err := lib.OpenDataStore(repoDir)
	if err != nil {
		return 0, fmt.Errorf("creating piece store from repo %s: %w", repoDir, err)
	}

	// Get the miner address
	maddr, err := modules.MinerAddress(ds)
	if err != nil {
		return 0, fmt.Errorf("getting miner address from repo %s: %w", repoDir, err)
	}

	logger.Infof("migrating piece store deal information to Local Index Directory for miner %s", address.Address(maddr).String())
	start := time.Now()

	// Create a mapping of on-chain deal ID to deal proposal cid.
	// This is needed below so that we can map from the legacy piece store
	// info to a legacy deal.
	propCidByChainDealID, err := lib.GetPropCidByChainDealID(ctx, ds)
	if err != nil {
		return 0, fmt.Errorf("building chain deal id -> proposal cid map: %w", err)
	}

	ps, err := lib.OpenPieceStore(ctx, ds)
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
				DealUuid:     uuid,
				IsLegacy:     isLegacy,
				ChainDealID:  d.DealID,
				MinerAddr:    address.Address(maddr),
				SectorID:     d.SectorID,
				PieceOffset:  d.Offset,
				PieceLength:  d.Length,
				IsDirectDeal: false, // Explicitly set it to false as there should be no direct deals before this migration
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
		return nil, fmt.Errorf("wanted %v but got %v", multicodec.CarMultihashIndexSorted, idx.Codec())
	}
	return records, nil
}

type idxPath struct {
	name string
	path string
}

func getIndexPaths(pathDir string) ([]idxPath, error) {
	files, err := os.ReadDir(pathDir)
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
	idxf, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = idxf.Close()
	}()

	subject, err := index.ReadFrom(idxf)
	if err != nil {
		return nil, err
	}

	return subject, nil
}

var migrateReverseCmd = &cli.Command{
	Name:  "reverse",
	Usage: "Do a reverse migration from the local index directory back to the legacy format",
	Subcommands: []*cli.Command{
		migrateReverseLeveldbCmd,
		migrateReverseYugabyteCmd,
	},
}

var migrateReverseLeveldbCmd = &cli.Command{
	Name:   "leveldb",
	Usage:  "Reverse migrate a leveldb local index directory",
	Before: before,
	Action: func(cctx *cli.Context) error {
		return migrateReverse(cctx, "leveldb")
	},
}

var migrateReverseYugabyteCmd = &cli.Command{
	Name:   "yugabyte",
	Usage:  "Reverse migrate a yugabyte local index directory",
	Before: before,
	Flags: []cli.Flag{
		&cli.StringSliceFlag{
			Name:     "hosts",
			Usage:    "yugabyte hosts to connect to over cassandra interface eg '127.0.0.1'",
			Required: true,
		},
		&cli.StringFlag{
			Name:  "username",
			Usage: "yugabyte username to connect to over cassandra interface eg 'cassandra'",
		},
		&cli.StringFlag{
			Name:  "password",
			Usage: "yugabyte password to connect to over cassandra interface eg 'cassandra'",
		},
		&cli.StringFlag{
			Name:     "connect-string",
			Usage:    "postgres connect string eg 'postgresql://postgres:postgres@localhost'",
			Required: true,
		},
		&cli.IntFlag{
			Name:  "insert-parallelism",
			Usage: "the number of threads to use when inserting into the PayloadToPieces index",
			Value: 16,
		},
	},
	Action: func(cctx *cli.Context) error {
		return migrateReverse(cctx, "yugabyte")
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

	// Get a leveldb / yugabyte store
	var store StoreMigrationApi
	if dbType == "leveldb" {
		// Create a connection to the leveldb store
		ldbRepoPath, err := svc.MakeLevelDBDir(repoDir)
		if err != nil {
			return err
		}
		store = ldb.NewStore(ldbRepoPath)
	} else {
		// Create a connection to the yugabyte local index directory
		settings := yugabyte.DBSettings{
			ConnectString:            cctx.String("connect-string"),
			Hosts:                    cctx.StringSlice("hosts"),
			Username:                 cctx.String("username"),
			Password:                 cctx.String("password"),
			PayloadPiecesParallelism: cctx.Int("insert-parallelism"),
		}
		migrator := yugabyte.NewMigrator(settings, migrations.DisabledMinerAddr)
		store = yugabyte.NewStore(settings, migrator)
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
		return fmt.Errorf("listing local index directory pieces: %w", err)
	}

	logger.Infof("starting migration of %d piece infos from %s local index directory to piece store", len(pcids), dbType)

	// Open the datastore
	ds, err := lib.OpenDataStore(repoDir)
	if err != nil {
		return fmt.Errorf("creating datastore from repo %s: %w", repoDir, err)
	}

	// Open the Piece Store
	ps, err := lib.OpenPieceStore(ctx, ds)
	if err != nil {
		return fmt.Errorf("opening piece store: %w", err)
	}

	// For each piece in the local index directory
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
	// Get the piece metadata from the local index directory
	pieceDirPieceInfo, err := pieceDir.GetPieceMetadata(ctx, pieceCid)
	if err != nil {
		return 0, fmt.Errorf("getting piece metadata for piece %s", pieceCid)
	}

	// Get the deals from the piece metadata
	var pieceStoreDeals []piecestore.DealInfo
	pieceStorePieceInfo, err := ps.GetPieceInfo(pieceCid)
	if err != nil {
		if !errors.Is(err, legacyretrievaltypes.ErrNotFound) {
			return 0, fmt.Errorf("getting piece info from piece store for piece %s", pieceCid)
		}
	} else {
		pieceStoreDeals = pieceStorePieceInfo.Deals
	}

	// Iterate over each local index directory deal and add it to the piece store
	// if it's not there already
	var migrated int
	for _, pieceDirDeal := range pieceDirPieceInfo.Deals {
		// Check if the local index directory deal is already in the piece store
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
