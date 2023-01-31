package main

import (
	"context"
	"fmt"
	"github.com/filecoin-project/boost/cmd/lib"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/boost/piecedirectory/types"
	"github.com/filecoin-project/boostd-data/couchbase"
	"github.com/filecoin-project/boostd-data/ldb"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/boostd-data/svc"
	"github.com/filecoin-project/go-address"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/node/modules"
	"github.com/ipfs/go-cid"
	"github.com/mitchellh/go-homedir"
	"github.com/schollz/progressbar/v3"
	"github.com/urfave/cli/v2"
	"path"
	"time"
)

var rebuildIndexesCmd = &cli.Command{
	Name:  "rebuild-indexes",
	Usage: "Rebuild indexes by iterating over all deals from the sqlite database and reading the corresponding unsealed sector file",
	Subcommands: []*cli.Command{
		rebuildIndexesLeveldbCmd,
		rebuildIndexesCouchbaseCmd,
	},
}

var rebuildFlags = []cli.Flag{
	&cli.StringFlag{
		Name:     "api-fullnode",
		Usage:    "the endpoint for the full node API",
		Required: true,
	},
	&cli.StringFlag{
		Name:     "api-storage",
		Usage:    "the endpoint for the storage node API",
		Required: true,
	},
	&cli.IntFlag{
		Name:  "concurrency",
		Usage: "the number of indexes to rebuild concurrently",
		Value: 1,
	},
	&cli.BoolFlag{
		Name:  "rebuild-indexes",
		Usage: "whether to rebuild indexes (or just deal metadata)",
		Value: true,
	},
	&cli.BoolFlag{
		Name:  "rebuild-deal-metadata",
		Usage: "whether to rebuild deal metadata (or just indexes)",
		Value: true,
	},
	&cli.StringFlag{
		Name:  "from-timestamp",
		Usage: "only re-index deals created at or after from-timestamp (inclusive) in RFC-3339 date format",
	},
	&cli.StringFlag{
		Name:  "to-timestamp",
		Usage: "only re-index deals created before to-timestamp (exclusive) in RFC-3339 date format",
	},
}

var rebuildIndexesLeveldbCmd = &cli.Command{
	Name:   "leveldb",
	Usage:  "Rebuild indexes for a leveldb local index directory",
	Before: before,
	Flags:  rebuildFlags,
	Action: func(cctx *cli.Context) error {
		// Open the leveldb repo directory
		fmt.Println("Starting local index directory...")
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
		svcCtx, cancel := context.WithCancel(cctx.Context)
		defer cancel()
		err = store.Start(svcCtx)
		if err != nil {
			return fmt.Errorf("starting leveldb store: %w", err)
		}

		return rebuildIndexes(cctx, repoDir, "leveldb", store)
	},
}

var rebuildIndexesCouchbaseCmd = &cli.Command{
	Name:   "couchbase",
	Usage:  "Rebuild indexes for a couchbase local index directory",
	Before: before,
	Flags:  append(rebuildFlags, append(couchbaseFlags, couchbaseMemFlags...)...),
	Action: func(cctx *cli.Context) error {
		// Create a connection to the couchbase local index directory
		settings := readCouchbaseSettings(cctx)

		fmt.Println("Starting local index directory...")
		store := couchbase.NewStore(settings)
		svcCtx, cancel := context.WithCancel(cctx.Context)
		defer cancel()
		err := store.Start(svcCtx)
		if err != nil {
			return fmt.Errorf("starting couchbase store: %w", err)
		}

		repoDir, err := homedir.Expand(cctx.String(FlagBoostRepo))
		if err != nil {
			return err
		}
		return rebuildIndexes(cctx, repoDir, "couchbase", store)
	},
}

type pieceDealInfo struct {
	PieceCid cid.Cid
	model.DealInfo
}

func rebuildIndexes(cctx *cli.Context, repoDir string, dbType string, store types.Store) error {
	ctx := lcli.ReqContext(cctx)
	start := time.Now()
	rebuildDeal := cctx.Bool("rebuild-deal-metadata")
	rebuildIndexes := cctx.Bool("rebuild-indexes")
	var fromTimestamp time.Time
	var toTimestamp time.Time
	var err error
	if cctx.IsSet("from-timestamp") {
		fromTimestamp, err = time.Parse(time.RFC3339, cctx.String("from-timestamp"))
		if err != nil {
			return fmt.Errorf("parsing from-timestamp %s: %w", cctx.String("from-timestamp"), err)
		}
	}
	if cctx.IsSet("to-timestamp") {
		toTimestamp, err = time.Parse(time.RFC3339, cctx.String("to-timestamp"))
		if err != nil {
			return fmt.Errorf("parsing to-timestamp %s: %w", cctx.String("to-timestamp"), err)
		}
	}

	// Connect to the full node API
	fnApiInfo := cctx.String("api-fullnode")
	fullnodeApi, ncloser, err := lib.GetFullNodeApi(ctx, fnApiInfo, log)
	if err != nil {
		return fmt.Errorf("getting full node API: %w", err)
	}
	defer ncloser()

	// Connect to the storage API and create a sector accessor
	storageApiInfo := cctx.String("api-storage")
	sa, storageCloser, err := lib.CreateSectorAccessor(ctx, storageApiInfo, fullnodeApi, log)
	if err != nil {
		return err
	}
	defer storageCloser()

	// Create interface to local index directory
	pr := &piecedirectory.SectorAccessorAsPieceReader{SectorAccessor: sa}
	pieceDir := piecedirectory.NewPieceDirectory(store, pr, cctx.Int("concurrency"))

	// Open the datastore in the existing repo
	ds, lr, err := openDataStore(repoDir)
	if err != nil {
		return fmt.Errorf("opening datastore from repo %s: %w", repoDir, err)
	}

	// Get the miner address
	maddr, err := modules.MinerAddress(ds)
	_ = lr.Close()
	if err != nil {
		return fmt.Errorf("getting miner address from repo %s: %w", repoDir, err)
	}

	// Get all deals from the boost database
	fmt.Println("Reading deals from boost database...")
	dbPath := path.Join(repoDir, "boost.db?cache=shared")
	sqldb, err := db.SqlDB(dbPath)
	if err != nil {
		return fmt.Errorf("opening boost sqlite db: %w", err)
	}

	qry := "SELECT ID, PieceCID, ChainDealID, SectorID, Offset, Length, TransferSize FROM Deals"
	where := ""
	var args []interface{}
	if !fromTimestamp.IsZero() {
		where += "CreatedAt >= ?"
		args = append(args, fromTimestamp)
	}
	if !toTimestamp.IsZero() {
		if where != "" {
			where += " AND "
		}
		where += "CreatedAt < ?"
		args = append(args, toTimestamp)
	}
	if where != "" {
		qry += " WHERE " + where
	}
	qry += " ORDER BY CreatedAt asc"
	rows, err := sqldb.QueryContext(ctx, qry, args...)
	if err != nil {
		return fmt.Errorf("executing select on Deals: %w", err)
	}

	var boostDeals []pieceDealInfo
	for rows.Next() {
		var pieceCidStr string
		var deal model.DealInfo
		err := rows.Scan(&deal.DealUuid, &pieceCidStr, &deal.ChainDealID, &deal.SectorID, &deal.PieceOffset, &deal.PieceLength, &deal.CarLength)
		if err != nil {
			return fmt.Errorf("executing row scan: %w", err)
		}

		deal.MinerAddr = address.Address(maddr)

		pieceCid, err := cid.Parse(pieceCidStr)
		if err != nil {
			return fmt.Errorf("parsing piece cid %s: %w", pieceCidStr, err)
		}

		boostDeals = append(boostDeals, pieceDealInfo{PieceCid: pieceCid, DealInfo: deal})
	}

	// Create a logger that outputs to a file in the current working directory
	logPath := "rebuild-indexes-" + dbType + ".log"
	logger, err := createLogger(logPath)
	if err != nil {
		return err
	}
	fmt.Println("Rebuilding indexes with output to log file " + logPath)
	defer func() { fmt.Println() }()
	logger.Infof("starting migration of %d boost deals from boost db to %s local index directory", len(boostDeals), dbType)

	// Create a progress bar
	bar := progressbar.NewOptions(len(boostDeals),
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
	bar.Describe("Re-building indices...")

	// For each deal
	var errorCount int
	indexedPieces := make(map[string]struct{})
	for i, deal := range boostDeals {
		if ctx.Err() != nil {
			fmt.Println("rebuild indexes cancelled")
			return nil
		}

		if deal.SectorID == 0 {
			logger.Infow("skipping deal because sector id is zero (maybe deal errored out before handoff stage)",
				"pieceCid", deal.PieceCid, "deal id", deal.DealUuid, "index", i, "total", len(boostDeals))
			bar.Add(1) //nolint:errcheck
			continue
		}

		if rebuildIndexes {
			if _, ok := indexedPieces[deal.PieceCid.String()]; ok {
				logger.Infow("skipping deal because index was already rebuilt",
					"pieceCid", deal.PieceCid, "deal id", deal.DealUuid, "sector", deal.SectorID,
					"offset", deal.PieceOffset, "index", i, "total", len(boostDeals))
			} else {
				isIndexed, err := pieceDir.IsIndexed(ctx, deal.PieceCid)
				if err != nil {
					errorCount++
					logger.Errorw("failed to check index status for piece", "pieceCid", deal.PieceCid, "index", i, "total", len(boostDeals), "error", err)
				} else if isIndexed {
					logger.Infow("skipping piece as it has already been indexed", "pieceCid", deal.PieceCid, "index", i, "total", len(boostDeals))
					indexedPieces[deal.PieceCid.String()] = struct{}{}
				} else {
					// Rebuild the index for the piece
					err := pieceDir.AddIndexForPieceThrottled(ctx, deal.PieceCid, deal.DealInfo)
					if err != nil {
						errorCount++
						logger.Errorw("failed to rebuild index for piece", "pieceCid", deal.PieceCid, "index", i, "total", len(boostDeals), "error", err)
					} else {
						logger.Infow("rebuilt index for piece", "pieceCid", deal.PieceCid, "index", i, "total", len(boostDeals))
					}
					indexedPieces[deal.PieceCid.String()] = struct{}{}
				}
			}
		}

		if rebuildDeal {
			err = pieceDir.AddDealInfoForPiece(ctx, deal.PieceCid, deal.DealInfo)
			if err != nil {
				errorCount++
				logger.Errorw("failed to rebuild deal for piece", "pieceCid", deal.PieceCid, "index", i, "total", len(boostDeals), "error", err)
			} else {
				logger.Infow("rebuilt deal for piece", "pieceCid", deal.PieceCid, "deal", deal.DealUuid, "index", i, "total", len(boostDeals))
			}
		}

		bar.Add(1) //nolint:errcheck
	}

	logger.Infow("rebuild indexes complete", "count", len(boostDeals), "errors", errorCount, "took", time.Since(start))
	return nil
}
