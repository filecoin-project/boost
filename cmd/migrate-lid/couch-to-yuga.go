package main

import (
	"context"
	"fmt"
	"github.com/filecoin-project/boostd-data/couchbase"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/boostd-data/yugabyte"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/ipfs/go-cid"
	"github.com/schollz/progressbar/v3"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
	"os"
	"sort"
	"time"
)

var migrateCouchToYugaCmd = &cli.Command{
	Name:        "couch2yuga",
	Description: "Migrate from couchbase to yugabyte",
	Usage:       "migrate-lid couch2yuga [index|piecestore]",
	Before:      before,
	Flags: append(commonFlags, []cli.Flag{
		&cli.StringFlag{
			Name:     "couch-connect-string",
			Usage:    "couchbase connect string eg 'couchbase://127.0.0.1'",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "couch-username",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "couch-password",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "yuga-connect-string",
			Usage:    "yugabyte postgres connect string eg 'postgresql://postgres:postgres@127.0.0.1:5433'",
			Required: true,
		},
		&cli.StringSliceFlag{
			Name:     "yuga-hosts",
			Usage:    "yugabyte cassandra hosts eg '127.0.0.1'",
			Required: true,
		},
	}...),
	Action: func(cctx *cli.Context) error {
		migrateType := cctx.Args().First()

		// Create a connection to the couchbase local index directory
		couchSettings := couchbase.DBSettings{
			ConnectString: cctx.String("couch-connect-string"),
			Auth: couchbase.DBSettingsAuth{
				Username: cctx.String("couch-username"),
				Password: cctx.String("couch-password"),
			},
		}
		couchStore := couchbase.NewStore(couchSettings)

		yugaSettings := yugabyte.DBSettings{
			Hosts:         cctx.StringSlice("yuga-hosts"),
			ConnectString: cctx.String("yuga-connect-string"),
		}
		yugaStore := yugabyte.NewStore(yugaSettings)

		ctx := lcli.ReqContext(cctx)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		fmt.Println("Starting yugabyte")
		err := yugaStore.Start(ctx)
		if err != nil {
			return fmt.Errorf("starting yugabyte store: %w", err)
		}

		fmt.Println("Creating yugabyte tables")
		err = yugaStore.Create(ctx)
		if err != nil {
			return fmt.Errorf("creating yugabyte store tables: %w", err)
		}

		fmt.Println("Starting couchbase")
		err = couchStore.Start(ctx)
		if err != nil {
			return fmt.Errorf("starting couchbase store: %w", err)
		}

		// Create a logger for the migration that outputs to a file in the
		// current working directory
		logPath := "migrate-couch-to-yuga.log"
		logger, err := createLogger(logPath)
		if err != nil {
			return err
		}

		fmt.Print("Migrating from couchbase to yugabyte Local Index Directory. ")
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

		if migrateType == "" || migrateType == "index" {
			// Migrate the indices
			bar.Describe("Migrating indices...")
			errCount, err := migrateLidToLidIndices(ctx, logger, bar, couchStore, yugaStore, cctx.Bool("force"))
			if errCount > 0 {
				msg := fmt.Sprintf("Warning: there were errors migrating %d indices.", errCount)
				msg += " See the log for details:\n" + logPath
				fmt.Fprintf(os.Stderr, "\n"+msg+"\n")
			}
			if err != nil {
				return fmt.Errorf("migrating indices: %w", err)
			}
			fmt.Println()
		}

		if migrateType == "" || migrateType == "piecestore" {
			// Migrate the piece store
			bar.Describe("Migrating piece info...")
			bar.Set(0) //nolint:errcheck
			errCount, err := migrateLidToLidPieceStore(ctx, logger, bar, couchStore, yugaStore)
			if errCount > 0 {
				msg := fmt.Sprintf("Warning: there were errors migrating %d piece deal infos.", errCount)
				msg += " See the log for details:\n" + logPath
				fmt.Fprintf(os.Stderr, "\n"+msg+"\n")
			}
			if err != nil {
				return fmt.Errorf("migrating piece store: %w", err)
			}
			fmt.Println()
		}

		return nil
	},
}

func migrateLidToLidIndices(ctx context.Context, logger *zap.SugaredLogger, bar *progressbar.ProgressBar, source StoreMigrationApi, dest StoreMigrationApi, force bool) (int, error) {
	logger.Infof("migrating indices")

	pieces, err := source.ListPieces(ctx)
	if err != nil {
		return 0, fmt.Errorf("listing pieces: %w", err)
	}

	logger.Infof("starting migration of %d indices", len(pieces))
	bar.ChangeMax(len(pieces))

	// Ensure the same order in case the import is stopped and restarted
	sort.Slice(pieces, func(i, j int) bool {
		return pieces[0].String() < pieces[1].String()
	})

	indicesStart := time.Now()
	var count int
	var errCount int
	var indexTime time.Duration
	for i, pcid := range pieces {
		if ctx.Err() != nil {
			return errCount, fmt.Errorf("index migration cancelled")
		}

		start := time.Now()

		indexed, err := migrateLidToLidIndex(ctx, pcid, source, dest, force)
		bar.Add(1) //nolint:errcheck
		if err != nil {
			logger.Errorw("migrate index failed", "piece cid", pcid, "err", err)
			errCount++
			continue
		}

		if indexed {
			count++
			took := time.Since(start)
			indexTime += took
			logger.Infow("migrated index", "piece cid", pcid, "processed", i+1, "total", len(pieces),
				"took", took.String(), "average", (indexTime / time.Duration(count)).String())
		} else {
			logger.Infow("index already migrated", "piece cid", pcid, "processed", i+1, "total", len(pieces))
		}
	}

	logger.Infow("migrated indices", "total", len(pieces), "took", time.Since(indicesStart).String())
	return errCount, nil
}

func migrateLidToLidIndex(ctx context.Context, pieceCid cid.Cid, source StoreMigrationApi, dest StoreMigrationApi, force bool) (bool, error) {
	if !force {
		// Check if the index has already been migrated
		isIndexed, err := dest.IsIndexed(ctx, pieceCid)
		if err != nil {
			return false, fmt.Errorf("checking if index %s is already migrated: %w", pieceCid, err)
		}
		if isIndexed {
			return false, nil
		}
	}

	// Load the index from the source store
	idx, err := source.GetIndex(ctx, pieceCid)
	if err != nil {
		return false, fmt.Errorf("loading index %s: %w", pieceCid, err)
	}

	var records []model.Record
	for r := range idx {
		if r.Error != nil {
			return false, r.Error
		}
		records = append(records, r.Record)
	}

	// Add the index to the destination store
	addStart := time.Now()
	respch := dest.AddIndex(ctx, pieceCid, records, true)
	for resp := range respch {
		if resp.Err != "" {
			return false, fmt.Errorf("adding index %s to store: %s", pieceCid, err)
		}
	}
	log.Debugw("AddIndex", "took", time.Since(addStart).String())

	return true, nil
}

func migrateLidToLidPieceStore(ctx context.Context, logger *zap.SugaredLogger, bar *progressbar.ProgressBar, source StoreMigrationApi, dest *yugabyte.Store) (int, error) {
	logger.Infof("migrating piece store")
	start := time.Now()

	pieces, err := source.ListPieces(ctx)
	if err != nil {
		return 0, fmt.Errorf("listing pieces: %w", err)
	}

	logger.Infof("starting migration of %d pieces", len(pieces))
	bar.ChangeMax(len(pieces))

	// Ensure the same order in case the import is stopped and restarted
	sort.Slice(pieces, func(i, j int) bool {
		return pieces[0].String() < pieces[1].String()
	})

	var indexTime time.Duration
	var count int
	var errorCount int
	for i, pcid := range pieces {
		bar.Add(1) //nolint:errcheck

		pieceStart := time.Now()

		deals, err := source.GetPieceDeals(ctx, pcid)
		if err != nil {
			errorCount++
			logger.Errorw("cant get piece deals for piece", "pcid", pcid, "err", err)
			continue
		}

		var addedDeals bool
		for _, dealInfo := range deals {
			err = dest.AddDealForPiece(ctx, pcid, dealInfo)
			if err == nil {
				addedDeals = true
			} else {
				logger.Errorw("cant add deal info for piece", "pcid", pcid, "chain-deal-id", dealInfo.ChainDealID, "err", err)
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
		logger.Infow("migrated piece deals", "piece cid", pcid, "processed", i+1, "total", len(pieces),
			"took", took.String(), "average", (indexTime / time.Duration(avgDenom)).String())
	}

	logger.Infow("migrated piece deals", "count", len(pieces), "errors", errorCount, "took", time.Since(start))

	return errorCount, nil
}
