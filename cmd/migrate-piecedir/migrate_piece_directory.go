package main

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/filecoin-project/boost/piecemeta"
	"github.com/filecoin-project/boostd-data/client"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/boostd-data/svc"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	piecestoreimpl "github.com/filecoin-project/go-fil-markets/piecestore/impl"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/lib/backupds"
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
)

var migrateLevelDBCmd = &cli.Command{
	Name:        "leveldb",
	Description: "Migrate boost piece information and dagstore to a leveldb store",
	Before:      before,
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)

		// Create a logger for the migration that outputs to a file in the
		// current working directory
		logPath := "migrate-leveldb.log"
		logCfg := zap.NewDevelopmentConfig()
		logCfg.OutputPaths = []string{logPath}
		zl, err := logCfg.Build()
		if err != nil {
			return err
		}
		defer zl.Sync() //nolint:errcheck
		logger := zl.Sugar()

		repoDir, err := homedir.Expand(cctx.String(FlagBoostRepo))
		if err != nil {
			return err
		}

		fmt.Print("Migrating dagstore to leveldb Piece Directory. ")
		fmt.Println("See detailed logs of the migration at")
		fmt.Println(logPath)

		// Start a leveldb server
		ldbRepoPath := path.Join(repoDir, "piece-directory")
		addr, cleanup, err := svc.Setup(ctx, "ldb", ldbRepoPath)
		if err != nil {
			return err
		}
		defer cleanup()

		// Create a client to connect to the server
		cl, err := client.NewStore("http://" + addr)
		if err != nil {
			return err
		}

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

		// Migrate the indices
		bar.Describe("[cyan][1/3][reset] Migrating indices...")
		errCount, err := migrateIndices(ctx, logger, bar, repoDir, cl)
		if errCount > 0 {
			msg := fmt.Sprintf("Warning: there were errors migrating %d indices.", errCount)
			msg += " See the log for details:\n" + logPath
			fmt.Fprintf(os.Stderr, "\n"+msg+"\n")
		}
		// TODO: just log error
		if err != nil {
			return fmt.Errorf("migrating indices: %w", err)
		}

		// Migrate the piece store
		bar.Describe("[cyan][2/3][reset] Migrating piece info...")
		bar.Set(0) //nolint:errcheck
		errCount, err = migratePieceStore(ctx, logger, bar, repoDir, cl)
		if errCount > 0 {
			msg := fmt.Sprintf("Warning: there were errors migrating %d piece deal infos.", errCount)
			msg += " See the log for details:\n" + logPath
			fmt.Fprintf(os.Stderr, "\n"+msg+"\n")
		}
		// TODO: just log error
		if err != nil {
			return fmt.Errorf("migrating piece store: %w", err)
		}

		return nil
	},
}

func migrateIndices(ctx context.Context, logger *zap.SugaredLogger, bar *progressbar.ProgressBar, repoDir string, store *client.Store) (int, error) {
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

func migrateIndex(ctx context.Context, ipath idxPath, store *client.Store) (bool, error) {
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

func migratePieceStore(ctx context.Context, logger *zap.SugaredLogger, bar *progressbar.ProgressBar, repoDir string, store piecemeta.Store) (int, error) {
	logger.Infof("migrating piece store deal information to Piece Directory")
	start := time.Now()

	// Open the piece store in the existing repo
	ps, err := newPieceStore(repoDir)
	if err != nil {
		return 0, fmt.Errorf("creating piece store from repo %s: %w", repoDir, err)
	}

	// Wait for the piece store to be ready
	ch := make(chan error, 1)
	ps.OnReady(func(e error) {
		ch <- e
	})

	err = ps.Start(ctx)
	if err != nil {
		return 0, fmt.Errorf("starting piece store: %w", err)
	}

	err = <-ch
	if err != nil {
		return 0, fmt.Errorf("waiting for piece store to be ready: %w", err)
	}

	pcids, err := ps.ListPieceInfoKeys()
	if err != nil {
		return 0, fmt.Errorf("getting piece store keys: %w", err)
	}

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
			dealInfo := model.DealInfo{
				ChainDealID: d.DealID,
				SectorID:    d.SectorID,
				PieceOffset: d.Offset,
				PieceLength: d.Length,
			}

			err = store.AddDealForPiece(ctx, pcid, dealInfo)
			if err == nil {
				addedDeals = true
			} else {
				logger.Errorw("cant add deal for piece", "pcid", pcid, "err", err)
			}
		}

		if addedDeals {
			count++
		} else {
			errorCount++
		}
		took := time.Since(pieceStart)
		indexTime += took
		logger.Infow("migrated piece deals", "piece cid", pcid, "processed", i+1, "total", len(pcids),
			"took", took.String(), "average", (indexTime / time.Duration(count)).String())
	}

	logger.Infow("migrated piece deals", "count", len(pcids), "errors", errorCount, "took", time.Since(start))

	return errorCount, nil
}

func newPieceStore(path string) (piecestore.PieceStore, error) {
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

	ps, err := piecestoreimpl.NewPieceStore(namespace.Wrap(bds, datastore.NewKey("/storagemarket")))
	if err != nil {
		return nil, fmt.Errorf("creating piece store: %w", err)
	}

	return ps, nil
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
		log.Debugw("loadindex", "took", fmt.Sprintf("%s", time.Since(now)))
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
