package main

import (
	"context"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/boostd-data/shared/cliutil"
	"github.com/ipfs/go-cid"
	carindex "github.com/ipld/go-car/v2/index"
	mh "github.com/multiformats/go-multihash"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
	"math/rand"
	"time"
)

const sectorSize = 32 * 1024 * 1024 * 1024

type pieceBlock struct {
	PieceCid         cid.Cid
	PayloadMultihash mh.Multihash
}

type BenchDB interface {
	Name() string
	Init(ctx context.Context) error
	AddIndexRecords(ctx context.Context, pieceCid cid.Cid, recs []model.Record) error
	Cleanup(ctx context.Context) error
	GetBlockSample(ctx context.Context, count int) ([]pieceBlock, error)
	PiecesContainingMultihash(ctx context.Context, m mh.Multihash) ([]cid.Cid, error)
	GetOffsetSize(ctx context.Context, pieceCid cid.Cid, hash mh.Multihash) (*model.OffsetSize, error)
	GetIterableIndex(ctx context.Context, pieceCid cid.Cid) (carindex.IterableIndex, error)
}

type runOpts struct {
	pieceParallelism          int
	blocksPerPiece            int
	pieceCount                int
	bitswapFetchCount         int
	bitswapFetchParallelism   int
	graphsyncFetchCount       int
	graphsyncFetchParallelism int
}

func run(ctx context.Context, db BenchDB, opts runOpts) error {
	log.Infof("Running benchmark for %s", db.Name())
	log.Infof("Initializing...")
	if err := db.Init(ctx); err != nil {
		return err
	}
	log.Infof("Initialized")

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := db.Cleanup(ctx); err != nil {
			log.Errorf("cleaning up database: %w", err)
		}
	}()

	// Add sample data to the database
	if err := addPieces(ctx, db, opts.pieceParallelism, opts.pieceCount, opts.blocksPerPiece); err != nil {
		return err
	}

	// Run bitswap fetch simulation
	if err := bitswapFetch(ctx, db, opts.bitswapFetchCount, opts.bitswapFetchParallelism); err != nil {
		return err
	}

	// Run graphsync fetch simulation
	if err := graphsyncFetch(ctx, db, opts.graphsyncFetchCount, opts.graphsyncFetchParallelism); err != nil {
		return err
	}

	return nil
}

func loadCmd(createDB func(context.Context, string) (BenchDB, error)) *cli.Command {
	return &cli.Command{
		Name:   "load",
		Before: before,
		Action: func(cctx *cli.Context) error {
			ctx := cliutil.ReqContext(cctx)
			db, err := createDB(ctx, cctx.String("connect-string"))
			if err != nil {
				return err
			}

			opts := runOptsFromCctx(cctx)
			return addPieces(ctx, db, opts.pieceParallelism, opts.pieceCount, opts.blocksPerPiece)
		},
	}
}

func addPieces(ctx context.Context, db BenchDB, parallelism int, pieceCount int, blocksPerPiece int) error {
	log.Infof("Adding %d pieces with %d blocks per piece...", pieceCount, blocksPerPiece)

	queue := make(chan struct{}, pieceCount)
	for i := 0; i < pieceCount; i++ {
		queue <- struct{}{}
	}
	close(queue)

	var totalCreateRecs, totalAddRecs time.Duration
	addStart := time.Now()
	var eg errgroup.Group
	baseCid := testutil.GenerateCid().Bytes()
	for i := 0; i < parallelism; i++ {
		eg.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case _, ok := <-queue:
					if !ok {
						return nil
					}

					// Create block records
					createRecsStart := time.Now()

					recs := make([]model.Record, 0, blocksPerPiece)
					for p := 0; p < blocksPerPiece; p++ {
						c, err := generateRandomCid(baseCid)
						if err != nil {
							return err
						}

						recs = append(recs, model.Record{
							Cid: c,
							OffsetSize: model.OffsetSize{
								Offset: uint64(rand.Intn(sectorSize)),
								Size:   uint64(rand.Intn(sectorSize)),
							},
						})
					}
					totalCreateRecs += time.Since(createRecsStart)

					// Add the records to the db
					addRecsStart := time.Now()
					pcid := testutil.GenerateCid()
					err := db.AddIndexRecords(ctx, pcid, recs)
					if err != nil {
						return err
					}
					totalAddRecs += time.Since(addRecsStart)
				}
			}
		})
	}

	err := eg.Wait()
	if err != nil {
		return err
	}

	log.Infof("Added %d pieces in %s", pieceCount, time.Since(addStart))
	log.Debugf("total CPU time %s", totalCreateRecs+totalAddRecs)
	log.Debugf("  created records in %s", totalCreateRecs)
	log.Debugf("  added records in %s", totalAddRecs)
	return nil
}

func bitswapCmd(createDB func(context.Context, string) (BenchDB, error)) *cli.Command {
	return &cli.Command{
		Name:   "bitswap",
		Before: before,
		Action: func(cctx *cli.Context) error {
			ctx := cliutil.ReqContext(cctx)
			db, err := createDB(ctx, cctx.String("connect-string"))
			if err != nil {
				return err
			}

			opts := runOptsFromCctx(cctx)
			return bitswapFetch(ctx, db, opts.bitswapFetchCount, opts.bitswapFetchParallelism)
		},
	}
}

func bitswapFetch(ctx context.Context, db BenchDB, count int, parallelism int) error {
	log.Infof("Bitswap simulation: fetching %d random blocks with parallelism %d...", count, parallelism)

	fetchStart := time.Now()
	var mhLookupTotal, getOffsetSizeTotal time.Duration
	err := executeFetch(ctx, db, count, parallelism, func(sample pieceBlock) error {
		mhLookupStart := time.Now()
		_, err := db.PiecesContainingMultihash(ctx, sample.PayloadMultihash)
		if err != nil {
			return err
		}
		mhLookupTotal += time.Since(mhLookupStart)

		getIdxStart := time.Now()
		_, err = db.GetOffsetSize(ctx, sample.PieceCid, sample.PayloadMultihash)
		getOffsetSizeTotal += time.Since(getIdxStart)
		return err
	})
	if err != nil {
		return err
	}

	log.Infof("Bitswap simulation completed in %s", time.Since(fetchStart))
	log.Debugf("total CPU time: %s", mhLookupTotal+getOffsetSizeTotal)
	log.Debugf("  multihash lookup total time: %s", mhLookupTotal)
	log.Debugf("  get offset / size total time: %s", getOffsetSizeTotal)
	return nil
}

func graphsyncCmd(createDB func(context.Context, string) (BenchDB, error)) *cli.Command {
	return &cli.Command{
		Name:   "graphsync",
		Before: before,
		Action: func(cctx *cli.Context) error {
			ctx := cliutil.ReqContext(cctx)
			db, err := createDB(ctx, cctx.String("connect-string"))
			if err != nil {
				return err
			}

			opts := runOptsFromCctx(cctx)
			return graphsyncFetch(ctx, db, opts.graphsyncFetchCount, opts.graphsyncFetchParallelism)
		},
	}
}

func graphsyncFetch(ctx context.Context, db BenchDB, count int, parallelism int) error {
	log.Infof("Graphsync simulation: fetching %d random blocks with parallelism %d...", count, parallelism)

	fetchStart := time.Now()
	var mhLookupTotal, getIdxTotal time.Duration
	err := executeFetch(ctx, db, count, parallelism, func(sample pieceBlock) error {
		mhLookupStart := time.Now()
		_, err := db.PiecesContainingMultihash(ctx, sample.PayloadMultihash)
		if err != nil {
			return err
		}
		mhLookupTotal += time.Since(mhLookupStart)

		getIdxStart := time.Now()
		_, err = db.GetIterableIndex(ctx, sample.PieceCid)
		getIdxTotal += time.Since(getIdxStart)
		return err
	})
	if err != nil {
		return err
	}

	log.Infof("Graphsync simulation completed in %s", time.Since(fetchStart))
	log.Debugf("total CPU time: %s", mhLookupTotal+getIdxTotal)
	log.Debugf("  multihash lookup total time: %s", mhLookupTotal)
	log.Debugf("  get index total time: %s", getIdxTotal)
	return nil
}

func executeFetch(ctx context.Context, db BenchDB, count int, parallelism int, processSample func(pieceBlock) error) error {
	samples, err := db.GetBlockSample(ctx, count)
	if err != nil {
		return err
	}

	sampleChan := make(chan pieceBlock, len(samples))
	for _, sample := range samples {
		sampleChan <- sample
	}
	close(sampleChan)

	var eg errgroup.Group
	for i := 0; i < parallelism; i++ {
		eg.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case sample, ok := <-sampleChan:
					if !ok {
						return nil
					}

					err = processSample(sample)
					if err != nil {
						return err
					}
				}
			}
		})
	}

	return eg.Wait()
}
