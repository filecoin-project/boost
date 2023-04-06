package main

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/boostd-data/shared/cliutil"
	"github.com/ipfs/go-cid"
	carindex "github.com/ipld/go-car/v2/index"
	mh "github.com/multiformats/go-multihash"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
)

const sectorSize = 32 * 1024 * 1024 * 1024

type pieceBlock struct {
	PieceCid         cid.Cid
	PayloadMultihash mh.Multihash
}

type BenchDB interface {
	Name() string
	Init(ctx context.Context, distributed bool) error
	AddIndexRecords(ctx context.Context, pieceCid cid.Cid, recs []model.Record) error
	Cleanup(ctx context.Context) error
	GetBlockSample(ctx context.Context, count int) ([]pieceBlock, error)
	PiecesContainingMultihash(ctx context.Context, m mh.Multihash) ([]cid.Cid, error)
	GetOffsetSize(ctx context.Context, pieceCid cid.Cid, hash mh.Multihash) (*model.OffsetSize, error)
	GetIterableIndex(ctx context.Context, pieceCid cid.Cid) (carindex.IterableIndex, error)
}

type addPiecesSpec struct {
	pieceCount     int
	blocksPerPiece int
}

type runOpts struct {
	addPiecesSpecs            []addPiecesSpec
	pieceParallelism          int
	bitswapFetchCount         int
	bitswapFetchParallelism   int
	graphsyncFetchCount       int
	graphsyncFetchParallelism int
}

func run(ctx context.Context, db BenchDB, opts runOpts) error {
	metrics.GetOrRegisterCounter("postgres.run", nil).Inc(1)
	defer func(now time.Time) {
		metrics.GetOrRegisterResettingTimer("postgres.run.duration", nil).UpdateSince(now)
	}(time.Now())

	log.Infof("Running benchmark for %s", db.Name())

	// Add sample data to the database
	for _, pc := range opts.addPiecesSpecs {
		if err := addPieces(ctx, db, opts.pieceParallelism, pc.pieceCount, pc.blocksPerPiece); err != nil {
			return err
		}
	}

	// Run bitswap fetch simulation
	//if err := bitswapFetch(ctx, db, opts.bitswapFetchCount, opts.bitswapFetchParallelism); err != nil {
	//return err
	//}

	// Run graphsync fetch simulation
	//if err := graphsyncFetch(ctx, db, opts.graphsyncFetchCount, opts.graphsyncFetchParallelism); err != nil {
	//return err
	//}

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
			for _, pc := range opts.addPiecesSpecs {
				err = addPieces(ctx, db, opts.pieceParallelism, pc.pieceCount, pc.blocksPerPiece)
				if err != nil {
					return err
				}
			}
			return nil
		},
	}
}

func addPieces(ctx context.Context, db BenchDB, parallelism int, pieceCount int, blocksPerPiece int) error {
	log.Infow("Adding pieces", "pieceCount", pieceCount, "blocksPerPiece", blocksPerPiece)

	queue := make(chan struct{}, pieceCount)
	for i := 0; i < pieceCount; i++ {
		queue <- struct{}{}
	}
	close(queue)

	var lk sync.Mutex
	var totalCreateRecs, totalAddRecs time.Duration

	addStart := time.Now()
	var eg errgroup.Group
	baseCid := testutil.GenerateCid().Bytes()
	for i := 0; i < parallelism; i++ {
		eg.Go(func() error {
			for ctx.Err() == nil {
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
					metrics.GetOrRegisterResettingTimer("runner.gen-n-blocks", nil).UpdateSince(createRecsStart)
					totalCreateRecsDelta := time.Since(createRecsStart)

					// Add the records to the db
					addRecsStart := time.Now()
					pcid := testutil.GenerateCid()

					start := time.Now()
					err := db.AddIndexRecords(ctx, pcid, recs)
					if err != nil {
						return err
					}
					metrics.GetOrRegisterResettingTimer("runner.add-index-records", nil).UpdateSince(start)

					lk.Lock()
					totalCreateRecs += totalCreateRecsDelta
					totalAddRecs += time.Since(addRecsStart)
					lk.Unlock()
				}
			}

			return ctx.Err()
		})
	}

	err := eg.Wait()
	if err != nil {
		return err
	}

	duration := time.Since(addStart)
	log.Infow("Add piece complete",
		"pieceCount", pieceCount,
		"duration", duration.String(),
		"duration-ms", duration.Milliseconds(),
		"blockRate", float64(pieceCount*blocksPerPiece)/duration.Seconds(),
		"total-cpu-ms", (totalCreateRecs + totalAddRecs).Milliseconds(),
		"create-ms", totalCreateRecs.Milliseconds(),
		"add-ms", totalAddRecs.Milliseconds())

	metrics.GetOrRegisterResettingTimer("postgres.fixtures", nil).UpdateSince(addStart)

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
	log.Infow("bitswap simulation", "blockCount", count, "parallelism", parallelism)

	var lk sync.Mutex
	var mhLookupTotal, getOffsetSizeTotal time.Duration

	fetchStart := time.Now()
	err := executeFetch(ctx, db, count, parallelism, func(sample pieceBlock) error {
		mhLookupStart := time.Now()

		_, err := db.PiecesContainingMultihash(ctx, sample.PayloadMultihash)
		if err != nil {
			return err
		}
		mhLookupTotalDelta := time.Since(mhLookupStart)
		metrics.GetOrRegisterResettingTimer("runner.pieces-containing-multihash", nil).UpdateSince(mhLookupStart)

		getIdxStart := time.Now()
		_, err = db.GetOffsetSize(ctx, sample.PieceCid, sample.PayloadMultihash)
		if err != nil {
			metrics.GetOrRegisterResettingTimer("runner.get-offset-size.err", nil).UpdateSince(getIdxStart)
			return err
		}
		metrics.GetOrRegisterResettingTimer("runner.get-offset-size", nil).UpdateSince(getIdxStart)

		lk.Lock()
		defer lk.Unlock()
		mhLookupTotal += mhLookupTotalDelta
		getOffsetSizeTotal += time.Since(getIdxStart)

		return err
	})
	if err != nil {
		return err
	}

	duration := time.Since(fetchStart)
	log.Infow("bitswap simulation complete",
		"blockCount", count,
		"duration", duration.String(),
		"duration-ms", duration.Milliseconds(),
		"total-cpu-ms", (mhLookupTotal + getOffsetSizeTotal).Milliseconds(),
		"mh-lookup-ms", mhLookupTotal.Milliseconds(),
		"get-offset-size-ms", getOffsetSizeTotal.Milliseconds())
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
	log.Infow("graphsync simulation", "blockCount", count, "parallelism", parallelism)

	fetchStart := time.Now()
	var mhLookupTotal, getIdxTotal time.Duration
	err := executeFetch(ctx, db, count, parallelism, func(sample pieceBlock) error {
		mhLookupStart := time.Now()
		_, err := db.PiecesContainingMultihash(ctx, sample.PayloadMultihash)
		if err != nil {
			return err
		}
		mhLookupTotal += time.Since(mhLookupStart)
		metrics.GetOrRegisterResettingTimer("runner.pieces-containing-multihash", nil).UpdateSince(mhLookupStart)

		getIdxStart := time.Now()
		_, err = db.GetIterableIndex(ctx, sample.PieceCid)
		getIdxTotal += time.Since(getIdxStart)
		metrics.GetOrRegisterResettingTimer("runner.get-iterable-index", nil).UpdateSince(getIdxStart)
		return err
	})
	if err != nil {
		return err
	}

	duration := time.Since(fetchStart)
	log.Infow("graphsync simulation complete",
		"blockCount", count,
		"duration", duration.String(),
		"duration-ms", duration.Milliseconds(),
		"total-cpu-ms", (mhLookupTotal + getIdxTotal).Milliseconds(),
		"mh-lookup-ms", mhLookupTotal.Milliseconds(),
		"get-index-ms", getIdxTotal.Milliseconds())
	return nil
}

func executeFetch(ctx context.Context, db BenchDB, count int, parallelism int, processSample func(pieceBlock) error) error {
	log.Infow("generating block samples", "count", count, "parallelism", parallelism)
	start := time.Now()
	samples, err := db.GetBlockSample(ctx, count)
	if err != nil {
		return err
	}

	metrics.GetOrRegisterResettingTimer("runner.get-block-sample", nil).UpdateSince(start)

	log.Infow("generated block samples", "count", count, "parallelism", parallelism, "duration", time.Since(start).String())

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
