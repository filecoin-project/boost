package main

import "github.com/urfave/cli/v2"

var commonFlags = []cli.Flag{
	&cli.IntFlag{
		Name:  "piece-add-parallelism",
		Value: 2,
	},
	&cli.IntFlag{
		Name:  "piece-count",
		Value: 30,
	},
	&cli.IntFlag{
		Name:  "blocks-per-piece",
		Value: 1024,
	},
	&cli.IntFlag{
		Name:  "bs-fetch-count",
		Value: 100,
	},
	&cli.IntFlag{
		Name:  "bs-fetch-parallelism",
		Value: 10,
	},
	&cli.IntFlag{
		Name:  "gs-fetch-count",
		Value: 10,
	},
	&cli.IntFlag{
		Name:  "gs-fetch-parallelism",
		Value: 3,
	},
}

func runOptsFromCctx(cctx *cli.Context) runOpts {
	return runOpts{
		pieceParallelism:          cctx.Int("piece-add-parallelism"),
		blocksPerPiece:            cctx.Int("blocks-per-piece"),
		pieceCount:                cctx.Int("piece-count"),
		bitswapFetchCount:         cctx.Int("bs-fetch-count"),
		bitswapFetchParallelism:   cctx.Int("bs-fetch-parallelism"),
		graphsyncFetchCount:       cctx.Int("gs-fetch-count"),
		graphsyncFetchParallelism: cctx.Int("gs-fetch-parallelism"),
	}
}
