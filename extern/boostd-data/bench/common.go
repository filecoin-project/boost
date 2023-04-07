package main

import (
	"github.com/urfave/cli/v2"
	"strconv"
	"strings"
)

var loadFlags = []cli.Flag{
	&cli.IntFlag{
		Name:  "piece-add-parallelism",
		Value: 2,
	},
	&cli.StringSliceFlag{
		Name:  "pieces",
		Usage: "Array of piece count / blocks per piece in the form '<piece count>x<blocks per piece>' (eg '10x128')",
		Value: cli.NewStringSlice("30x1024"),
	},
}

var bitswapFlags = []cli.Flag{
	&cli.IntFlag{
		Name:  "bs-fetch-count",
		Value: 100,
	},
	&cli.IntFlag{
		Name:  "bs-fetch-parallelism",
		Value: 10,
	},
}

var graphsyncFlags = []cli.Flag{
	&cli.IntFlag{
		Name:  "gs-fetch-count",
		Value: 10,
	},
	&cli.IntFlag{
		Name:  "gs-fetch-parallelism",
		Value: 3,
	},
}

var commonFlags = append(loadFlags, append(bitswapFlags, graphsyncFlags...)...)

const piecesUsageErr = "pieces parameter must be of the form <piece count>x<blocks per piece>"

func runOptsFromCctx(cctx *cli.Context) runOpts {
	pieces := cctx.StringSlice("pieces")
	addPiecesSpecs := make([]addPiecesSpec, 0, len(pieces))
	for _, pc := range pieces {
		countBlocks := strings.Split(pc, "x")
		if len(countBlocks) != 2 {
			panic(piecesUsageErr)
		}

		pieceCount, err := strconv.Atoi(countBlocks[0])
		if err != nil {
			panic(piecesUsageErr)
		}

		blocksPerPiece, err := strconv.Atoi(countBlocks[1])
		if err != nil {
			panic(piecesUsageErr)
		}

		addPiecesSpecs = append(addPiecesSpecs, addPiecesSpec{
			pieceCount:     pieceCount,
			blocksPerPiece: blocksPerPiece,
		})
	}

	return runOpts{
		addPiecesSpecs:            addPiecesSpecs,
		pieceParallelism:          cctx.Int("piece-add-parallelism"),
		bitswapFetchCount:         cctx.Int("bs-fetch-count"),
		bitswapFetchParallelism:   cctx.Int("bs-fetch-parallelism"),
		graphsyncFetchCount:       cctx.Int("gs-fetch-count"),
		graphsyncFetchParallelism: cctx.Int("gs-fetch-parallelism"),
	}
}
