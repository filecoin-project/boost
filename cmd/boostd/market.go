package main

import (
	"fmt"
	"github.com/docker/go-units"
	bcli "github.com/filecoin-project/boost/cli"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"os"
	"text/tabwriter"
	"time"
)

var storageDealsCmd = &cli.Command{
	Name:  "storage-deals",
	Usage: "Manage storage deals and related configuration",
	Subcommands: []*cli.Command{
		setAskCmd,
		getAskCmd,
	},
}

var setAskCmd = &cli.Command{
	Name:  "set-ask",
	Usage: "Configure the miner's ask",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "price",
			Usage:    "Set the price of the ask for unverified deals (specified as FIL / GiB / Epoch) to `PRICE`.",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "verified-price",
			Usage:    "Set the price of the ask for verified deals (specified as FIL / GiB / Epoch) to `PRICE`",
			Required: true,
		},
		&cli.StringFlag{
			Name:        "min-piece-size",
			Usage:       "Set minimum piece size (w/bit-padding, in bytes) in ask to `SIZE`",
			DefaultText: "256B",
			Value:       "256B",
		},
		&cli.StringFlag{
			Name:        "max-piece-size",
			Usage:       "Set maximum piece size (w/bit-padding, in bytes) in ask to `SIZE`",
			DefaultText: "miner sector size",
			Value:       "0",
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := lcli.DaemonContext(cctx)

		minerApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		boostApi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		pri, err := types.ParseFIL(cctx.String("price"))
		if err != nil {
			return err
		}

		vpri, err := types.ParseFIL(cctx.String("verified-price"))
		if err != nil {
			return err
		}

		dur, err := time.ParseDuration("720h0m0s")
		if err != nil {
			return xerrors.Errorf("cannot parse duration: %w", err)
		}

		qty := dur.Seconds() / float64(build.BlockDelaySecs)

		min, err := units.RAMInBytes(cctx.String("min-piece-size"))
		if err != nil {
			return xerrors.Errorf("cannot parse min-piece-size to quantity of bytes: %w", err)
		}

		if min < 256 {
			return xerrors.New("minimum piece size (w/bit-padding) is 256B")
		}

		max, err := units.RAMInBytes(cctx.String("max-piece-size"))
		if err != nil {
			return xerrors.Errorf("cannot parse max-piece-size to quantity of bytes: %w", err)
		}

		maddr, err := minerApi.ActorAddress(ctx)
		if err != nil {
			return err
		}

		ssize, err := minerApi.ActorSectorSize(ctx, maddr)
		if err != nil {
			return err
		}

		smax := int64(ssize)

		if max == 0 {
			max = smax
		}

		if max > smax {
			return xerrors.Errorf("max piece size (w/bit-padding) %s cannot exceed miner sector size %s", types.SizeStr(types.NewInt(uint64(max))), types.SizeStr(types.NewInt(uint64(smax))))
		}

		return boostApi.MarketSetAsk(ctx, types.BigInt(pri), types.BigInt(vpri), abi.ChainEpoch(qty), abi.PaddedPieceSize(min), abi.PaddedPieceSize(max))
	},
}

var getAskCmd = &cli.Command{
	Name:  "get-ask",
	Usage: "Print the miner's ask",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		ctx := lcli.DaemonContext(cctx)

		fnapi, closer, err := lcli.GetFullNodeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		boostApi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		sask, err := boostApi.MarketGetAsk(ctx)
		if err != nil {
			return err
		}

		var ask *storagemarket.StorageAsk
		if sask != nil && sask.Ask != nil {
			ask = sask.Ask
		}

		w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
		fmt.Fprintf(w, "Price per GiB/Epoch\tVerified\tMin. Piece Size (padded)\tMax. Piece Size (padded)\tExpiry (Epoch)\tExpiry (Appx. Rem. Time)\tSeq. No.\n")
		if ask == nil {
			fmt.Fprintf(w, "<miner does not have an ask>\n")

			return w.Flush()
		}

		head, err := fnapi.ChainHead(ctx)
		if err != nil {
			return err
		}

		dlt := ask.Expiry - head.Height()
		rem := "<expired>"
		if dlt > 0 {
			rem = (time.Second * time.Duration(int64(dlt)*int64(build.BlockDelaySecs))).String()
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%s\t%d\n", types.FIL(ask.Price), types.FIL(ask.VerifiedPrice), types.SizeStr(types.NewInt(uint64(ask.MinPieceSize))), types.SizeStr(types.NewInt(uint64(ask.MaxPieceSize))), ask.Expiry, rem, ask.SeqNo)

		return w.Flush()
	},
}
