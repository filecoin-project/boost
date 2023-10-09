package main

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/filecoin-project/boost-gfm/stores"
	clinode "github.com/filecoin-project/boost/cli/node"
	"github.com/filecoin-project/boost/cmd"
	"github.com/filecoin-project/boost/cmd/lib"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/node/repo"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-commp-utils/writer"
	"github.com/filecoin-project/go-state-types/abi"
	markettypes "github.com/filecoin-project/go-state-types/builtin/v9/market"
	"github.com/filecoin-project/lotus/chain/actors"
	marketactor "github.com/filecoin-project/lotus/chain/actors/builtin/market"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/lib/backupds"
	"github.com/filecoin-project/lotus/lib/unixfs"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	"github.com/filecoin-project/lotus/node/repo/imports"
	"github.com/ipfs/go-cidutil/cidenc"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipld/go-car"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/multiformats/go-multibase"
	"github.com/urfave/cli/v2"
)

var (
	flagAssumeYes = &cli.BoolFlag{
		Name:    "assume-yes",
		Usage:   "automatic yes to prompts; assume 'yes' as answer to all prompts and run non-interactively",
		Aliases: []string{"y", "yes"},
	}
)

var marketAddCmd = &cli.Command{
	Name:        "market-add",
	Usage:       "Add funds to the Storage Market actor",
	Description: "Send signed message to add funds for the default wallet to the Storage Market actor. Uses 2x current BaseFee and a maximum fee of 1 nFIL. This is an experimental utility, do not use in production.",
	Flags: []cli.Flag{
		cmd.FlagRepo,
		flagAssumeYes,
		&cli.StringFlag{
			Name:  "wallet",
			Usage: "move balance from this wallet address to its market actor",
		},
	},
	ArgsUsage: "<amount>",
	Before:    before,
	Action: func(cctx *cli.Context) error {
		if !cctx.Args().Present() {
			return fmt.Errorf("must pass amount to add")
		}
		f, err := types.ParseFIL(cctx.Args().First())
		if err != nil {
			return fmt.Errorf("parsing 'amount' argument: %w", err)
		}

		amt := abi.TokenAmount(f)

		ctx := lcli.ReqContext(cctx)

		n, err := clinode.Setup(cctx.String(cmd.FlagRepo.Name))
		if err != nil {
			return err
		}

		api, closer, err := lcli.GetGatewayAPI(cctx)
		if err != nil {
			return fmt.Errorf("cant setup gateway connection: %w", err)
		}
		defer closer()

		walletAddr, err := n.GetProvidedOrDefaultWallet(ctx, cctx.String("wallet"))
		if err != nil {
			return err
		}

		log.Infow("selected wallet", "wallet", walletAddr)

		params, err := actors.SerializeParams(&walletAddr)
		if err != nil {
			return err
		}

		msg := &types.Message{
			To:     marketactor.Address,
			From:   walletAddr,
			Value:  amt,
			Method: marketactor.Methods.AddBalance,
			Params: params,
		}

		cid, sent, err := lib.SignAndPushToMpool(cctx, ctx, api, n, msg)
		if err != nil {
			return err
		}
		if !sent {
			return nil
		}

		log.Infow("submitted market-add message", "cid", cid.String())

		return nil
	},
}

var marketWithdrawCmd = &cli.Command{
	Name:        "market-withdraw",
	Usage:       "Withdraw funds from the Storage Market actor",
	Description: "",
	Flags: []cli.Flag{
		cmd.FlagRepo,
		flagAssumeYes,
		&cli.StringFlag{
			Name:  "wallet",
			Usage: "move balance to this wallet address from its market actor",
		},
	},
	ArgsUsage: "<amount>",
	Before:    before,
	Action: func(cctx *cli.Context) error {
		if !cctx.Args().Present() {
			return fmt.Errorf("must pass amount to add")
		}
		f, err := types.ParseFIL(cctx.Args().First())
		if err != nil {
			return fmt.Errorf("parsing 'amount' argument: %w", err)
		}

		amt := abi.TokenAmount(f)

		ctx := lcli.ReqContext(cctx)

		n, err := clinode.Setup(cctx.String(cmd.FlagRepo.Name))
		if err != nil {
			return err
		}

		api, closer, err := lcli.GetGatewayAPI(cctx)
		if err != nil {
			return fmt.Errorf("cant setup gateway connection: %w", err)
		}
		defer closer()

		walletAddr, err := n.GetProvidedOrDefaultWallet(ctx, cctx.String("wallet"))
		if err != nil {
			return err
		}

		log.Infow("selected wallet", "wallet", walletAddr)

		params, err := actors.SerializeParams(&markettypes.WithdrawBalanceParams{
			ProviderOrClientAddress: walletAddr,
			Amount:                  amt,
		})
		if err != nil {
			return err
		}

		msg := &types.Message{
			To:     marketactor.Address,
			From:   walletAddr,
			Value:  types.NewInt(0),
			Method: marketactor.Methods.WithdrawBalance,
			Params: params,
		}

		cid, sent, err := lib.SignAndPushToMpool(cctx, ctx, api, n, msg)
		if err != nil {
			return err
		}
		if !sent {
			return nil
		}

		log.Infow("submitted market-withdraw message", "cid", cid.String())

		return nil
	},
}

var commpCmd = &cli.Command{
	Name:      "commp",
	Usage:     "",
	ArgsUsage: "<inputPath>",
	Before:    before,
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() != 1 {
			return fmt.Errorf("usage: commP <inputPath>")
		}

		inPath := cctx.Args().Get(0)

		rdr, err := os.Open(inPath)
		if err != nil {
			return err
		}
		defer rdr.Close() //nolint:errcheck

		w := &writer.Writer{}
		_, err = io.CopyBuffer(w, rdr, make([]byte, writer.CommPBuf))
		if err != nil {
			return fmt.Errorf("copy into commp writer: %w", err)
		}

		commp, err := w.Sum()
		if err != nil {
			return fmt.Errorf("computing commP failed: %w", err)
		}

		encoder := cidenc.Encoder{Base: multibase.MustNewEncoder(multibase.Base32)}

		stat, err := os.Stat(inPath)
		if err != nil {
			return err
		}

		fmt.Println("CommP CID: ", encoder.Encode(commp.PieceCID))
		fmt.Println("Piece size: ", types.NewInt(uint64(commp.PieceSize.Unpadded().Padded())))
		fmt.Println("Car file size: ", stat.Size())
		return nil
	},
}

var generateRandCar = &cli.Command{
	Name:      "generate-rand-car",
	Usage:     "creates a randomly generated dense car",
	ArgsUsage: "<outputPath>",
	Before:    before,
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:    "size",
			Aliases: []string{"s"},
			Usage:   "The size of the data to turn into a car",
			Value:   8000000,
		},
		&cli.IntFlag{
			Name:    "chunksize",
			Aliases: []string{"c"},
			Value:   512,
			Usage:   "Size of chunking that should occur",
		},
		&cli.IntFlag{
			Name:    "maxlinks",
			Aliases: []string{"l"},
			Value:   8,
			Usage:   "Max number of leaves per level",
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() < 1 {
			return fmt.Errorf("usage: generate-car <outputPath> -size -chunksize -maxleaves")
		}

		outPath := cctx.Args().Get(0)
		size := cctx.Int("size")
		cs := cctx.Int64("chunksize")
		ml := cctx.Int("maxlinks")

		rf, err := testutil.CreateRandomFile(outPath, int(time.Now().Unix()), size)
		if err != nil {
			return err
		}

		// carv1
		caropts := []carv2.Option{
			blockstore.WriteAsCarV1(true),
		}

		root, cn, err := testutil.CreateDenseCARWith(outPath, rf, cs, ml, caropts)
		if err != nil {
			return err
		}

		err = os.Remove(rf)
		if err != nil {
			return err
		}

		encoder := cidenc.Encoder{Base: multibase.MustNewEncoder(multibase.Base32)}
		rn := encoder.Encode(root)
		base := path.Dir(cn)
		np := path.Join(base, rn+".car")

		err = os.Rename(cn, np)
		if err != nil {
			return err
		}

		fmt.Printf("Payload CID: %s, written to: %s\n", rn, np)

		return nil
	},
}

var generatecarCmd = &cli.Command{
	Name:      "generate-car",
	Usage:     "",
	ArgsUsage: "<inputPath> <outputPath>",
	Before:    before,
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() != 2 {
			return fmt.Errorf("usage: generate-car <inputPath> <outputPath>")
		}

		inPath := cctx.Args().First()
		outPath := cctx.Args().Get(1)

		ctx := lcli.ReqContext(cctx)

		r := lotus_repo.NewMemory(nil)
		lr, err := r.Lock(repo.Boost)
		if err != nil {
			return err
		}
		mds, err := lr.Datastore(ctx, "/metadata")
		if err != nil {
			return err
		}
		ds, err := backupds.Wrap(mds, "")
		if err != nil {
			return fmt.Errorf("opening backupds: %w", err)
		}

		// import manager - store the imports under the repo's `imports` subdirectory.
		dir := filepath.Join(lr.Path(), "imports")
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}

		ns := namespace.Wrap(ds, datastore.NewKey("/client"))
		importMgr := imports.NewManager(ns, dir)

		// create a temporary import to represent this job and obtain a staging CAR.
		id, err := importMgr.CreateImport()
		if err != nil {
			return fmt.Errorf("failed to create temporary import: %w", err)
		}
		defer importMgr.Remove(id) //nolint:errcheck

		tmp, err := importMgr.AllocateCAR(id)
		if err != nil {
			return fmt.Errorf("failed to allocate temporary CAR: %w", err)
		}
		defer os.Remove(tmp) //nolint:errcheck

		// generate and import the UnixFS DAG into a filestore (positional reference) CAR.
		root, err := unixfs.CreateFilestore(ctx, inPath, tmp)
		if err != nil {
			return fmt.Errorf("failed to import file using unixfs: %w", err)
		}

		// open the positional reference CAR as a filestore.
		fs, err := stores.ReadOnlyFilestore(tmp)
		if err != nil {
			return fmt.Errorf("failed to open filestore from carv2 in path %s: %w", tmp, err)
		}
		defer fs.Close() //nolint:errcheck

		f, err := os.Create(outPath)
		if err != nil {
			return err
		}

		// build a dense deterministic CAR (dense = containing filled leaves)
		if err := car.NewSelectiveCar(
			ctx,
			fs,
			[]car.Dag{{
				Root:     root,
				Selector: selectorparse.CommonSelector_ExploreAllRecursively,
			}},
			car.MaxTraversalLinks(config.MaxTraversalLinks),
		).Write(
			f,
		); err != nil {
			return fmt.Errorf("failed to write CAR to output file: %w", err)
		}

		err = f.Close()
		if err != nil {
			return err
		}

		encoder := cidenc.Encoder{Base: multibase.MustNewEncoder(multibase.Base32)}

		fmt.Println("Payload CID: ", encoder.Encode(root))

		return nil
	},
}
