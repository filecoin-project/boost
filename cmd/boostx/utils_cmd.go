package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"

	clinode "github.com/filecoin-project/boost/cli/node"
	"github.com/filecoin-project/boost/lib/messagesigner"
	"github.com/filecoin-project/boost/node"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/go-commp-utils/writer"
	"github.com/filecoin-project/go-fil-markets/stores"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors"
	marketactor "github.com/filecoin-project/lotus/chain/actors/builtin/market"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/lib/backupds"

	"github.com/filecoin-project/lotus/lib/unixfs"
	"github.com/filecoin-project/lotus/node/modules"
	"github.com/filecoin-project/lotus/node/repo/imports"
	"github.com/ipfs/go-cidutil/cidenc"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	ds_sync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-car"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/mitchellh/go-homedir"
	"github.com/multiformats/go-multibase"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	lotus_repo "github.com/filecoin-project/lotus/node/repo"
)

var marketCmd = &cli.Command{
	Name:        "market-add",
	Usage:       "Add funds to the Storage Market actor",
	Description: "Send signed message to add funds for the default wallet to the Storage Market actor. Uses 2x current BaseFee and a maximum fee of 1 nFIL. This is an experimental utility, do not use in production.",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "repo",
			Usage: "repo directory for Boost client",
			Value: "~/.boost-client",
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
			return xerrors.Errorf("parsing 'amount' argument: %w", err)
		}

		amt := abi.TokenAmount(f)

		ctx := lcli.ReqContext(cctx)

		sdir, err := homedir.Expand(cctx.String("repo"))
		if err != nil {
			return err
		}

		n, err := clinode.Setup(ctx, sdir)
		if err != nil {
			return err
		}

		api, closer, err := lcli.GetGatewayAPI(cctx)
		if err != nil {
			return fmt.Errorf("cant setup gateway connection: %w", err)
		}
		defer closer()

		walletAddr, err := n.Wallet.GetDefault()
		if err != nil {
			return err
		}

		log.Infow("selected wallet", "wallet", walletAddr)

		ds := ds_sync.MutexWrap(datastore.NewMapDatastore())
		messagesigner := messagesigner.NewMessageSigner(n.Wallet, &modules.MpoolNonceAPI{ChainModule: api, StateModule: api}, ds)

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

		head, err := api.ChainHead(ctx)
		if err != nil {
			return err
		}
		basefee := head.Blocks()[0].ParentBaseFee

		spec := &lapi.MessageSendSpec{
			MaxFee: abi.NewTokenAmount(1000000000), // 1 nFIL
		}
		msg, err = api.GasEstimateMessageGas(ctx, msg, spec, types.EmptyTSK)
		if err != nil {
			return xerrors.Errorf("GasEstimateMessageGas error: %w", err)
		}

		newGasFeeCap := big.Mul(big.Int(basefee), big.NewInt(2)) // use 2*basefee, so that this message confirms quickly

		if big.Cmp(msg.GasFeeCap, newGasFeeCap) < 0 {
			msg.GasFeeCap = newGasFeeCap
		}

		smsg, err := messagesigner.SignMessage(ctx, msg, func(*types.SignedMessage) error { return nil })
		if err != nil {
			return err
		}

		cid, err := api.MpoolPush(ctx, smsg)
		if err != nil {
			return xerrors.Errorf("mpool push: failed to push message: %w", err)
		}
		log.Infow("submitted market-add message", "cid", cid.String())

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

		// check that the data is a car file; if it's not, retrieval won't work
		_, err = car.ReadHeader(bufio.NewReader(rdr))
		if err != nil {
			return xerrors.Errorf("not a car file: %w", err)
		}

		if _, err := rdr.Seek(0, io.SeekStart); err != nil {
			return xerrors.Errorf("seek to start: %w", err)
		}

		w := &writer.Writer{}
		_, err = io.CopyBuffer(w, rdr, make([]byte, writer.CommPBuf))
		if err != nil {
			return xerrors.Errorf("copy into commp writer: %w", err)
		}

		commp, err := w.Sum()
		if err != nil {
			return xerrors.Errorf("computing commP failed: %w", err)
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
		lr, err := r.Lock(node.Boost)
		if err != nil {
			return err
		}
		mds, err := lr.Datastore(ctx, "/metadata")
		if err != nil {
			return err
		}
		ds, err := backupds.Wrap(mds, "")
		if err != nil {
			return xerrors.Errorf("opening backupds: %w", err)
		}

		// import manager - store the imports under the repo's `imports` subdirectory.
		dir := filepath.Join(lr.Path(), "imports")
		if err := os.MkdirAll(dir, 0755); err != nil {
			return xerrors.Errorf("failed to create directory %s: %w", dir, err)
		}

		ns := namespace.Wrap(ds, datastore.NewKey("/client"))
		importMgr := imports.NewManager(ns, dir)

		// create a temporary import to represent this job and obtain a staging CAR.
		id, err := importMgr.CreateImport()
		if err != nil {
			return xerrors.Errorf("failed to create temporary import: %w", err)
		}
		defer importMgr.Remove(id) //nolint:errcheck

		tmp, err := importMgr.AllocateCAR(id)
		if err != nil {
			return xerrors.Errorf("failed to allocate temporary CAR: %w", err)
		}
		defer os.Remove(tmp) //nolint:errcheck

		// generate and import the UnixFS DAG into a filestore (positional reference) CAR.
		root, err := unixfs.CreateFilestore(ctx, inPath, tmp)
		if err != nil {
			return xerrors.Errorf("failed to import file using unixfs: %w", err)
		}

		// open the positional reference CAR as a filestore.
		fs, err := stores.ReadOnlyFilestore(tmp)
		if err != nil {
			return xerrors.Errorf("failed to open filestore from carv2 in path %s: %w", tmp, err)
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
			return xerrors.Errorf("failed to write CAR to output file: %w", err)
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
