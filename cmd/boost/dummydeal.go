package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"

	lcli "github.com/filecoin-project/boost/cli"
	"github.com/filecoin-project/boost/gql"
	"github.com/filecoin-project/boost/storagemarket"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/testutil"
	types2 "github.com/filecoin-project/boost/transport/types"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api/v0api"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var dummydealCmd = &cli.Command{
	Name:      "dummydeal",
	Usage:     "Trigger a sample deal",
	ArgsUsage: "<client addr> <miner addr>",
	Before:    before,
	Action: func(cctx *cli.Context) error {
		boostApi, ncloser, err := lcli.GetBoostAPI(cctx)
		if err != nil {
			return fmt.Errorf("getting boost api: %w", err)
		}
		defer ncloser()

		ctx := lcli.DaemonContext(cctx)
		fullNodeApi, fncloser, err := lcli.GetFullNodeAPI(cctx)
		if err != nil {
			return fmt.Errorf("getting full node api: %w", err)
		}
		defer fncloser()

		if cctx.NArg() != 2 {
			return fmt.Errorf("must specify client and miner address as arguments")
		}

		clientAddr, err := address.NewFromString(cctx.Args().First())
		if err != nil {
			return fmt.Errorf("invalid client address '%s': %w", cctx.Args().First(), err)
		}

		minerAddr, err := address.NewFromString(cctx.Args().Get(1))
		if err != nil {
			return fmt.Errorf("invalid miner address '%s': %w", cctx.Args().Get(1), err)
		}

		// Create a CAR file
		randomFilepath, err := testutil.CreateRandomFile(rand.Int(), 2000000)
		if err != nil {
			return fmt.Errorf("creating random file: %w", err)
		}
		rootCid, carFilepath, err := testutil.CreateDenseCARv2(randomFilepath)
		if err != nil {
			return fmt.Errorf("creating CAR: %w", err)
		}

		// Register the file to be served from the web server
		dealUuid := uuid.New()
		url, err := serveCarFile(dealUuid, carFilepath)
		if err != nil {
			return err
		}

		// Store the path to the CAR file as a transfer parameter
		transferParams := &types2.HttpRequest{URL: url}
		paramsBytes, err := json.Marshal(transferParams)
		if err != nil {
			return fmt.Errorf("marshalling request parameters: %w", err)
		}

		peerID, err := boostApi.ID(ctx)
		if err != nil {
			return fmt.Errorf("getting boost peer ID: %w", err)
		}

		// Create a deal proposal
		dealProposal, err := dealProposal(ctx, fullNodeApi, carFilepath, rootCid, clientAddr, minerAddr)
		if err != nil {
			return fmt.Errorf("creating deal proposal: %w", err)
		}

		carFileInfo, err := os.Stat(carFilepath)
		if err != nil {
			return fmt.Errorf("getting stat of %s: %w", carFilepath, err)
		}

		dealParams := &types.ClientDealParams{
			DealUuid:           dealUuid,
			MinerPeerID:        peerID,
			ClientPeerID:       peerID,
			ClientDealProposal: *dealProposal,
			DealDataRoot:       rootCid,
			Transfer: types.Transfer{
				Type:   "http",
				Params: paramsBytes,
				Size:   uint64(carFileInfo.Size()),
			},
		}

		log.Debug("Make API call to start dummy deal " + dealUuid.String())
		rej, err := boostApi.MarketDummyDeal(ctx, dealParams)
		if err != nil {
			return xerrors.Errorf("creating dummy deal: %w", err)
		}

		if rej != nil && rej.Reason != "" {
			fmt.Printf("Dummy deal %s rejected: %s\n", dealUuid, rej.Reason)
			return nil
		}
		fmt.Println("Made dummy deal " + dealUuid.String())

		return nil
	},
}

func serveCarFile(dealUuid uuid.UUID, fpath string) (string, error) {
	carName := dealUuid.String() + ".car"
	destPath := path.Join(gql.DummyDealsDir, carName)

	bytes, err := ioutil.ReadFile(fpath)
	if err != nil {
		return "", fmt.Errorf("reading source car file: %w", err)
	}

	err = ioutil.WriteFile(destPath, bytes, 0644)
	if err != nil {
		return "", fmt.Errorf("writing destination car file: %w", err)
	}

	log.Debugf("copied %d bytes from %s to %s", len(bytes), fpath, destPath)

	url := gql.DummyDealsBase + "/" + carName
	return url, nil
}

func dealProposal(ctx context.Context, fullNode v0api.FullNode, carFilePath string, rootCid cid.Cid, clientAddr address.Address, minerAddr address.Address) (*market.ClientDealProposal, error) {
	cidAndSize, err := storagemarket.GenerateCommP(carFilePath)
	if err != nil {
		return nil, err
	}

	proposal := market.DealProposal{
		PieceCID:             cidAndSize.PieceCID,
		PieceSize:            cidAndSize.PieceSize,
		VerifiedDeal:         false,
		Client:               clientAddr,
		Provider:             minerAddr,
		Label:                rootCid.String(),
		StartEpoch:           abi.ChainEpoch(rand.Intn(100000)),
		EndEpoch:             800000 + abi.ChainEpoch(rand.Intn(10000)),
		StoragePricePerEpoch: abi.NewTokenAmount(1),
		ProviderCollateral:   abi.NewTokenAmount(0),
		ClientCollateral:     abi.NewTokenAmount(0),
	}

	buf, err := cborutil.Dump(&proposal)
	if err != nil {
		return nil, err
	}

	sig, err := fullNode.WalletSign(ctx, clientAddr, buf)
	if err != nil {
		return nil, err
	}

	return &market.ClientDealProposal{
		Proposal:        proposal,
		ClientSignature: *sig,
	}, nil
}
