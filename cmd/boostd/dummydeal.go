package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path"

	bcli "github.com/filecoin-project/boost/cli"
	"github.com/filecoin-project/boost/gql"
	"github.com/filecoin-project/boost/policy"
	"github.com/filecoin-project/boost/storagemarket"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/testutil"
	types2 "github.com/filecoin-project/boost/transport/types"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	"github.com/filecoin-project/lotus/api/v0api"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
)

var dummydealCmd = &cli.Command{
	Name:      "dummydeal",
	Hidden:    true,
	Usage:     "Trigger a sample deal",
	ArgsUsage: "<client addr> <miner addr>",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "url",
			Usage: "url to CAR file",
		},
		&cli.StringFlag{
			Name:  "piece-cid",
			Usage: "commp of the CAR file",
		},
		&cli.Uint64Flag{
			Name:  "piece-size",
			Usage: "size of the CAR file as a padded piece",
		},
		&cli.StringFlag{
			Name:  "payload-cid",
			Usage: "root CID of the CAR file",
		},
	},
	Before: before,
	Action: func(cctx *cli.Context) error {
		boostApi, ncloser, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return fmt.Errorf("getting boost api: %w", err)
		}
		defer ncloser()

		ctx := bcli.ReqContext(cctx)
		fullNodeApi, fncloser, err := bcli.GetFullNodeAPI(cctx)
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

		dealUuid := uuid.New()
		var pieceCid cid.Cid
		var pieceSize abi.PaddedPieceSize
		var rootCid cid.Cid
		var carFileSize uint64

		// If the URL is not specified, create a randomly generated file
		// locally, convert it to a CAR and serve the CAR with a web server
		url := cctx.String("url")
		if url == "" {
			// Create a CAR file
			randomFilepath, err := testutil.CreateRandomFile(os.TempDir(), rand.Int(), 2000000)
			if err != nil {
				return fmt.Errorf("creating random file: %w", err)
			}
			payloadCid, carFilepath, err := testutil.CreateDenseCARv2(os.TempDir(), randomFilepath)
			if err != nil {
				return fmt.Errorf("creating CAR: %w", err)
			}
			rootCid = payloadCid

			// Register the file to be served from the web server
			url, err = serveCarFile(dealUuid, carFilepath)
			if err != nil {
				return err
			}

			// Generate CommP
			cidAndSize, err := storagemarket.GenerateCommPLocally(carFilepath)
			if err != nil {
				return fmt.Errorf("generating commp for %s: %w", carFilepath, err)
			}
			pieceCid = cidAndSize.PieceCID
			pieceSize = cidAndSize.Size

			if pieceCid.Prefix() != market.PieceCIDPrefix {
				return fmt.Errorf("piece cid has wrong prefix: %s", pieceCid)
			}

			// Get the CAR file size
			carFileInfo, err := os.Stat(carFilepath)
			if err != nil {
				return fmt.Errorf("getting stat of %s: %w", carFilepath, err)
			}
			carFileSize = uint64(carFileInfo.Size())

			fmt.Printf("CAR file\n")
			fmt.Printf("  Path: %s\n", carFilepath)
			fmt.Printf("  Piece CID: %s\n", pieceCid)
			fmt.Printf("  Piece size: %d\n", pieceSize)
			fmt.Printf("  Payload CID: %s\n", rootCid)
			fmt.Printf("  File size: %d\n", carFileSize)
		} else {
			pieceCidStr := cctx.String("piece-cid")
			if pieceCidStr == "" {
				return fmt.Errorf("must provide piece-cid parameter for CAR url")
			}
			pieceCid, err = cid.Parse(pieceCidStr)
			if err != nil {
				return fmt.Errorf("parsing piece cid %s: %w", pieceCidStr, err)
			}

			pieceSizeParam := cctx.Uint64("piece-size")
			if pieceSizeParam == 0 {
				return fmt.Errorf("must provide piece-size parameter for CAR url")
			}
			pieceSize = abi.PaddedPieceSize(pieceSizeParam)

			payloadCidStr := cctx.String("payload-cid")
			if payloadCidStr == "" {
				return fmt.Errorf("must provide payload-cid parameter for CAR url")
			}
			rootCid, err = cid.Parse(payloadCidStr)
			if err != nil {
				return fmt.Errorf("parsing payload cid %s: %w", payloadCidStr, err)
			}

			carFileSize, err = getHTTPFileSize(ctx, url)
			if err != nil {
				return fmt.Errorf("getting size of file at %s", url)
			}
			if carFileSize == 0 {
				return fmt.Errorf("unable to get size of file at %s", url)
			}
		}

		// Store the path to the CAR file as a transfer parameter
		transferParams := &types2.HttpRequest{URL: url}
		paramsBytes, err := json.Marshal(transferParams)
		if err != nil {
			return fmt.Errorf("marshalling request parameters: %w", err)
		}

		tipset, err := fullNodeApi.ChainHead(ctx)
		if err != nil {
			return fmt.Errorf("getting chain head: %w", err)
		}

		head := tipset.Height()

		// Create a deal proposal
		dealProposal, err := dummydealProposal(ctx, fullNodeApi, rootCid, pieceSize, pieceCid, clientAddr, minerAddr, head)
		if err != nil {
			return fmt.Errorf("creating deal proposal: %w", err)
		}

		dealParams := types.DealParams{
			DealUUID:           dealUuid,
			ClientDealProposal: *dealProposal,
			DealDataRoot:       rootCid,
			Transfer: types.Transfer{
				Type:   "http",
				Params: paramsBytes,
				Size:   carFileSize,
			},
		}

		log.Debug("Make API call to start dummy deal " + dealUuid.String())

		rej, err := boostApi.BoostDummyDeal(ctx, dealParams)
		if err != nil {
			return fmt.Errorf("creating dummy deal: %w", err)
		}

		if rej != nil && rej.Reason != "" {
			fmt.Printf("Dummy deal %s rejected: %s\n", dealUuid, rej.Reason)
			return nil
		}
		fmt.Println("Made dummy deal " + dealUuid.String())

		return nil
	},
}

// Get the size of the CAR file by making an HTTP request and reading the
// Content-Length header
func getHTTPFileSize(ctx context.Context, url string) (uint64, error) {
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		return 0, err
	}

	req = req.WithContext(ctx)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}

	defer resp.Body.Close() // nolint
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return 0, fmt.Errorf("http req failed: code: %d, status: '%s'", resp.StatusCode, resp.Status)
	}

	return uint64(resp.ContentLength), nil
}

func serveCarFile(dealUuid uuid.UUID, fpath string) (string, error) {
	carName := dealUuid.String() + ".car"
	destPath := path.Join(gql.DummyDealsDir, carName)

	bytes, err := os.ReadFile(fpath)
	if err != nil {
		return "", fmt.Errorf("reading source car file: %w", err)
	}

	err = os.WriteFile(destPath, bytes, 0644)
	if err != nil {
		return "", fmt.Errorf("writing destination car file: %w", err)
	}

	log.Debugf("copied %d bytes from %s to %s", len(bytes), fpath, destPath)

	url := gql.DummyDealsBase + "/" + carName
	return url, nil
}

func dummydealProposal(ctx context.Context, fullNode v0api.FullNode, rootCid cid.Cid, pieceSize abi.PaddedPieceSize, pieceCid cid.Cid, clientAddr address.Address, minerAddr address.Address, head abi.ChainEpoch) (*market.ClientDealProposal, error) {
	startEpoch := head + abi.ChainEpoch(5760)
	endEpoch := startEpoch + 521280 // startEpoch + 181 days
	l, err := market.NewLabelFromString(rootCid.String())
	if err != nil {
		return nil, err
	}
	proposal := market.DealProposal{
		PieceCID:             pieceCid,
		PieceSize:            pieceSize,
		VerifiedDeal:         false,
		Client:               clientAddr,
		Provider:             minerAddr,
		Label:                l,
		StartEpoch:           startEpoch,
		EndEpoch:             endEpoch,
		StoragePricePerEpoch: abi.NewTokenAmount(1),
		ProviderCollateral:   policy.MaxProviderCollateral, // this value is chain dependent and is 0 on devnet, and currently a multiple of 147276332 on mainnet
		ClientCollateral:     abi.NewTokenAmount(0),
	}

	buf, err := cborutil.Dump(&proposal)
	if err != nil {
		return nil, err
	}

	log.Debugf("about to sign with clientAddr: %s", clientAddr)
	sig, err := fullNode.WalletSign(ctx, clientAddr, buf)
	if err != nil {
		return nil, fmt.Errorf("wallet sign failed: %w", err)
	}

	return &market.ClientDealProposal{
		Proposal:        proposal,
		ClientSignature: *sig,
	}, nil
}
