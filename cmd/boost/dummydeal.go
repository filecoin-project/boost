package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"sync"
	"time"

	lcli "github.com/filecoin-project/boost/cli"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/testutil"
	types2 "github.com/filecoin-project/boost/transport/types"
	"github.com/filecoin-project/boost/util"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/google/uuid"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var websvr = newWebServer()

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

		if cctx.NArg() != 2 {
			return fmt.Errorf("must specify client and miner address as arguments")
		}

		clientAddr, err := address.NewFromString(cctx.Args().First())
		if err != nil {
			return fmt.Errorf("invalid client address '%s': %w", err)
		}

		minerAddr, err := address.NewFromString(cctx.Args().Get(1))
		if err != nil {
			return fmt.Errorf("invalid miner address '%s': %w", err)
		}

		// Create a CAR file
		carRes, err := testutil.CreateRandomCARv1(5, 1600)
		if err != nil {
			return fmt.Errorf("creating CAR: %w", err)
		}

		// Register the file to be served from a web server
		url := websvr.serve(path.Base(carRes.CarFile), carRes.CarFile)

		// TODO: get fullNode API
		var fullNode v1api.FullNode
		dealProposal, err := dealProposal(ctx, fullNode, carRes, url, clientAddr, minerAddr)
		if err != nil {
			return fmt.Errorf("creating deal proposal: %w", err)
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

		dealUuid := uuid.New()
		dealParams := &types.ClientDealParams{
			DealUuid:           dealUuid,
			MinerPeerID:        peerID,
			ClientPeerID:       peerID,
			ClientDealProposal: *dealProposal,
			DealDataRoot:       carRes.Root,
			Transfer: types.Transfer{
				Type:   "http",
				Params: paramsBytes,
				Size:   carRes.CarSize,
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

func dealProposal(ctx context.Context, fullNode v1api.FullNode, carRes *testutil.CarRes, url string, clientAddr address.Address, minerAddr address.Address) (*market.ClientDealProposal, error) {
	pieceCid, pieceSize, err := util.CommP(ctx, carRes.Blockstore, carRes.Root)
	if err != nil {
		return nil, err
	}

	proposal := market.DealProposal{
		PieceCID:             pieceCid,
		PieceSize:            pieceSize.Padded(),
		VerifiedDeal:         false,
		Client:               clientAddr,
		Provider:             minerAddr,
		Label:                carRes.Root.String(),
		StartEpoch:           abi.ChainEpoch(rand.Intn(100000)),
		EndEpoch:             800000 + abi.ChainEpoch(rand.Intn(10000)),
		StoragePricePerEpoch: abi.NewTokenAmount(1),
		ProviderCollateral:   abi.NewTokenAmount(0),
		ClientCollateral:     abi.NewTokenAmount(0),
	}

	buf, err := cborutil.Dump(proposal)
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

type webServer struct {
	lk     sync.Mutex
	server *httptest.Server
	files  map[string]string
}

func newWebServer() *webServer {
	return &webServer{}
}

func (ws *webServer) serve(name string, path string) string {
	ws.lk.Lock()
	defer ws.lk.Unlock()

	ws.files[name] = path

	if ws.server != nil {
		return ws.server.URL + "/" + name
	}

	ws.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Path

		ws.lk.Lock()
		filePath, ok := ws.files[name]
		ws.lk.Unlock()
		if !ok {
			log.Errorf("file '%s' not found", name)
			w.WriteHeader(404)
			return
		}

		file, err := os.Open(filePath)
		if err != nil {
			log.Errorf("serving file '%s': %s", name, err)
			w.WriteHeader(500)
			return
		}

		http.ServeContent(w, r, "", time.Now(), file)
	}))

	return ws.server.URL + "/" + name
}
