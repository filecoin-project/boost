package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/filecoin-project/boost/storagemarket/types"

	types2 "github.com/filecoin-project/boost/transport/types"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	chain_types "github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	cli "github.com/urfave/cli/v2"

	inet "github.com/libp2p/go-libp2p-core/network"
)

const DealProtocolv120 = "/fil/storage/mk/1.2.0"

var proposeDealCmd = &cli.Command{
	Name:  "propose-deal",
	Usage: "Trigger a deal",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "url",
			Usage:    "url to CAR file",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "provider",
			Usage:    "provider address",
			Required: true,
		},
		&cli.StringFlag{
			Name:  "commp",
			Usage: "commp of the CAR file",
		},
		&cli.Uint64Flag{
			Name:     "piece-size",
			Usage:    "size of the CAR file as a padded piece",
			Required: true,
		},
		&cli.Uint64Flag{
			Name:     "car-size",
			Usage:    "size of the CAR file",
			Required: true,
		},
		&cli.StringFlag{
			Name:  "payload-cid",
			Usage: "root CID of the CAR file",
		},
		&cli.IntFlag{
			Name:  "days",
			Usage: "how many days should this deal be active for",
			Value: 181,
		},
		&cli.BoolFlag{
			Name:  "verified",
			Usage: "whether deal should be verified or not",
			Value: true,
		},
	},
	Before: before,
	Action: func(cctx *cli.Context) error {
		ctx := lcli.DaemonContext(cctx)

		node, err := setup(ctx, "/Users/nonsense/boost-libp2p-node")
		if err != nil {
			return err
		}

		api, closer, err := lcli.GetGatewayAPI(cctx)
		if err != nil {
			return fmt.Errorf("cant setup gateway connection: %w", err)
		}
		defer closer()

		walletAddr, err := node.Wallet.GetDefault()
		if err != nil {
			return err
		}

		log.Infow("selected wallet", "wallet", walletAddr)

		maddr, err := address.NewFromString(cctx.String("provider"))
		if err != nil {
			return err
		}
		minfo, err := api.StateMinerInfo(ctx, maddr, chain_types.EmptyTSK)
		if err != nil {
			return err
		}

		if minfo.PeerId == nil {
			return fmt.Errorf("miner %s has no peer ID set", maddr)
		}

		var maddrs []multiaddr.Multiaddr
		for _, mma := range minfo.Multiaddrs {
			ma, err := multiaddr.NewMultiaddrBytes(mma)
			if err != nil {
				return fmt.Errorf("miner %s had invalid multiaddrs in their info: %w", maddr, err)
			}
			maddrs = append(maddrs, ma)
		}

		addrInfo := &peer.AddrInfo{
			ID:    *minfo.PeerId,
			Addrs: maddrs,
		}

		log.Infow("found miner", "id", *minfo.PeerId, "multiaddr", maddrs)

		providerStr := cctx.String("provider")
		minerAddr, err := address.NewFromString(providerStr)
		if err != nil {
			return fmt.Errorf("invalid miner address '%s': %w", providerStr, err)
		}

		log.Infow("miner on-chain address", "addr", minerAddr)

		dealUuid := uuid.New()

		commp := cctx.String("commp")
		if commp == "" {
			return fmt.Errorf("must provide commp parameter for CAR url")
		}

		pieceCid, err := cid.Parse(commp)
		if err != nil {
			return fmt.Errorf("parsing commp '%s': %w", commp, err)
		}

		pieceSize := cctx.Uint64("piece-size")
		if pieceSize == 0 {
			return fmt.Errorf("must provide piece-size parameter for CAR url")
		}

		payloadCidStr := cctx.String("payload-cid")
		if payloadCidStr == "" {
			return fmt.Errorf("must provide payload-cid parameter for CAR url")
		}

		rootCid, err := cid.Parse(payloadCidStr)
		if err != nil {
			return fmt.Errorf("parsing payload cid %s: %w", payloadCidStr, err)
		}

		carFileSize := cctx.Uint64("car-size")
		if carFileSize == 0 {
			return fmt.Errorf("size of car file cannot be 0")
		}

		tipset, err := api.ChainHead(ctx)
		if err != nil {
			return fmt.Errorf("getting chain head: %w", err)
		}

		head := tipset.Height()

		log.Infow("current block height", "number", head)

		if err := node.Host.Connect(ctx, *addrInfo); err != nil {
			return err
		}

		s, err := node.Host.NewStream(ctx, addrInfo.ID, DealProtocolv120)
		if err != nil {
			return fmt.Errorf("failed to open stream to peer: %w", err)
		}

		defer s.Close()

		// Store the path to the CAR file as a transfer parameter
		transferParams := &types2.HttpRequest{URL: cctx.String("url")}
		paramsBytes, err := json.Marshal(transferParams)
		if err != nil {
			return fmt.Errorf("marshalling request parameters: %w", err)
		}

		// Create a deal proposal to storage provider using deal protocol v1.2.0 format
		dealProposal, err := dealProposal(ctx, node, rootCid, abi.PaddedPieceSize(pieceSize), pieceCid, minerAddr, head, cctx.Int("days"), cctx.Bool("verified"))
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

		log.Debugw("about to submit deal", "uuid", dealUuid.String())

		var resp types.DealResponse
		if err := doRpc(ctx, s, &dealParams, &resp); err != nil {
			return fmt.Errorf("send proposal rpc: %w", err)
		}

		// Check if the deal proposal was accepted
		if !resp.Accepted {
			return fmt.Errorf("deal proposal rejected: %s", resp.Message)
		}

		log.Infow("submitted deal", "uuid", dealUuid.String())

		return nil
	},
}

func dealProposal(ctx context.Context, node *Node, rootCid cid.Cid, pieceSize abi.PaddedPieceSize, pieceCid cid.Cid, minerAddr address.Address, head abi.ChainEpoch, days int, verified bool) (*market.ClientDealProposal, error) {
	clientAddr, err := node.Wallet.GetDefault()
	if err != nil {
		return nil, err
	}

	startEpoch := head + abi.ChainEpoch(5760)
	endEpoch := startEpoch + abi.ChainEpoch(2880*days) // startEpoch + N days
	proposal := market.DealProposal{
		PieceCID:             pieceCid,
		PieceSize:            pieceSize,
		VerifiedDeal:         verified,
		Client:               clientAddr,
		Provider:             minerAddr,
		Label:                rootCid.String(),
		StartEpoch:           startEpoch,
		EndEpoch:             endEpoch,
		StoragePricePerEpoch: abi.NewTokenAmount(1),
		ProviderCollateral:   abi.NewTokenAmount(23775644862274), // TODO: fixme
		//ClientCollateral:   abi.NewTokenAmount(0),
	}

	buf, err := cborutil.Dump(&proposal)
	if err != nil {
		return nil, err
	}

	log.Debugf("about to sign with clientAddr: %s", clientAddr)

	sig, err := node.Wallet.WalletSign(ctx, clientAddr, buf, api.MsgMeta{Type: api.MTDealProposal})
	if err != nil {
		return nil, fmt.Errorf("wallet sign failed: %w", err)
	}

	return &market.ClientDealProposal{
		Proposal:        proposal,
		ClientSignature: *sig,
	}, nil
}

func doRpc(ctx context.Context, s inet.Stream, req interface{}, resp interface{}) error {
	dline, ok := ctx.Deadline()
	if ok {
		s.SetDeadline(dline)
		defer s.SetDeadline(time.Time{})
	}

	if err := cborutil.WriteCborRPC(s, req); err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	if err := cborutil.ReadCborRPC(s, resp); err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	return nil
}
