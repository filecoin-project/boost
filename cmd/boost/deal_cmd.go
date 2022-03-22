package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/mitchellh/go-homedir"

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

var dealCmd = &cli.Command{
	Name:  "deal",
	Usage: "Make an online deal with Boost",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "repo",
			Usage:    "repo directory for Boost client",
			Required: true,
			Value:    "~/.boost-client",
		},
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
			Name:     "commp",
			Usage:    "commp of the CAR file",
			Required: true,
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
			Name:     "payload-cid",
			Usage:    "root CID of the CAR file",
			Required: true,
		},
		&cli.IntFlag{
			Name:  "start-epoch",
			Usage: "",
		},
		&cli.IntFlag{
			Name:  "duration",
			Usage: "epochs for this deal to be active for",
			Value: 518400, // default is 2880 * 180 == 180 days
		},
		&cli.IntFlag{
			Name:  "provider-collateral",
			Usage: "",
			Value: 0,
		},
		&cli.Int64Flag{
			Name:  "storage-price-per-epoch",
			Usage: "",
			Value: 1,
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

		sdir, err := homedir.Expand(cctx.String("repo"))
		if err != nil {
			return err
		}

		node, err := setup(ctx, sdir)
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

		log.Debugw("selected wallet", "wallet", walletAddr)

		maddr, err := address.NewFromString(cctx.String("provider"))
		if err != nil {
			return err
		}
		minfo, err := api.StateMinerInfo(ctx, maddr, chain_types.EmptyTSK)
		if err != nil {
			return err
		}
		if minfo.PeerId == nil {
			return fmt.Errorf("could not find peer ID for stoarge provider %s", maddr)
		}

		var maddrs []multiaddr.Multiaddr
		for _, mma := range minfo.Multiaddrs {
			ma, err := multiaddr.NewMultiaddrBytes(mma)
			if err != nil {
				return fmt.Errorf("storage provider %s had invalid multiaddrs in their info: %w", maddr, err)
			}
			maddrs = append(maddrs, ma)
		}
		if len(maddrs) == 0 {
			return fmt.Errorf("storage provider %s has no multiaddrs set on-chain", maddr)
		}

		addrInfo := &peer.AddrInfo{
			ID:    *minfo.PeerId,
			Addrs: maddrs,
		}

		log.Debugw("found storage provider", "id", *minfo.PeerId, "multiaddr", maddrs)

		providerStr := cctx.String("provider")
		minerAddr, err := address.NewFromString(providerStr)
		if err != nil {
			return fmt.Errorf("invalid storage provider address '%s': %w", providerStr, err)
		}

		log.Debugw("storage provider on-chain address", "addr", minerAddr)

		dealUuid := uuid.New()

		commp := cctx.String("commp")
		pieceCid, err := cid.Parse(commp)
		if err != nil {
			return fmt.Errorf("parsing commp '%s': %w", commp, err)
		}

		pieceSize := cctx.Uint64("piece-size")
		if pieceSize == 0 {
			return fmt.Errorf("must provide piece-size parameter for CAR url")
		}

		payloadCidStr := cctx.String("payload-cid")
		rootCid, err := cid.Parse(payloadCidStr)
		if err != nil {
			return fmt.Errorf("parsing payload cid %s: %w", payloadCidStr, err)
		}

		carFileSize := cctx.Uint64("car-size")
		if carFileSize == 0 {
			return fmt.Errorf("size of car file cannot be 0")
		}

		if err := node.Host.Connect(ctx, *addrInfo); err != nil {
			return fmt.Errorf("failed to connect to peer %s: %w", addrInfo.ID, err)
		}

		s, err := node.Host.NewStream(ctx, addrInfo.ID, DealProtocolv120)
		if err != nil {
			return fmt.Errorf("failed to open stream to peer %s: %w", addrInfo.ID, err)
		}
		defer s.Close()

		// Store the path to the CAR file as a transfer parameter
		transferParams := &types2.HttpRequest{URL: cctx.String("url")}
		paramsBytes, err := json.Marshal(transferParams)
		if err != nil {
			return fmt.Errorf("marshalling request parameters: %w", err)
		}

		var providerCollateral abi.TokenAmount
		if cctx.IsSet("provider-collateral") {
			providerCollateral = abi.NewTokenAmount(cctx.Int64("provider-collateral"))
		} else {
			bounds, err := api.StateDealProviderCollateralBounds(ctx, abi.PaddedPieceSize(pieceSize), cctx.Bool("verified"), chain_types.EmptyTSK)
			if err != nil {
				return fmt.Errorf("node error getting collateral bounds: %w", err)
			}

			providerCollateral = bounds.Min
		}

		var startEpoch abi.ChainEpoch
		if cctx.IsSet("start-epoch") {
			startEpoch = abi.ChainEpoch(cctx.Int("start-epoch"))
		} else {
			tipset, err := api.ChainHead(ctx)
			if err != nil {
				return fmt.Errorf("getting chain head: %w", err)
			}

			head := tipset.Height()

			log.Debugw("current block height", "number", head)

			startEpoch = head + abi.ChainEpoch(5760) // head + 2 days
		}

		// Create a deal proposal to storage provider using deal protocol v1.2.0 format
		dealProposal, err := dealProposal(ctx, node, rootCid, abi.PaddedPieceSize(pieceSize), pieceCid, minerAddr, startEpoch, cctx.Int("duration"), cctx.Bool("verified"), providerCollateral, abi.NewTokenAmount(cctx.Int64("storage-price-per-epoch")))
		if err != nil {
			return fmt.Errorf("failed to create a deal proposal: %w", err)
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

		log.Debugw("about to submit deal proposal", "uuid", dealUuid.String())

		var resp types.DealResponse
		if err := doRpc(ctx, s, &dealParams, &resp); err != nil {
			return fmt.Errorf("send proposal rpc: %w", err)
		}

		if !resp.Accepted {
			return fmt.Errorf("deal proposal rejected: %s", resp.Message)
		}

		msg := fmt.Sprintf("sent deal proposal %s to storage provider %s:\n", dealUuid, maddr)
		msg += fmt.Sprintf("  client wallet: %s\n", walletAddr)
		msg += fmt.Sprintf("  payload cid: %s\n", rootCid)
		msg += fmt.Sprintf("  url: %s\n", transferParams.URL)
		msg += fmt.Sprintf("  commp: %s\n", dealProposal.Proposal.PieceCID)
		msg += fmt.Sprintf("  start epoch: %d\n", dealProposal.Proposal.StartEpoch)
		msg += fmt.Sprintf("  end epoch: %d\n", dealProposal.Proposal.EndEpoch)
		msg += fmt.Sprintf("  client collateral: %s\n", chain_types.FIL(dealProposal.Proposal.ClientCollateral).Short())
		msg += fmt.Sprintf("  provider collateral: %s\n", chain_types.FIL(dealProposal.Proposal.ProviderCollateral).Short())
		fmt.Println(msg)

		return nil
	},
}

func dealProposal(ctx context.Context, node *Node, rootCid cid.Cid, pieceSize abi.PaddedPieceSize, pieceCid cid.Cid, minerAddr address.Address, startEpoch abi.ChainEpoch, duration int, verified bool, providerCollateral abi.TokenAmount, storagePricePerEpoch abi.TokenAmount) (*market.ClientDealProposal, error) {
	clientAddr, err := node.Wallet.GetDefault()
	if err != nil {
		return nil, err
	}

	endEpoch := startEpoch + abi.ChainEpoch(duration)
	proposal := market.DealProposal{
		PieceCID:             pieceCid,
		PieceSize:            pieceSize,
		VerifiedDeal:         verified,
		Client:               clientAddr,
		Provider:             minerAddr,
		Label:                rootCid.String(),
		StartEpoch:           startEpoch,
		EndEpoch:             endEpoch,
		StoragePricePerEpoch: storagePricePerEpoch,
		ProviderCollateral:   providerCollateral,
	}

	buf, err := cborutil.Dump(&proposal)
	if err != nil {
		return nil, err
	}

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
		s.SetDeadline(dline)             //nolint:errcheck
		defer s.SetDeadline(time.Time{}) //nolint:errcheck
	}

	if err := cborutil.WriteCborRPC(s, req); err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	if err := cborutil.ReadCborRPC(s, resp); err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	return nil
}
