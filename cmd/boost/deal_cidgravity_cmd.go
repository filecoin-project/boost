package main

import (
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	"strings"

	bcli "github.com/filecoin-project/boost/cli"
	clinode "github.com/filecoin-project/boost/cli/node"
	"github.com/filecoin-project/boost/cmd"
	"github.com/filecoin-project/boost/storagemarket/types"
	types2 "github.com/filecoin-project/boost/transport/types"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	chain_types "github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
)

var dealCIDgravityFlags = []cli.Flag{
	&cli.StringFlag{
		Name:  "label",
		Usage: "label to be specified in the proposal (default: root CID)",
		Value: "",
	},
	&cli.StringFlag{
		Name:     "provider",
		Usage:    "storage provider on-chain address",
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
		Name:        "start-epoch",
		Usage:       "start epoch by when the deal should be proved by provider on-chain",
		DefaultText: "current chain head + 2 days",
	},
	&cli.IntFlag{
		Name:  "duration",
		Usage: "duration of the deal in epochs",
		Value: 518400, // default is 2880 * 180 == 180 days
	},
	&cli.IntFlag{
		Name:  "provider-collateral",
		Usage: "deal collateral that storage miner must put in escrow; if empty, the min collateral for the given piece size will be used",
	},
	&cli.Int64Flag{
		Name:  "storage-price",
		Usage: "storage price in attoFIL per epoch per GiB",
		Value: 1,
	},
	&cli.BoolFlag{
		Name:  "verified",
		Usage: "whether the deal funds should come from verified client data-cap",
		Value: true,
	},
}

var dealCidGravityCmd = &cli.Command{
	Name:   "cidgravity-deal",
	Usage:  "Make an Keep Alive deal with Boost using CIDgravity proposal format (offline deal)",
	Flags:  dealCIDgravityFlags,
	Before: before,
	Action: func(cctx *cli.Context) error {
		return dealCidGravityCmdAction(cctx, false)
	},
}

func dealCidGravityCmdAction(cctx *cli.Context, isOnline bool) error {
	ctx := bcli.ReqContext(cctx)

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

	log.Debugw("selected wallet", "wallet", walletAddr)

	maddr, err := address.NewFromString(cctx.String("provider"))
	if err != nil {
		return err
	}

	addrInfo, err := cmd.GetAddrInfo(ctx, api, maddr)
	if err != nil {
		return err
	}

	log.Debugw("found storage provider", "id", addrInfo.ID, "multiaddrs", addrInfo.Addrs, "addr", maddr)

	if err := n.Host.Connect(ctx, *addrInfo); err != nil {
		return fmt.Errorf("failed to connect to peer %s: %w", addrInfo.ID, err)
	}

	x, err := n.Host.Peerstore().FirstSupportedProtocol(addrInfo.ID, DealProtocolv120)
	if err != nil {
		return fmt.Errorf("getting protocols for peer %s: %w", addrInfo.ID, err)
	}

	if len(x) == 0 {
		return fmt.Errorf("boost client cannot make a deal with storage provider %s because it does not support protocol version 1.2.0", maddr)
	}

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

	transfer := types.Transfer{
		Size: carFileSize,
	}

	if isOnline {
		// Store the path to the CAR file as a transfer parameter
		transferParams := &types2.HttpRequest{URL: cctx.String("http-url")}

		if cctx.IsSet("http-headers") {
			transferParams.Headers = make(map[string]string)

			for _, header := range cctx.StringSlice("http-headers") {
				sp := strings.Split(header, "=")
				if len(sp) != 2 {
					return fmt.Errorf("malformed http header: %s", header)
				}

				transferParams.Headers[sp[0]] = sp[1]
			}
		}

		paramsBytes, err := json.Marshal(transferParams)
		if err != nil {
			return fmt.Errorf("marshalling request parameters: %w", err)
		}
		transfer.Type = "http"
		transfer.Params = paramsBytes
	}

	var providerCollateral abi.TokenAmount
	if cctx.IsSet("provider-collateral") {
		providerCollateral = abi.NewTokenAmount(cctx.Int64("provider-collateral"))
	} else {
		bounds, err := api.StateDealProviderCollateralBounds(ctx, abi.PaddedPieceSize(pieceSize), cctx.Bool("verified"), chain_types.EmptyTSK)
		if err != nil {
			return fmt.Errorf("node error getting collateral bounds: %w", err)
		}

		providerCollateral = big.Div(big.Mul(bounds.Min, big.NewInt(6)), big.NewInt(5)) // add 20%
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
	dealProposal, err := dealProposal(ctx, n, walletAddr, rootCid, abi.PaddedPieceSize(pieceSize), pieceCid, maddr, startEpoch, cctx.Int("duration"), cctx.Bool("verified"), providerCollateral, abi.NewTokenAmount(cctx.Int64("storage-price")))
	if err != nil {
		return fmt.Errorf("failed to create a deal proposal: %w", err)
	}

	// If param --label is empty, use root CID
	// If not, put the value in Label field (format use for CIDgravity keep-alive service)
	label := cctx.String("label")

	if label != "" {
		customLabel, err := market.NewLabelFromString(label)

		if err != nil {
			return fmt.Errorf("failed to parse the custom label as DealLabel: %w", err)
		}

		dealProposal.Proposal.Label = customLabel
	}

	// Generate the final proposal
	dealParams := types.DealParams{
		DealUUID:           dealUuid,
		ClientDealProposal: *dealProposal,
		DealDataRoot:       rootCid,
		IsOffline:          false,
		Transfer:           transfer,
		RemoveUnsealedCopy: true,
		SkipIPNIAnnounce:   false,
	}

	log.Debugw("about to submit deal proposal", "uuid", dealUuid.String())

	s, err := n.Host.NewStream(ctx, addrInfo.ID, DealProtocolv120)
	if err != nil {
		return fmt.Errorf("failed to open stream to peer %s: %w", addrInfo.ID, err)
	}
	defer s.Close()

	var resp types.DealResponse
	if err := doRpc(ctx, s, &dealParams, &resp); err != nil {
		return fmt.Errorf("send proposal rpc: %w", err)
	}

	if !resp.Accepted {
		return fmt.Errorf("deal proposal rejected: %s", resp.Message)
	}

	if cctx.Bool("json") {
		out := map[string]interface{}{
			"dealUuid":           dealUuid.String(),
			"provider":           maddr.String(),
			"clientWallet":       walletAddr.String(),
			"payloadCid":         rootCid.String(),
			"commp":              dealProposal.Proposal.PieceCID.String(),
			"startEpoch":         dealProposal.Proposal.StartEpoch.String(),
			"endEpoch":           dealProposal.Proposal.EndEpoch.String(),
			"providerCollateral": dealProposal.Proposal.ProviderCollateral.String(),
		}
		if isOnline {
			out["url"] = cctx.String("http-url")
		}
		return cmd.PrintJson(out)
	}

	msg := "sent deal proposal for CIDgravity offline deal"
	msg += "\n"
	msg += fmt.Sprintf("  deal uuid: %s\n", dealUuid)
	msg += fmt.Sprintf("  storage provider: %s\n", maddr)
	msg += fmt.Sprintf("  client wallet: %s\n", walletAddr)
	msg += fmt.Sprintf("  payload cid: %s\n", rootCid)
	if isOnline {
		msg += fmt.Sprintf("  url: %s\n", cctx.String("http-url"))
	}
	msg += fmt.Sprintf("  commp: %s\n", dealProposal.Proposal.PieceCID)
	msg += fmt.Sprintf("  start epoch: %d\n", dealProposal.Proposal.StartEpoch)
	msg += fmt.Sprintf("  end epoch: %d\n", dealProposal.Proposal.EndEpoch)
	msg += fmt.Sprintf("  provider collateral: %s\n", chain_types.FIL(dealProposal.Proposal.ProviderCollateral).Short())
	fmt.Println(msg)

	return nil
}
