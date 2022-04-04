package main

import (
	"fmt"

	bcli "github.com/filecoin-project/boost/cli"
	"github.com/filecoin-project/boost/cli/node"
	clinode "github.com/filecoin-project/boost/cli/node"
	"github.com/filecoin-project/boost/storagemarket/lp2pimpl"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/go-address"
	chain_types "github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/mitchellh/go-homedir"
	"github.com/multiformats/go-multiaddr"
	cli "github.com/urfave/cli/v2"
)

var dealStatusCmd = &cli.Command{
	Name:  "deal-status",
	Usage: "",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "repo",
			Usage: "repo directory for Boost client",
			Value: "~/.boost-client",
		},
		&cli.StringFlag{
			Name:     "provider",
			Usage:    "storage provider on-chain address",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "deal-uuid",
			Usage:    "",
			Required: true,
		},
	},
	Before: before,
	Action: func(cctx *cli.Context) error {
		ctx := bcli.ReqContext(cctx)

		dealUUID, err := uuid.Parse(cctx.String("deal-uuid"))
		if err != nil {
			return err
		}

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
			return fmt.Errorf("storage provider %s has no peer ID set on-chain", maddr)
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

		log.Debugw("found storage provider", "id", *minfo.PeerId, "multiaddr", maddrs)

		providerStr := cctx.String("provider")
		minerAddr, err := address.NewFromString(providerStr)
		if err != nil {
			return fmt.Errorf("invalid storage provider address '%s': %w", providerStr, err)
		}

		addrInfo := &peer.AddrInfo{
			ID:    *minfo.PeerId,
			Addrs: maddrs,
		}

		if err := n.Host.Connect(ctx, *addrInfo); err != nil {
			return fmt.Errorf("failed to connect to peer %s: %w", addrInfo.ID, err)
		}

		log.Debugw("storage provider on-chain address", "addr", minerAddr)

		dc := lp2pimpl.NewDealClient(n.Host, walletAddr, node.DealProposalSigner{LocalWallet: n.Wallet})
		resp, err := dc.SendDealStatusRequest(ctx, *minfo.PeerId, dealUUID)
		if err != nil {
			return fmt.Errorf("send deal status request failed: %w", err)
		}

		msg := "got deal status response"
		msg += "\n"

		if resp.Error != "" {
			msg += fmt.Sprintf("  error: %s\n", resp.Error)
			fmt.Println(msg)

			return nil
		}

		msg += fmt.Sprintf("  deal uuid: %s\n", resp.DealUUID)
		msg += fmt.Sprintf("  deal status: %s\n", statusMessage(resp))
		msg += fmt.Sprintf("  deal label: %s\n", resp.DealStatus.Proposal.Label)
		msg += fmt.Sprintf("  publish cid: %s\n", resp.DealStatus.PublishCid)
		msg += fmt.Sprintf("  chain deal id: %d\n", resp.DealStatus.ChainDealID)
		fmt.Println(msg)

		return nil
	},
}

// statusMessage is based on dealResolver.Message
func statusMessage(resp *types.DealStatusResponse) string {
	switch resp.DealStatus.Status {
	case dealcheckpoints.Accepted.String():
		//TODO: handle offline deals

		//if dr.IsOffline {
		//return "Awaiting Offline Data Import"
		//}
		switch resp.NBytesReceived {
		case 0:
			return "Transfer Queued"
		case 100:
			return "Transfer Complete"
		default:
			pct := (100 * float64(resp.NBytesReceived)) / float64(resp.TransferSize)
			return fmt.Sprintf("Transferring %.2f%%", pct)
		}
	case dealcheckpoints.Transferred.String():
		return "Ready to Publish"
	case dealcheckpoints.Published.String():
		return "Awaiting Publish Confirmation"
	case dealcheckpoints.PublishConfirmed.String():
		return "Adding to Sector"
	case dealcheckpoints.AddedPiece.String():
		return "Announcing"
	case dealcheckpoints.IndexedAndAnnounced.String():
		return "Sealing"
	case dealcheckpoints.Complete.String():
		return "Complete"
	}
	return resp.DealStatus.Status
}
