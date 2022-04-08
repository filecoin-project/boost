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
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/google/uuid"
	"github.com/mitchellh/go-homedir"
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
		&cli.StringFlag{
			Name:  "wallet",
			Usage: "the wallet address that was used to sign the deal proposal",
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

		walletAddr, err := n.GetProvidedOrDefaultWallet(ctx, cctx.String("wallet"))
		if err != nil {
			return err
		}

		log.Debugw("selected wallet", "wallet", walletAddr)

		maddr, err := address.NewFromString(cctx.String("provider"))
		if err != nil {
			return err
		}

		addrInfo, err := getAddrInfo(cctx, ctx, api, maddr)
		if err != nil {
			return err
		}

		log.Debugw("found storage provider", "id", addrInfo.ID, "multiaddrs", addrInfo.Addrs, "addr", maddr)

		if err := n.Host.Connect(ctx, *addrInfo); err != nil {
			return fmt.Errorf("failed to connect to peer %s: %w", addrInfo.ID, err)
		}

		dc := lp2pimpl.NewDealClient(n.Host, walletAddr, node.DealProposalSigner{LocalWallet: n.Wallet})
		resp, err := dc.SendDealStatusRequest(ctx, addrInfo.ID, dealUUID)
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
		if resp.IsOffline {
			return "Awaiting Offline Data Import"
		}
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
		if resp.DealStatus.Error != "" {
			return "Error: " + resp.DealStatus.Error
		}
		return "Complete"
	}
	return resp.DealStatus.Status
}
