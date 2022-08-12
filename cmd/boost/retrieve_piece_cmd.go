package main

import (
	"fmt"

	bcli "github.com/filecoin-project/boost/cli"
	clinode "github.com/filecoin-project/boost/cli/node"
	"github.com/filecoin-project/boost/cmd"
	"github.com/filecoin-project/boost/retrievalmarket/lp2pimpl"
	"github.com/filecoin-project/boost/retrievalmarket/types"
	"github.com/filecoin-project/go-address"
	lapi "github.com/filecoin-project/lotus/api"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/urfave/cli/v2"
)

var retrievePieceCmd = &cli.Command{
	Name:  "retrieve-piece",
	Usage: "",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "provider",
			Usage:    "storage provider on-chain address",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "piece-cid",
			Usage:    "",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "wallet",
			Usage:    "wallet to identify the client of a potential deal",
			Required: false,
		},
	},
	Before: before,
	Action: func(cctx *cli.Context) error {
		ctx := bcli.ReqContext(cctx)

		pieceCID, err := cid.Parse(cctx.String("piece-cid"))
		if err != nil {
			return err
		}

		n, err := clinode.Setup(cctx.String(cmd.FlagRepo.Name))
		if err != nil {
			return err
		}

		api, closer, err := lcli.GetGatewayAPI(cctx)
		if err != nil {
			return fmt.Errorf("cant setup gateway connection: %w", err)
		}
		defer closer()

		maddr, err := address.NewFromString(cctx.String("provider"))
		if err != nil {
			return err
		}

		addrInfo, err := cmd.GetAddrInfo(ctx, api, maddr)
		if err != nil {
			return err
		}

		walletAddr, err := n.GetProvidedOrDefaultWallet(ctx, cctx.String("wallet"))
		if err != nil {
			return err
		}

		log.Debugw("found storage provider", "id", addrInfo.ID, "multiaddrs", addrInfo.Addrs, "addr", maddr)

		if err := n.Host.Connect(ctx, *addrInfo); err != nil {
			return fmt.Errorf("failed to connect to peer %s: %w", addrInfo.ID, err)
		}

		query := types.Query{
			PieceCID: &pieceCID,
		}

		buf, err := types.BindnodeRegistry.TypeToBytes(&query, dagcbor.Encode)
		if err != nil {
			return err
		}

		sig, err := n.Wallet.WalletSign(ctx, walletAddr, buf, lapi.MsgMeta{})
		if err != nil {
			return err
		}

		dc := lp2pimpl.NewQueryClient(n.Host)
		resp, err := dc.SendQuery(ctx, addrInfo.ID, types.SignedQuery{
			Query:           query,
			ClientAddress:   &walletAddr,
			ClientSignature: sig,
		})

		if err != nil {
			return fmt.Errorf("send deal status request failed: %w", err)
		}

		if resp.Error != "" {
			return fmt.Errorf("query response error: %s", resp.Error)
		}
		fmt.Println(resp.Protocols.HTTPFilecoinV1.URL)
		return nil
	},
}
