package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/mitchellh/go-homedir"

	"github.com/filecoin-project/boost/cli/node"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	cli "github.com/urfave/cli/v2"
)

var initCmd = &cli.Command{
	Name:  "init",
	Usage: "Initialise Boost client repo",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "repo",
			Usage: "repo directory for Boost client",
			Value: "~/.boost-client",
		},
	},
	Before: before,
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)

		sdir, err := homedir.Expand(cctx.String("repo"))
		if err != nil {
			return err
		}

		os.Mkdir(sdir, 0755) //nolint:errcheck

		n, err := node.Setup(ctx, sdir)
		if err != nil {
			return err
		}

		addr := cctx.String("gateway-url")
		api, closer, err := client.NewGatewayRPCV1(ctx, addr, nil)
		if err != nil {
			log.Warnw("couldnt connect to gateway", "addr", addr)

			api, closer, err = lcli.GetGatewayAPI(cctx)
			if err != nil {
				return fmt.Errorf("cant setup gateway connection: %w", err)
			}
		}
		defer closer()

		walletAddr, err := n.Wallet.GetDefault()
		if err != nil {
			return err
		}

		log.Infow("default wallet set", "wallet", walletAddr)

		walletBalance, err := api.WalletBalance(ctx, walletAddr)
		if err != nil {
			return err
		}

		log.Infow("wallet balance", "value", types.FIL(walletBalance).Short())

		marketBalance, err := api.StateMarketBalance(ctx, walletAddr, types.EmptyTSK)
		if err != nil {
			if strings.Contains(err.Error(), "actor not found") {
				log.Warn("market actor is not initialised, you must add funds to it in order to send online deals")

				return nil
			}
			return err
		}

		log.Infow("market balance", "escrow", types.FIL(marketBalance.Escrow).Short(), "locked", types.FIL(marketBalance.Locked).Short())

		return nil
	},
}
