package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/filecoin-project/boost/cli/node"
	"github.com/filecoin-project/boost/cmd"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/mitchellh/go-homedir"
	cli "github.com/urfave/cli/v2"
)

var initCmd = &cli.Command{
	Name:   "init",
	Usage:  "Initialise Boost client repo",
	Before: before,
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)

		sdir, err := homedir.Expand(cctx.String(cmd.FlagRepo.Name))
		if err != nil {
			return err
		}

		os.Mkdir(sdir, 0755) //nolint:errcheck

		n, err := node.Setup(cctx.String(cmd.FlagRepo.Name))
		if err != nil {
			return err
		}

		api, closer, err := lcli.GetGatewayAPIV1(cctx)
		if err != nil {
			return fmt.Errorf("cant setup gateway connection: %w", err)
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
