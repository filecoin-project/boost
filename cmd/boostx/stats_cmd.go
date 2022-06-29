package main

import (
	"fmt"
	"sort"
	"sync"

	"github.com/filecoin-project/boost/cli/ctxutil"
	clinode "github.com/filecoin-project/boost/cli/node"
	"github.com/filecoin-project/boost/cmd"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/urfave/cli/v2"
)

var statsCmd = &cli.Command{
	Name:        "stats",
	Usage:       "",
	ArgsUsage:   "",
	Description: "",
	Before:      before,
	Action: func(cctx *cli.Context) error {
		ctx := ctxutil.ReqContext(cctx)

		n, err := clinode.Setup(cctx.String(cmd.FlagRepo.Name))
		if err != nil {
			return fmt.Errorf("setting up CLI node: %w", err)
		}

		api, closer, err := lcli.GetGatewayAPI(cctx)
		if err != nil {
			return fmt.Errorf("setting up gateway connection: %w", err)
		}
		defer closer()

		miners, err := api.StateListMiners(ctx, types.EmptyTSK)
		if err != nil {
			return err
		}

		fmt.Println("got len total miners: ", len(miners))

		var wg sync.WaitGroup
		wg.Add(len(miners))
		var lk sync.Mutex
		var withMinPower []address.Address

		throttle := make(chan struct{}, 50)
		for _, miner := range miners {
			throttle <- struct{}{}
			go func(miner address.Address) {
				defer wg.Done()
				defer func() {
					<-throttle
				}()

				power, err := api.StateMinerPower(ctx, miner, types.EmptyTSK)
				if err != nil {
					panic(err)
				}

				if power.HasMinPower { // TODO: Lower threshold
					lk.Lock()
					withMinPower = append(withMinPower, miner)
					lk.Unlock()
				}
			}(miner)
		}

		wg.Wait()

		fmt.Println("got len miners with min power: ", len(withMinPower))

		var boostNodes, marketsNodes, noProtocolsNodes int

		for _, maddr := range withMinPower {
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			err := func() error {
				addrInfo, err := cmd.GetAddrInfo(ctx, api, maddr)
				if err != nil {
					return fmt.Errorf("getting provider multi-address: %w", err)
				}

				log.Debugw("connecting to storage provider",
					"id", addrInfo.ID, "multiaddrs", addrInfo.Addrs, "addr", maddr)

				if err := n.Host.Connect(ctx, *addrInfo); err != nil {
					return fmt.Errorf("connecting to peer %s: %w", addrInfo.ID, err)
				}

				protos, err := n.Host.Peerstore().GetProtocols(addrInfo.ID)
				if err != nil {
					return fmt.Errorf("getting protocols for peer %s: %w", addrInfo.ID, err)
				}
				sort.Strings(protos)

				fmt.Print("provider " + maddr.String())
				if contains(protos, "/fil/storage/mk/1.2.0") {
					fmt.Print(" running boost")
					boostNodes++
				} else if contains(protos, "/fil/storage/mk/1.1.0") {
					fmt.Print(" running markets")
					marketsNodes++
				} else {
					fmt.Print(" running fewer protocols")
					noProtocolsNodes++
				}
				fmt.Println()

				return nil
			}()
			if err != nil {
				fmt.Println("err: ", err)
				continue
			}
		}

		fmt.Println("total boost nodes:", boostNodes)
		fmt.Println("total markets nodes:", marketsNodes)
		fmt.Println("total few-protocols nodes:", noProtocolsNodes)

		return nil
	},
}
