package main

import (
	"fmt"
	"sort"
	"strings"

	clinode "github.com/filecoin-project/boost/cli/node"
	cliutil "github.com/filecoin-project/boost/cli/util"
	"github.com/filecoin-project/boost/cmd"
	"github.com/filecoin-project/go-address"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/urfave/cli/v2"
)

var providerCmd = &cli.Command{
	Name:  "provider",
	Usage: "Info about Storage Providers",
	Flags: []cli.Flag{cmd.FlagRepo},
	Subcommands: []*cli.Command{
		protocolsCmd,
	},
}

var protocolsCmd = &cli.Command{
	Name:        "protocols",
	Usage:       "",
	ArgsUsage:   "<provider address>",
	Description: "Lists the libp2p protocols supported by the Storage Provider",
	Before:      before,
	Action: func(cctx *cli.Context) error {
		ctx := cliutil.ReqContext(cctx)

		if cctx.Args().Len() != 1 {
			return fmt.Errorf("usage: protocols <provider address>")
		}

		n, err := clinode.Setup(cctx.String(cmd.FlagRepo.Name))
		if err != nil {
			return fmt.Errorf("setting up CLI node: %w", err)
		}

		api, closer, err := lcli.GetGatewayAPI(cctx)
		if err != nil {
			return fmt.Errorf("settings up gateway connection: %w", err)
		}
		defer closer()

		addrStr := cctx.Args().Get(0)
		maddr, err := address.NewFromString(addrStr)
		if err != nil {
			return fmt.Errorf("parsing provider on-chain address %s: %w", addrStr, err)
		}

		addrInfo, err := cmd.GetAddrInfo(ctx, api, maddr)
		if err != nil {
			return fmt.Errorf("getting provider address: %w", err)
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

		if cctx.Bool("json") {
			return cmd.PrintJson(map[string]interface{}{"provider": addrStr, "protocols": protos})
		}

		fmt.Println(strings.Join(protos, "\n"))
		return nil
	},
}
