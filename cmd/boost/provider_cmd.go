package main

import (
	"fmt"
	"sort"
	"strings"

	bcli "github.com/filecoin-project/boost/cli"
	clinode "github.com/filecoin-project/boost/cli/node"
	"github.com/filecoin-project/boost/cmd"
	"github.com/filecoin-project/boost/extern/boostd-data/shared/cliutil"
	"github.com/filecoin-project/boost/retrievalmarket/lp2pimpl"
	"github.com/filecoin-project/boost/storagemarket/types/legacytypes/network"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/ipni/go-libipni/maurl"
	"github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
)

const (
	AskProtocolID   = "/fil/storage/ask/1.1.0"
	QueryProtocolID = "/fil/retrieval/qry/1.0.0"
)

var providerCmd = &cli.Command{
	Name:  "provider",
	Usage: "Info about Storage Providers",
	Subcommands: []*cli.Command{
		libp2pInfoCmd,
		storageAskCmd,
		retrievalTransportsCmd,
	},
}

var libp2pInfoCmd = &cli.Command{
	Name:        "libp2p-info",
	Usage:       "",
	ArgsUsage:   "<provider address>",
	Description: "Lists the libp2p address and protocols supported by the Storage Provider",
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

		api, closer, err := lcli.GetGatewayAPIV1(cctx)
		if err != nil {
			return fmt.Errorf("setting up gateway connection: %w", err)
		}
		defer closer()

		addrStr := cctx.Args().Get(0)
		maddr, err := address.NewFromString(addrStr)
		if err != nil {
			return fmt.Errorf("parsing provider on-chain address %s: %w", addrStr, err)
		}

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
		protostrs := make([]string, 0, len(protos))
		for _, proto := range protos {
			protostrs = append(protostrs, string(proto))
		}
		sort.Strings(protostrs)

		agentVersionI, err := n.Host.Peerstore().Get(addrInfo.ID, "AgentVersion")
		if err != nil {
			return fmt.Errorf("getting agent version for peer %s: %w", addrInfo.ID, err)
		}
		agentVersion, _ := agentVersionI.(string)

		if cctx.Bool("json") {
			return cmd.PrintJson(map[string]interface{}{
				"provider":   addrStr,
				"agent":      agentVersion,
				"id":         addrInfo.ID.String(),
				"multiaddrs": addrInfo.Addrs,
				"protocols":  protostrs,
			})
		}

		fmt.Println("Provider: " + addrStr)
		fmt.Println("Agent: " + agentVersion)
		fmt.Println("Peer ID: " + addrInfo.ID.String())
		fmt.Println("Peer Addresses:")
		for _, addr := range addrInfo.Addrs {
			fmt.Println("  " + addr.String())
		}
		fmt.Println("Protocols:\n" + "  " + strings.Join(protostrs, "\n  "))
		return nil
	},
}

var storageAskCmd = &cli.Command{
	Name:      "storage-ask",
	Usage:     "Query a storage provider's storage ask",
	ArgsUsage: "[provider]",
	Flags: []cli.Flag{
		&cli.Int64Flag{
			Name:  "size",
			Usage: "data size in bytes",
		},
		&cli.Int64Flag{
			Name:  "duration",
			Usage: "deal duration in epochs",
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := bcli.ReqContext(cctx)

		afmt := NewAppFmt(cctx.App)

		if cctx.NArg() != 1 {
			afmt.Println("Usage: storage-ask [provider]")
			return nil
		}

		n, err := clinode.Setup(cctx.String(cmd.FlagRepo.Name))
		if err != nil {
			return err
		}

		api, closer, err := lcli.GetGatewayAPIV1(cctx)
		if err != nil {
			return fmt.Errorf("cant setup gateway connection: %w", err)
		}
		defer closer()

		maddr, err := address.NewFromString(cctx.Args().First())
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

		s, err := n.Host.NewStream(ctx, addrInfo.ID, AskProtocolID)
		if err != nil {
			return fmt.Errorf("failed to open stream to peer %s: %w", addrInfo.ID, err)
		}
		defer func() {
			_ = s.Close()
		}()

		var resp network.AskResponse

		askRequest := network.AskRequest{
			Miner: maddr,
		}

		if err := doRpc(ctx, s, &askRequest, &resp); err != nil {
			return fmt.Errorf("send ask request rpc: %w", err)
		}

		ask := resp.Ask.Ask

		afmt.Printf("Ask: %s\n", maddr)
		afmt.Printf("Price per GiB: %s\n", types.FIL(ask.Price))
		afmt.Printf("Verified Price per GiB: %s\n", types.FIL(ask.VerifiedPrice))
		afmt.Printf("Max Piece size: %s\n", types.SizeStr(types.NewInt(uint64(ask.MaxPieceSize))))
		afmt.Printf("Min Piece size: %s\n", types.SizeStr(types.NewInt(uint64(ask.MinPieceSize))))

		size := cctx.Int64("size")
		if size == 0 {
			return nil
		}
		perEpoch := types.BigDiv(types.BigMul(ask.Price, types.NewInt(uint64(size))), types.NewInt(1<<30))
		afmt.Printf("Price per Block: %s\n", types.FIL(perEpoch))

		duration := cctx.Int64("duration")
		if duration == 0 {
			return nil
		}
		afmt.Printf("Total Price: %s\n", types.FIL(types.BigMul(perEpoch, types.NewInt(uint64(duration)))))

		return nil
	},
}

var retrievalTransportsCmd = &cli.Command{
	Name:      "retrieval-transports",
	Usage:     "Query a storage provider's available retrieval transports (libp2p, http, etc)",
	ArgsUsage: "[provider]",
	Action: func(cctx *cli.Context) error {
		ctx := bcli.ReqContext(cctx)

		afmt := NewAppFmt(cctx.App)
		if cctx.NArg() != 1 {
			afmt.Println("Usage: retrieval-transports [provider]")
			return nil
		}

		n, err := clinode.Setup(cctx.String(cmd.FlagRepo.Name))
		if err != nil {
			return err
		}

		api, closer, err := lcli.GetGatewayAPIV1(cctx)
		if err != nil {
			return fmt.Errorf("cant setup gateway connection: %w", err)
		}
		defer closer()

		maddr, err := address.NewFromString(cctx.Args().First())
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

		// Send the query to the Storage Provider
		client := lp2pimpl.NewTransportsClient(n.Host)
		resp, err := client.SendQuery(ctx, addrInfo.ID)
		if err != nil {
			return fmt.Errorf("failed to fetch transports from peer %s: %w", addrInfo.ID, err)
		}

		if cctx.Bool("json") {
			type addr struct {
				Multiaddr string `json:"multiaddr"`
				Address   string `json:"address,omitempty"`
			}
			json := make(map[string]interface{}, len(resp.Protocols))
			for _, p := range resp.Protocols {
				addrs := make([]addr, 0, len(p.Addresses))
				for _, ma := range p.Addresses {
					// Get the multiaddress, and also try to get the address
					// in the protocol's native format (eg URL format for
					// http protocol)
					addrs = append(addrs, addr{
						Multiaddr: ma.String(),
						Address:   multiaddrToNative(p.Name, ma),
					})
				}
				json[p.Name] = addrs
			}
			return cmd.PrintJson(json)
		}

		if len(resp.Protocols) == 0 {
			afmt.Println("No available retrieval protocols")
			return nil
		}
		for _, p := range resp.Protocols {
			afmt.Println(p.Name)
			for _, a := range p.Addresses {
				// Output the multiaddress
				afmt.Println("  " + a.String())
				// Try to get the address in the protocol's native format
				// (eg URL format for http protocol)
				nativeFmt := multiaddrToNative(p.Name, a)
				if nativeFmt != "" {
					afmt.Println("    " + nativeFmt)
				}
			}
		}

		return nil
	},
}

func multiaddrToNative(proto string, ma multiaddr.Multiaddr) string {
	switch proto {
	case "http", "https":
		u, err := maurl.ToURL(ma)
		if err != nil {
			return ""
		}
		return u.String()
	}

	return ""
}
