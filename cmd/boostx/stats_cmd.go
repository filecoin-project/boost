package main

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/filecoin-project/boost/retrievalmarket/lp2pimpl"
	transports_types "github.com/filecoin-project/boost/retrievalmarket/types"

	clinode "github.com/filecoin-project/boost/cli/node"
	"github.com/filecoin-project/boost/cmd"
	"github.com/filecoin-project/boost/extern/boostd-data/shared/cliutil"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/chain/actors/builtin/power"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/urfave/cli/v2"
)

var statsCmd = &cli.Command{
	Name:        "stats",
	Description: "Statistics on how many SPs are running Boost",
	Before:      before,
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:  "lotus-read-concurrency",
			Value: 50,
		},
		&cli.IntFlag{
			Name:  "sp-query-concurrency",
			Value: 1,
		},
		&cli.IntFlag{
			Name:  "sp-query-max",
			Value: 0,
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := cliutil.ReqContext(cctx)

		n, err := clinode.Setup(cctx.String(cmd.FlagRepo.Name))
		if err != nil {
			return fmt.Errorf("setting up CLI node: %w", err)
		}

		api, closer, err := lcli.GetGatewayAPIV1(cctx)
		if err != nil {
			return fmt.Errorf("setting up gateway connection: %w", err)
		}
		defer closer()

		miners, err := api.StateListMiners(ctx, types.EmptyTSK)
		if err != nil {
			return err
		}

		fmt.Println("Total SPs on chain: ", len(miners))

		var wg sync.WaitGroup
		wg.Add(len(miners))
		var lk sync.Mutex
		var withMinPower []address.Address
		minerToMinerPower := make(map[address.Address]power.Claim)
		minerToTotalPower := make(map[address.Address]power.Claim)

		throttle := make(chan struct{}, cctx.Int("lotus-read-concurrency"))
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
					minerToMinerPower[miner] = power.MinerPower
					minerToTotalPower[miner] = power.TotalPower
					lk.Unlock()
				}
			}(miner)
		}

		wg.Wait()

		fmt.Println("Total SPs with minimum power: ", len(withMinPower))

		var boostNodes, marketsNodes, venusNodes, noProtocolsNodes, indexerNodes, curioNodes int
		boostRawBytePower := big.NewInt(0)
		boostQualityAdjPower := big.NewInt(0)
		venusRawBytePower := big.NewInt(0)
		venusQualityAdjPower := big.NewInt(0)
		agentVersions := make(map[string]int)
		transportProtos := make(map[string]int)
		curioRawBytePower := big.NewInt(0)
		curioQualityAdjPower := big.NewInt(0)

		throttle = make(chan struct{}, cctx.Int("sp-query-concurrency"))
		for i, maddr := range withMinPower {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			spQueryMax := cctx.Int("sp-query-max")
			if spQueryMax > 0 && i >= spQueryMax {
				break
			}

			throttle <- struct{}{}
			wg.Add(1)
			go func(maddr address.Address) {
				defer wg.Done()
				defer func() {
					<-throttle
				}()
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

					// Get peer's libp2p protocols
					protos, err := n.Host.Peerstore().GetProtocols(addrInfo.ID)
					if err != nil {
						return fmt.Errorf("getting protocols for peer %s: %w", addrInfo.ID, err)
					}
					protostrs := make([]string, 0, len(protos))
					for _, proto := range protos {
						protostrs = append(protostrs, string(proto))
					}
					sort.Strings(protostrs)

					// Get peer's libp2p agent version
					agentVersionI, err := n.Host.Peerstore().Get(addrInfo.ID, "AgentVersion")
					if err != nil {
						return fmt.Errorf("getting agent version for peer %s: %w", addrInfo.ID, err)
					}
					agentVersion, ok := agentVersionI.(string)
					if !ok {
						return fmt.Errorf("AgentVersion for peer %s is not a string: type %T", addrInfo.ID, agentVersionI)
					}
					agentVersionShort := shortAgentVersion(agentVersion)

					// Get SP's supported transports
					var transports *transports_types.QueryResponse
					if contains(protostrs, string(lp2pimpl.TransportsProtocolID)) {
						client := lp2pimpl.NewTransportsClient(n.Host)
						transports, err = client.SendQuery(ctx, addrInfo.ID)
						if err != nil {
							fmt.Printf("Failed to fetch transports from peer %s: %s\n", addrInfo.ID, err)
						}
					}

					lk.Lock()
					var out string
					out += "Provider " + maddr.String()
					if strings.Contains(agentVersion, "curio") {
						curioNodes++
						curioQualityAdjPower = big.Add(curioQualityAdjPower, minerToMinerPower[maddr].QualityAdjPower)
						curioRawBytePower = big.Add(curioRawBytePower, minerToMinerPower[maddr].RawBytePower)
					} else if strings.Contains(agentVersion, "venus") || strings.Contains(agentVersion, "droplet") {
						out += " is running venus"

						venusNodes++
						venusQualityAdjPower = big.Add(venusQualityAdjPower, minerToMinerPower[maddr].QualityAdjPower)
						venusRawBytePower = big.Add(venusRawBytePower, minerToMinerPower[maddr].RawBytePower)
					} else if contains(protostrs, "/fil/storage/mk/1.2.0") {
						out += " is running boost"

						boostNodes++
						boostQualityAdjPower = big.Add(boostQualityAdjPower, minerToMinerPower[maddr].QualityAdjPower)
						boostRawBytePower = big.Add(boostRawBytePower, minerToMinerPower[maddr].RawBytePower)
						agentVersions[agentVersionShort]++
						if transports != nil {
							for _, p := range transports.Protocols {
								transportProtos[p.Name]++
							}
						}
					} else if contains(protostrs, "/fil/storage/mk/1.1.0") {
						out += " is running markets"
						agentVersions[agentVersionShort]++
						marketsNodes++
					} else {
						out += " is not running markets or boost"
						noProtocolsNodes++
					}
					if contains(protostrs, "/legs/head/") {
						out += " (with indexer)"
						indexerNodes++
					}
					lk.Unlock()

					out += "\n"
					out += "  agent version:     " + agentVersion + "\n"
					out += "  raw power:         " + minerToMinerPower[maddr].RawBytePower.String() + "\n"
					out += "  quality adj power: " + minerToMinerPower[maddr].QualityAdjPower.String() + "\n"
					out += "  protocols:\n"
					out += "    " + strings.Join(protostrs, "\n    ") + "\n"

					if transports != nil {
						out += "  transports:\n"
						for _, p := range transports.Protocols {
							out += "    " + p.Name + "\n"
						}
					}

					fmt.Print(out)
					return nil
				}()
				if err != nil {
					fmt.Println("Error: ", err)
				}
			}(maddr)
		}

		wg.Wait()

		fmt.Println()
		fmt.Println("Total Boost nodes:", boostNodes)
		fmt.Println("Total Boost raw power:", boostRawBytePower)
		fmt.Println("Total Boost quality adj power:", boostQualityAdjPower)
		fmt.Println("Total Curio nodes:", curioNodes)
		fmt.Println("Total Curio raw power:", curioRawBytePower)
		fmt.Println("Total Curio quality adj power:", curioQualityAdjPower)
		fmt.Println("Total Venus nodes:", venusNodes)
		fmt.Println("Total Venus raw power:", venusRawBytePower)
		fmt.Println("Total Venus quality adj power:", venusQualityAdjPower)
		fmt.Println("Total Markets nodes:", marketsNodes)
		fmt.Println("Total SPs with minimum power: ", len(withMinPower))
		fmt.Println("Total Indexer nodes:", indexerNodes)

		agentVersionsOrder := make([]string, 0, len(agentVersions))
		for av := range agentVersions {
			agentVersionsOrder = append(agentVersionsOrder, av)
		}
		sort.Strings(agentVersionsOrder)
		fmt.Println("Agent Versions:")
		for _, av := range agentVersionsOrder {
			fmt.Printf("  %s: %d\n", av, agentVersions[av])
		}

		transportsOrder := make([]string, 0, len(transportProtos))
		for transport := range transportProtos {
			transportsOrder = append(transportsOrder, transport)
		}
		sort.Strings(transportsOrder)
		fmt.Println("Retrieval Protocol Support:")
		for _, transport := range transportsOrder {
			fmt.Printf("  %s: %d\n", transport, transportProtos[transport])
		}

		return nil
	},
}

func contains(sl []string, substr string) bool {
	for _, s := range sl {
		if strings.Contains(s, substr) {
			return true
		}
	}

	return false
}

var agentVersionRegExp = regexp.MustCompile(`^(.+)\+.+$`)

func shortAgentVersion(av string) string {
	return agentVersionRegExp.ReplaceAllString(av, "$1")
}
