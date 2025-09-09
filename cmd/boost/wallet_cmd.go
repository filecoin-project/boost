package main

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/boost/cli/node"
	"github.com/filecoin-project/boost/cmd"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/lib/tablewriter"
	"github.com/urfave/cli/v2"
	"golang.org/x/term"
)

var walletCmd = &cli.Command{
	Name:  "wallet",
	Usage: "Manage wallets with Boost",
	Subcommands: []*cli.Command{
		walletNew,
		walletList,
		walletBalance,
		walletExport,
		walletImport,
		walletGetDefault,
		walletSetDefault,
		walletDelete,
		walletSign,
	},
}

var walletNew = &cli.Command{
	Name:      "new",
	Usage:     "Generate a new key of the given type",
	ArgsUsage: "[bls|secp256k1|delegated (default secp256k1)]",
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)

		n, err := node.Setup(cctx.String(cmd.FlagRepo.Name))
		if err != nil {
			return err
		}

		t := cctx.Args().First()
		if t == "" {
			t = "secp256k1"
		}

		nk, err := n.Wallet.WalletNew(ctx, types.KeyType(t))
		if err != nil {
			return err
		}

		if cctx.Bool("json") {
			out := map[string]interface{}{
				"address": nk.String(),
			}
			cmd.PrintJson(out) //nolint:errcheck
		} else {
			fmt.Println(nk.String())
		}

		return nil
	},
}

var walletList = &cli.Command{
	Name:  "list",
	Usage: "List wallet address",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:    "addr-only",
			Usage:   "Only print addresses",
			Aliases: []string{"a"},
		},
		&cli.BoolFlag{
			Name:    "id",
			Usage:   "Output ID addresses",
			Aliases: []string{"i"},
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)

		n, err := node.Setup(cctx.String(cmd.FlagRepo.Name))
		if err != nil {
			return err
		}

		api, closer, err := lcli.GetGatewayAPIV1(cctx)
		if err != nil {
			return fmt.Errorf("cant setup gateway connection: %w", err)
		}
		defer closer()

		afmt := NewAppFmt(cctx.App)

		addrs, err := n.Wallet.WalletList(ctx)
		if err != nil {
			return err
		}

		// Assume an error means no default key is set
		def, _ := n.Wallet.GetDefault()

		// Map Keys. Corresponds to the standard tablewriter output
		addressKey := "Address"
		idKey := "ID"
		balanceKey := "Balance"
		marketKey := "market" // for json only
		marketAvailKey := "Market(Avail)"
		marketLockedKey := "Market(Locked)"
		nonceKey := "Nonce"
		defaultKey := "Default"
		errorKey := "Error"
		dataCapKey := "DataCap"

		// One-to-one mapping between tablewriter keys and JSON keys
		tableKeysToJsonKeys := map[string]string{
			addressKey: strings.ToLower(addressKey),
			idKey:      strings.ToLower(idKey),
			balanceKey: strings.ToLower(balanceKey),
			marketKey:  marketKey, // only in JSON
			nonceKey:   strings.ToLower(nonceKey),
			defaultKey: strings.ToLower(defaultKey),
			errorKey:   strings.ToLower(errorKey),
			dataCapKey: strings.ToLower(dataCapKey),
		}

		// List of Maps whose keys are defined above. One row = one list element = one wallet
		var wallets []map[string]interface{}

		for _, addr := range addrs {
			if cctx.Bool("addr-only") {
				afmt.Println(addr.String())
			} else {
				a, err := api.StateGetActor(ctx, addr, types.EmptyTSK)
				if err != nil {
					if !strings.Contains(err.Error(), "actor not found") {
						wallet := map[string]interface{}{
							addressKey: addr,
							errorKey:   err,
						}
						wallets = append(wallets, wallet)
						continue
					}

					a = &types.Actor{
						Balance: big.Zero(),
					}
				}

				wallet := map[string]interface{}{
					addressKey: addr,
					balanceKey: types.FIL(a.Balance),
					nonceKey:   a.Nonce,
				}

				if cctx.Bool("json") {
					if addr == def {
						wallet[defaultKey] = true
					} else {
						wallet[defaultKey] = false
					}
				} else {
					if addr == def {
						wallet[defaultKey] = "X"
					}
				}

				if cctx.Bool("id") {
					id, err := api.StateLookupID(ctx, addr, types.EmptyTSK)
					if err != nil {
						wallet[idKey] = "n/a"
					} else {
						wallet[idKey] = id
					}
				}

				mbal, err := api.StateMarketBalance(ctx, addr, types.EmptyTSK)
				if err == nil {
					marketAvailValue := types.FIL(types.BigSub(mbal.Escrow, mbal.Locked))
					marketLockedValue := types.FIL(mbal.Locked)
					// structure is different for these particular keys so we have to distinguish the cases here
					if cctx.Bool("json") {
						wallet[marketKey] = map[string]interface{}{
							"available": marketAvailValue,
							"locked":    marketLockedValue,
						}
					} else {
						wallet[marketAvailKey] = marketAvailValue
						wallet[marketLockedKey] = marketLockedValue
					}
				}
				dcap, err := api.StateVerifiedClientStatus(ctx, addr, types.EmptyTSK)
				if err == nil {
					wallet[dataCapKey] = dcap
					if !cctx.Bool("json") && dcap == nil {
						wallet[dataCapKey] = "X"
					} else if dcap != nil {
						wallet[dataCapKey] = humanize.IBytes(dcap.Uint64())
					}
				} else {
					wallet[dataCapKey] = "n/a"
					if cctx.Bool("json") {
						wallet[dataCapKey] = nil
					}
				}

				wallets = append(wallets, wallet)
			}
		}

		if !cctx.Bool("addr-only") {

			if cctx.Bool("json") {
				// get a new list of wallets with json keys instead of tablewriter keys
				var jsonWallets []map[string]interface{}
				for _, wallet := range wallets {
					jsonWallet := make(map[string]interface{})
					for k, v := range wallet {
						jsonWallet[tableKeysToJsonKeys[k]] = v
					}
					jsonWallets = append(jsonWallets, jsonWallet)
				}
				// then return this!
				return cmd.PrintJson(jsonWallets)
			} else {
				// Init the tablewriter's columns
				tw := tablewriter.New(
					tablewriter.Col(addressKey),
					tablewriter.Col(idKey),
					tablewriter.Col(balanceKey),
					tablewriter.Col(marketAvailKey),
					tablewriter.Col(marketLockedKey),
					tablewriter.Col(nonceKey),
					tablewriter.Col(defaultKey),
					tablewriter.NewLineCol(errorKey))
				// populate it with content
				for _, wallet := range wallets {
					tw.Write(wallet)
				}
				// return the corresponding string
				return tw.Flush(os.Stdout)
			}
		}

		return nil
	},
}

var walletBalance = &cli.Command{
	Name:      "balance",
	Usage:     "Get account balance",
	ArgsUsage: "[address]",
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)

		n, err := node.Setup(cctx.String(cmd.FlagRepo.Name))
		if err != nil {
			return err
		}

		api, closer, err := lcli.GetGatewayAPIV1(cctx)
		if err != nil {
			return fmt.Errorf("cant setup gateway connection: %w", err)
		}
		defer closer()

		afmt := NewAppFmt(cctx.App)

		var addr address.Address
		if cctx.Args().First() != "" {
			addr, err = address.NewFromString(cctx.Args().First())
		} else {
			addr, err = n.Wallet.GetDefault()
		}
		if err != nil {
			return err
		}

		balance, err := api.WalletBalance(ctx, addr)
		if err != nil {
			return err
		}

		if balance.Equals(types.NewInt(0)) {
			warningMessage := "may display 0 if chain sync in progress"
			if cctx.Bool("json") {
				out := map[string]interface{}{
					"balance": types.FIL(balance),
					"warning": warningMessage,
				}
				return cmd.PrintJson(out)
			} else {
				afmt.Printf("%s", fmt.Sprintf("%s (warning: %s)\n", types.FIL(balance), warningMessage))
			}
		} else {
			if cctx.Bool("json") {
				out := map[string]interface{}{
					"balance": types.FIL(balance),
				}
				return cmd.PrintJson(out)
			} else {
				afmt.Printf("%s\n", types.FIL(balance))
			}
		}

		return nil
	},
}

var walletExport = &cli.Command{
	Name:      "export",
	Usage:     "export keys",
	ArgsUsage: "[address]",
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)

		n, err := node.Setup(cctx.String(cmd.FlagRepo.Name))
		if err != nil {
			return err
		}

		afmt := NewAppFmt(cctx.App)

		if !cctx.Args().Present() {
			err := fmt.Errorf("must specify key to export")
			return err
		}

		addr, err := address.NewFromString(cctx.Args().First())
		if err != nil {
			return err
		}

		ki, err := n.Wallet.WalletExport(ctx, addr)
		if err != nil {
			return err
		}

		b, err := json.Marshal(ki)
		if err != nil {
			return err
		}

		if cctx.Bool("json") {
			out := map[string]interface{}{
				"key": hex.EncodeToString(b),
			}
			return cmd.PrintJson(out)
		} else {
			afmt.Println(hex.EncodeToString(b))
		}
		return nil
	},
}

var walletImport = &cli.Command{
	Name:      "import",
	Usage:     "import keys",
	ArgsUsage: "[<path> (optional, will read from stdin if omitted)]",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "format",
			Usage: "specify input format for key",
			Value: "hex-lotus",
		},
		&cli.BoolFlag{
			Name:  "as-default",
			Usage: "import the given key as your new default key",
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)

		n, err := node.Setup(cctx.String(cmd.FlagRepo.Name))
		if err != nil {
			return err
		}

		var inpdata []byte
		if !cctx.Args().Present() || cctx.Args().First() == "-" {
			if term.IsTerminal(int(os.Stdin.Fd())) {
				fmt.Print("Enter private key(not display in the terminal): ")

				sigCh := make(chan os.Signal, 1)
				// Notify the channel when SIGINT is received
				signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

				go func() {
					<-sigCh
					fmt.Println("\nInterrupt signal received. Exiting...")
					os.Exit(1)
				}()

				inpdata, err = term.ReadPassword(int(os.Stdin.Fd()))
				if err != nil {
					return err
				}
				fmt.Println()
			} else {
				reader := bufio.NewReader(os.Stdin)
				indata, err := reader.ReadBytes('\n')
				if err != nil {
					return err
				}
				inpdata = indata
			}

		} else {
			fdata, err := os.ReadFile(cctx.Args().First())
			if err != nil {
				return err
			}
			inpdata = fdata
		}

		var ki types.KeyInfo
		switch cctx.String("format") {
		case "hex-lotus":
			data, err := hex.DecodeString(strings.TrimSpace(string(inpdata)))
			if err != nil {
				return err
			}

			if err := json.Unmarshal(data, &ki); err != nil {
				return err
			}
		case "json-lotus":
			if err := json.Unmarshal(inpdata, &ki); err != nil {
				return err
			}
		case "gfc-json":
			var f struct {
				KeyInfo []struct {
					PrivateKey []byte
					SigType    int
				}
			}
			if err := json.Unmarshal(inpdata, &f); err != nil {
				return fmt.Errorf("failed to parse go-filecoin key: %s", err)
			}

			gk := f.KeyInfo[0]
			ki.PrivateKey = gk.PrivateKey
			switch gk.SigType {
			case 1:
				ki.Type = types.KTSecp256k1
			case 2:
				ki.Type = types.KTBLS
			case 3:
				ki.Type = types.KTDelegated
			default:
				return fmt.Errorf("unrecognized key type: %d", gk.SigType)
			}
		default:
			return fmt.Errorf("unrecognized format: %s", cctx.String("format"))
		}

		addr, err := n.Wallet.WalletImport(ctx, &ki)
		if err != nil {
			return err
		}

		if cctx.Bool("as-default") {
			if err := n.Wallet.SetDefault(addr); err != nil {
				return fmt.Errorf("failed to set default key: %w", err)
			}
		}

		if cctx.Bool("json") {
			out := map[string]interface{}{
				"address": addr,
			}
			return cmd.PrintJson(out)
		} else {
			fmt.Printf("imported key %s successfully!\n", addr)
		}
		return nil
	},
}

var walletGetDefault = &cli.Command{
	Name:    "default",
	Usage:   "Get default wallet address",
	Aliases: []string{"get-default"},
	Action: func(cctx *cli.Context) error {
		n, err := node.Setup(cctx.String(cmd.FlagRepo.Name))
		if err != nil {
			return err
		}

		afmt := NewAppFmt(cctx.App)

		addr, err := n.Wallet.GetDefault()
		if err != nil {
			return err
		}

		if cctx.Bool("json") {
			out := map[string]interface{}{
				"address": addr.String(),
			}
			return cmd.PrintJson(out)
		} else {
			afmt.Printf("%s\n", addr.String())
		}
		return nil
	},
}

var walletSetDefault = &cli.Command{
	Name:      "set-default",
	Usage:     "Set default wallet address",
	ArgsUsage: "[address]",
	Action: func(cctx *cli.Context) error {
		n, err := node.Setup(cctx.String(cmd.FlagRepo.Name))
		if err != nil {
			return err
		}

		if !cctx.Args().Present() {
			return fmt.Errorf("must pass address to set as default")
		}

		addr, err := address.NewFromString(cctx.Args().First())
		if err != nil {
			return err
		}

		return n.Wallet.SetDefault(addr)
	},
}

var walletDelete = &cli.Command{
	Name:      "delete",
	Usage:     "Delete an account from the wallet",
	ArgsUsage: "<address> ",
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)

		n, err := node.Setup(cctx.String(cmd.FlagRepo.Name))
		if err != nil {
			return err
		}

		if !cctx.Args().Present() || cctx.NArg() != 1 {
			return fmt.Errorf("must specify address to delete")
		}

		addr, err := address.NewFromString(cctx.Args().First())
		if err != nil {
			return err
		}

		return n.Wallet.WalletDelete(ctx, addr)
	},
}

var walletSign = &cli.Command{
	Name:      "sign",
	Usage:     "Sign a message",
	ArgsUsage: "<signing address> <hexMessage>",
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)

		n, err := node.Setup(cctx.String(cmd.FlagRepo.Name))
		if err != nil {
			return err
		}

		if !cctx.Args().Present() || cctx.NArg() != 2 {
			return fmt.Errorf("must specify signing address and message to sign")
		}

		addr, err := address.NewFromString(cctx.Args().First())
		if err != nil {
			return err
		}

		msg, err := hex.DecodeString(cctx.Args().Get(1))
		if err != nil {
			return err
		}

		sig, err := n.Wallet.WalletSign(ctx, addr, msg, api.MsgMeta{Type: api.MTUnknown})
		if err != nil {
			return err
		}

		sigBytes := append([]byte{byte(sig.Type)}, sig.Data...)

		if cctx.Bool("json") {
			out := map[string]interface{}{
				"signature": hex.EncodeToString(sigBytes),
			}
			err := cmd.PrintJson(out)
			if err != nil {
				return err
			}
		} else {
			fmt.Println(hex.EncodeToString(sigBytes))
		}

		return nil
	},
}
