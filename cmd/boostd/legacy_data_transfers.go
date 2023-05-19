package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	bapi "github.com/filecoin-project/boost/api"
	datatransferv2 "github.com/filecoin-project/go-data-transfer/v2"
	"github.com/filecoin-project/lotus/api"
	"os"
	"strconv"
	"time"

	tm "github.com/buger/goterm"
	bcli "github.com/filecoin-project/boost/cli"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/urfave/cli/v2"
)

var dataTransfersCmd = &cli.Command{
	Name:     "data-transfers",
	Usage:    "Manage legacy data transfers (Markets V1)",
	Category: "legacy",
	Subcommands: []*cli.Command{
		transfersListCmd,
		marketRestartTransfer,
		marketCancelTransfer,
		transfersDiagnosticsCmd,
	},
}

var marketRestartTransfer = &cli.Command{
	Name:      "restart",
	Usage:     "Force restart a stalled data transfer",
	ArgsUsage: "<transfer id>",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "peerid",
			Usage: "narrow to transfer with specific peer",
		},
		&cli.BoolFlag{
			Name:  "initiator",
			Usage: "specify only transfers where peer is/is not initiator",
			Value: false,
		},
	},
	Action: func(cctx *cli.Context) error {
		if !cctx.Args().Present() {
			return cli.ShowCommandHelp(cctx, cctx.Command.Name)
		}
		nodeApi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := lcli.ReqContext(cctx)

		transferUint, err := strconv.ParseUint(cctx.Args().First(), 10, 64)
		if err != nil {
			return fmt.Errorf("Error reading transfer ID: %w", err)
		}
		transferID := datatransfer.TransferID(transferUint)
		initiator := cctx.Bool("initiator")
		var other peer.ID
		if pidstr := cctx.String("peerid"); pidstr != "" {
			p, err := peer.Decode(pidstr)
			if err != nil {
				return err
			}
			other = p
		} else {
			channels, err := nodeApi.MarketListDataTransfers(ctx)
			if err != nil {
				return err
			}
			found := false
			for _, channel := range channels {
				if channel.IsInitiator == initiator && channel.TransferID == transferID {
					other = channel.OtherPeer
					found = true
					break
				}
			}
			if !found {
				return errors.New("unable to find matching data transfer")
			}
		}

		return nodeApi.MarketRestartDataTransfer(ctx, transferID, other, initiator)
	},
}

var marketCancelTransfer = &cli.Command{
	Name:      "cancel",
	Usage:     "Force cancel a data transfer",
	ArgsUsage: "<transfer id>",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "peerid",
			Usage: "narrow to transfer with specific peer",
		},
		&cli.BoolFlag{
			Name:  "initiator",
			Usage: "specify only transfers where peer is/is not initiator",
			Value: false,
		},
		&cli.DurationFlag{
			Name:  "cancel-timeout",
			Usage: "time to wait for cancel to be sent to client",
			Value: 5 * time.Second,
		},
	},
	Action: func(cctx *cli.Context) error {
		if !cctx.Args().Present() {
			return cli.ShowCommandHelp(cctx, cctx.Command.Name)
		}
		nodeApi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := lcli.ReqContext(cctx)

		transferUint, err := strconv.ParseUint(cctx.Args().First(), 10, 64)
		if err != nil {
			return fmt.Errorf("Error reading transfer ID: %w", err)
		}
		transferID := datatransfer.TransferID(transferUint)
		initiator := cctx.Bool("initiator")
		var other peer.ID
		if pidstr := cctx.String("peerid"); pidstr != "" {
			p, err := peer.Decode(pidstr)
			if err != nil {
				return err
			}
			other = p
		} else {
			channels, err := nodeApi.MarketListDataTransfers(ctx)
			if err != nil {
				return err
			}
			found := false
			for _, channel := range channels {
				if channel.IsInitiator == initiator && channel.TransferID == transferID {
					other = channel.OtherPeer
					found = true
					break
				}
			}
			if !found {
				return errors.New("unable to find matching data transfer")
			}
		}

		timeoutCtx, cancel := context.WithTimeout(ctx, cctx.Duration("cancel-timeout"))
		defer cancel()
		return nodeApi.MarketCancelDataTransfer(timeoutCtx, transferID, other, initiator)
	},
}

var transfersListCmd = &cli.Command{
	Name:  "list",
	Usage: "List ongoing data transfers for this miner",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:    "verbose",
			Aliases: []string{"v"},
			Usage:   "print verbose transfer details",
		},
		&cli.BoolFlag{
			Name:  "completed",
			Usage: "show completed data transfers",
		},
		&cli.BoolFlag{
			Name:  "watch",
			Usage: "watch deal updates in real-time, rather than a one time list",
		},
		&cli.BoolFlag{
			Name:  "show-failed",
			Usage: "show failed/cancelled transfers",
		},
	},
	Action: func(cctx *cli.Context) error {
		api, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := lcli.ReqContext(cctx)

		channels, err := api.MarketListDataTransfers(ctx)
		if err != nil {
			return err
		}

		verbose := cctx.Bool("verbose")
		completed := cctx.Bool("completed")
		watch := cctx.Bool("watch")
		showFailed := cctx.Bool("show-failed")
		if watch {
			channelUpdates, err := api.MarketDataTransferUpdates(ctx)
			if err != nil {
				return err
			}

			for {
				tm.Clear() // Clear current screen

				tm.MoveCursor(1, 1)

				lcli.OutputDataTransferChannels(tm.Screen, toDTv2Channels(channels), verbose, completed, showFailed)

				tm.Flush()

				select {
				case <-ctx.Done():
					return nil
				case channelUpdate := <-channelUpdates:
					var found bool
					for i, existing := range channels {
						if existing.TransferID == channelUpdate.TransferID &&
							existing.OtherPeer == channelUpdate.OtherPeer &&
							existing.IsSender == channelUpdate.IsSender &&
							existing.IsInitiator == channelUpdate.IsInitiator {
							channels[i] = channelUpdate
							found = true
							break
						}
					}
					if !found {
						channels = append(channels, channelUpdate)
					}
				}
			}
		}
		lcli.OutputDataTransferChannels(os.Stdout, toDTv2Channels(channels), verbose, completed, showFailed)
		return nil
	},
}

func toDTv2Channels(channels []bapi.DataTransferChannel) []api.DataTransferChannel {
	v2chs := make([]api.DataTransferChannel, 0, len(channels))
	for _, ch := range channels {
		v2chs = append(v2chs, api.DataTransferChannel{
			TransferID:  datatransferv2.TransferID(ch.TransferID),
			Status:      datatransferv2.Status(ch.Status),
			BaseCID:     ch.BaseCID,
			IsInitiator: ch.IsInitiator,
			IsSender:    ch.IsSender,
			Voucher:     ch.Voucher,
			Message:     ch.Message,
			OtherPeer:   ch.OtherPeer,
			Transferred: ch.Transferred,
			Stages:      toDTv2Stages(ch.Stages),
		})
	}
	return v2chs
}

func toDTv2Stages(stages *datatransfer.ChannelStages) *datatransferv2.ChannelStages {
	if stages == nil {
		return nil
	}

	v2stgs := make([]*datatransferv2.ChannelStage, 0, len(stages.Stages))
	for _, s := range stages.Stages {
		v2stgs = append(v2stgs, &datatransferv2.ChannelStage{
			Name:        s.Name,
			Description: s.Description,
			CreatedTime: s.CreatedTime,
			UpdatedTime: s.UpdatedTime,
			Logs:        toDTv2Logs(s.Logs),
		})
	}
	return &datatransferv2.ChannelStages{Stages: v2stgs}
}

func toDTv2Logs(logs []*datatransfer.Log) []*datatransferv2.Log {
	if logs == nil {
		return nil
	}

	v2logs := make([]*datatransferv2.Log, 0, len(logs))
	for _, l := range logs {
		v2logs = append(v2logs, &datatransferv2.Log{Log: l.Log, UpdatedTime: l.UpdatedTime})
	}

	return v2logs
}

var transfersDiagnosticsCmd = &cli.Command{
	Name:      "diagnostics",
	Usage:     "Get detailed diagnostics on active transfers with a specific peer",
	ArgsUsage: "<peer id>",
	Flags:     []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		if !cctx.Args().Present() {
			return cli.ShowCommandHelp(cctx, cctx.Command.Name)
		}
		api, closer, err := lcli.GetMarketsAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := lcli.ReqContext(cctx)

		targetPeer, err := peer.Decode(cctx.Args().First())
		if err != nil {
			return err
		}
		diagnostics, err := api.MarketDataTransferDiagnostics(ctx, targetPeer)
		if err != nil {
			return err
		}
		out, err := json.MarshalIndent(diagnostics, "", "\t")
		if err != nil {
			return err
		}
		fmt.Println(string(out))
		return nil
	},
}
