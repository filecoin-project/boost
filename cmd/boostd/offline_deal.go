package main

import (
	"fmt"

	bcli "github.com/filecoin-project/boost/cli"
	"github.com/google/uuid"
	"github.com/urfave/cli/v2"
)

var offlineDealCmd = &cli.Command{
	Name:  "offline-deal",
	Usage: "Make offline deal on Boost",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "deal-uuid",
			Usage:    "uuid of the offline deal",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "filepath",
			Usage:    "path of the file containing the offline deal data",
			Required: true,
		},
	},
	Action: func(cctx *cli.Context) error {
		napi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		filePath := cctx.String("filepath")
		id := cctx.String("deal-uuid")

		dealUuid, err := uuid.Parse(id)
		if err != nil {
			return fmt.Errorf("failed to parse deal uuid")
		}
		rej, err := napi.MakeOfflineDealWithData(dealUuid, filePath)
		if err != nil {
			return fmt.Errorf("failed to execute offline deal: %w", err)
		}
		if rej != nil && rej.Reason != "" {
			return fmt.Errorf("offline deal %s rejected: %s", dealUuid, rej.Reason)
		}
		fmt.Println("\n offline deal is being executed")
		return nil
	},
}
