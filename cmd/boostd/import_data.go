package main

import (
	"fmt"

	bcli "github.com/filecoin-project/boost/cli"
	"github.com/google/uuid"
	"github.com/urfave/cli/v2"
)

var importDataCmd = &cli.Command{
	Name:  "import-data",
	Usage: "Import data for offline deal made with Boost",
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
			return fmt.Errorf("failed to parse deal uuid '%s'", id)
		}
		rej, err := napi.BoostOfflineDealWithData(cctx.Context, dealUuid, filePath)
		if err != nil {
			return fmt.Errorf("failed to execute offline deal: %w", err)
		}
		if rej != nil && rej.Reason != "" {
			return fmt.Errorf("offline deal %s rejected: %s", dealUuid, rej.Reason)
		}
		fmt.Println("Offline deal accepted and scheduled for execution")
		return nil
	},
}
