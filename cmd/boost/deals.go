package main

import (
	"fmt"

	bcli "github.com/filecoin-project/boost/cli"
	"github.com/google/uuid"
	"github.com/urfave/cli/v2"
)

var dealCmd = &cli.Command{
	Name:  "deal",
	Usage: "Manage Boost deals",
	Subcommands: []*cli.Command{
		makeOfflineDealWithData,
	},
}

var makeOfflineDealWithData = &cli.Command{
	Name:  "offline-deal-with-data",
	Usage: "Make offline deal with data",
	Flags: []cli.Flag{&cli.StringFlag{
		Name:  "dealUuid",
		Usage: "uuid of the offline deal",
	},

		&cli.StringFlag{
			Name:  "filepath",
			Usage: "path of the file containing the offline deal data",
		},
	},

	Action: func(cctx *cli.Context) error {
		if cctx.NArg() != 2 {
			return fmt.Errorf("must specify deal uuid and deal data file path")
		}

		napi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		filePath := cctx.String("filepath")
		id := cctx.String("dealUuid")

		dealUuid, err := uuid.Parse(id)
		if err != nil {
			return fmt.Errorf("failed to parse deal uuid")
		}
		rej, err := napi.MakeOfflineDealWithData(dealUuid, filePath)
		if err != nil {
			return fmt.Errorf("failed to execute offline deal: %w", err)
		}
		if rej != nil && rej.Reason != "" {
			return fmt.Errorf("offline deal %s rejected: %s\n", dealUuid, rej.Reason)
		}
		fmt.Println("\n offline deal is being executed")
		return nil
	},
}
