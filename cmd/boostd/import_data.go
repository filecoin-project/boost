package main

import (
	"fmt"
	"github.com/ipfs/go-cid"
	"strings"

	bcli "github.com/filecoin-project/boost/cli"
	"github.com/google/uuid"
	"github.com/urfave/cli/v2"
)

var importDataCmd = &cli.Command{
	Name:      "import-data",
	Usage:     "Import data for offline deal made with Boost",
	ArgsUsage: "<proposal CID>/<deal UUID> <file>",
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() < 2 {
			return fmt.Errorf("must specify proposal CID / deal UUID and file path")
		}

		id := cctx.Args().Get(0)
		filePath := cctx.Args().Get(1)

		// Parse the first parameter as a deal UUID or a proposal CID
		var proposalCid *cid.Cid
		dealUuid, err := uuid.Parse(id)
		if err != nil {
			propCid, err := cid.Decode(id)
			if err != nil {
				return fmt.Errorf("could not parse '%s' as deal uuid or proposal cid", id)
			}
			proposalCid = &propCid
		}

		napi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		// If the user has supplied a signed proposal cid
		if proposalCid != nil {
			// Look up the deal in the boost database
			deal, err := napi.BoostDealBySignedProposalCid(cctx.Context, *proposalCid)
			if err != nil {
				// If the error is anything other than a Not Found error,
				// return the error
				if !strings.Contains(err.Error(), "not found") {
					return err
				}

				// The deal is not in the boost database, try the legacy
				// markets datastore (v1.1.0 deal)
				err := napi.MarketImportDealData(cctx.Context, *proposalCid, filePath)
				if err != nil {
					return err
				}
				fmt.Printf("Offline deal import for v1.1.0 deal %s scheduled for execution\n", proposalCid.String())
				return nil
			}

			// Get the deal UUID from the deal
			dealUuid = deal.DealUuid
		}

		// Deal proposal by deal uuid (v1.2.0 deal)
		rej, err := napi.BoostOfflineDealWithData(cctx.Context, dealUuid, filePath)
		if err != nil {
			return fmt.Errorf("failed to execute offline deal: %w", err)
		}
		if rej != nil && rej.Reason != "" {
			return fmt.Errorf("offline deal %s rejected: %s", dealUuid, rej.Reason)
		}
		fmt.Printf("Offline deal import for v1.2.0 deal %s scheduled for execution\n", dealUuid)
		return nil
	},
}
