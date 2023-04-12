package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	bcli "github.com/filecoin-project/boost/cli"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
)

var importDataCmd = &cli.Command{
	Name:      "import-data",
	Usage:     "Import data for offline deal made with Boost",
	ArgsUsage: "<proposal CID> <file> or <deal UUID> <file>",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "delete-after-import",
			Usage: "whether to delete the data for the offline deal after the deal has been added to a sector",
			Value: false,
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() < 2 {
			return fmt.Errorf("must specify proposal CID / deal UUID and file path")
		}

		id := cctx.Args().Get(0)
		tpath := cctx.Args().Get(1)

		path, err := homedir.Expand(tpath)
		if err != nil {
			return fmt.Errorf("expanding file path: %w", err)
		}

		filePath, err := filepath.Abs(path)
		if err != nil {
			return fmt.Errorf("failed get absolute path for file: %w", err)
		}

		_, err = os.Stat(filePath)
		if err != nil {
			return fmt.Errorf("opening file %s: %w", filePath, err)
		}

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
		deleteAfterImport := cctx.Bool("delete-after-import")
		if proposalCid != nil {
			if deleteAfterImport {
				return fmt.Errorf("legacy deal data cannot be automatically deleted after import (only new deals)")
			}

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
					return fmt.Errorf("couldnt import v1.1.0 deal, or find boost deal: %w", err)
				}

				fmt.Printf("Offline deal import for v1.1.0 deal %s scheduled for execution\n", proposalCid.String())
				return nil
			}

			// Get the deal UUID from the deal
			dealUuid = deal.DealUuid
		}

		// Deal proposal by deal uuid (v1.2.0 deal)
		rej, err := napi.BoostOfflineDealWithData(cctx.Context, dealUuid, filePath, deleteAfterImport)
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
