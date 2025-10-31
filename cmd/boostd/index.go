package main

import (
	"fmt"
	"strings"

	bcli "github.com/filecoin-project/boost/cli"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/urfave/cli/v2"
)

var indexProvCmd = &cli.Command{
	Name:  "index",
	Usage: "Manage the index provider on Boost",
	Subcommands: []*cli.Command{
		indexProvAnnounceAllCmd,
		indexProvListMultihashesCmd,
		indexProvAnnounceLatest,
		indexProvAnnounceLatestHttp,
		indexProvAnnounceDealRemovalAd,
		indexProvAnnounceDeal,
		indexProvRemoveAllCmd,
	},
}

var indexProvAnnounceAllCmd = &cli.Command{
	Name:  "announce-all",
	Usage: "Announce all active deals to indexers so they can download the indices",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)

		// get boost api
		napi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		// announce markets and boost deals
		return napi.BoostIndexerAnnounceAllDeals(ctx)
	},
}

var indexProvListMultihashesCmd = &cli.Command{
	Name:      "list-multihashes",
	Usage:     "list-multihashes <proposal cid / deal UUID>",
	UsageText: "List multihashes for a deal by proposal cid or deal UUID",
	Action: func(cctx *cli.Context) error {
		if cctx.NArg() != 1 {
			return fmt.Errorf("must supply a proposal cid or deal UUID")
		}

		ctx := lcli.ReqContext(cctx)

		// get boost api
		napi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		if cctx.Args().Len() != 1 {
			return fmt.Errorf("must specify only one proposal CID / deal UUID")
		}

		id := cctx.Args().Get(0)

		var proposalCid cid.Cid
		var mhs []multihash.Multihash
		dealUuid, err := uuid.Parse(id)
		if err != nil {
			propCid, err := cid.Decode(id)
			if err != nil {
				return fmt.Errorf("could not parse '%s' as deal uuid or proposal cid", id)
			}
			proposalCid = propCid
		}

		if !proposalCid.Defined() {
			contextID, err := dealUuid.MarshalBinary()
			if err != nil {
				return fmt.Errorf("parsing UUID to bytes: %w", err)
			}
			mhs, err = napi.BoostIndexerListMultihashes(ctx, contextID)
			if err != nil {
				return err
			}
			fmt.Printf("Found %d multihashes for deal with ID %s:\n", len(mhs), id)
			for _, mh := range mhs {
				fmt.Println("  " + mh.String())
			}

			return nil
		}

		mhs, err = napi.BoostIndexerListMultihashes(ctx, proposalCid.Bytes())
		if err != nil {
			return err
		}

		fmt.Printf("Found %d multihashes for deal with ID %s:\n", len(mhs), id)
		for _, mh := range mhs {
			fmt.Println("  " + mh.String())
		}

		return nil
	},
}

var indexProvAnnounceLatest = &cli.Command{
	Name:  "announce-latest",
	Usage: "Re-publish the latest existing advertisement to pubsub",
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)

		napi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		c, err := napi.BoostIndexerAnnounceLatest(ctx)
		if err != nil {
			return err
		}

		fmt.Printf("Announced advertisement with cid %s\n", c)
		return nil
	},
}

var indexProvAnnounceLatestHttp = &cli.Command{
	Name:  "announce-latest-http",
	Usage: "Re-publish the latest existing advertisement to specific indexers over http",
	Flags: []cli.Flag{
		&cli.StringSliceFlag{
			Name:     "announce-url",
			Usage:    "The url(s) to announce to. If not specified, announces to the http urls in config",
			Required: false,
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)

		napi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		c, err := napi.BoostIndexerAnnounceLatestHttp(ctx, cctx.StringSlice("announce-url"))
		if err != nil {
			return err
		}

		fmt.Printf("Announced advertisement to indexers over http with cid %s\n", c)
		return nil
	},
}

var indexProvAnnounceDealRemovalAd = &cli.Command{
	Name:  "announce-remove-deal",
	Usage: "Published a removal ad for given deal UUID or Signed Proposal CID (legacy deals)",
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)

		napi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		if cctx.Args().Len() != 1 {
			return fmt.Errorf("must specify only one proposal CID / deal UUID")
		}

		id := cctx.Args().Get(0)

		var proposalCid cid.Cid
		dealUuid, err := uuid.Parse(id)
		if err != nil {
			propCid, err := cid.Decode(id)
			if err != nil {
				return fmt.Errorf("could not parse '%s' as deal uuid or proposal cid", id)
			}
			proposalCid = propCid
		}

		if !proposalCid.Defined() {
			deal, err := napi.BoostDeal(ctx, dealUuid)
			if err != nil {
				return fmt.Errorf("deal not found with UUID %s: %w", dealUuid.String(), err)
			}
			prop, err := deal.SignedProposalCid()
			if err != nil {
				return fmt.Errorf("generating proposal cid for deal %s: %w", dealUuid.String(), err)
			}
			proposalCid = prop
		} else {
			_, err = napi.BoostLegacyDealByProposalCid(ctx, proposalCid)
			if err != nil {
				_, err := napi.BoostDealBySignedProposalCid(ctx, proposalCid)
				if err != nil {
					return fmt.Errorf("no deal with proposal CID %s found in boost and legacy database", proposalCid.String())
				}
			}
		}

		fmt.Printf("the proposal cid is %s\n", proposalCid)

		cid, err := napi.BoostIndexerAnnounceDealRemoved(ctx, proposalCid)
		if err != nil {
			return fmt.Errorf("failed to send removal ad: %w", err)
		}
		fmt.Printf("Announced the removal Ad with cid %s\n", cid)

		return nil
	},
}

var indexProvAnnounceDeal = &cli.Command{
	Name:  "announce-deal",
	Usage: "Publish an ad for for given deal UUID or Signed Proposal CID (legacy deals)",
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)

		napi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		if cctx.Args().Len() != 1 {
			return fmt.Errorf("must specify only one deal UUID")
		}

		id := cctx.Args().Get(0)

		var proposalCid cid.Cid
		dealUuid, err := uuid.Parse(id)
		if err != nil {
			propCid, err := cid.Decode(id)
			if err != nil {
				return fmt.Errorf("could not parse '%s' as deal uuid or proposal cid", id)
			}
			proposalCid = propCid
		}

		if !proposalCid.Defined() {
			deal, err := napi.BoostDeal(ctx, dealUuid)
			if err != nil {
				return fmt.Errorf("deal not found with UUID %s: %w", dealUuid.String(), err)
			}
			ad, err := napi.BoostIndexerAnnounceDeal(ctx, deal)
			if err != nil {
				return fmt.Errorf("failed to announce the deal: %w", err)
			}
			if ad.Defined() {
				fmt.Printf("Announced the deal with Ad cid %s\n", ad)
				return nil
			}
			fmt.Printf("Deal already announced\n")
			return nil
		}

		_, err = napi.BoostLegacyDealByProposalCid(ctx, proposalCid)
		if err != nil {
			// If the error is anything other than a Not Found error,
			// return the error
			if !strings.Contains(err.Error(), "not found") {
				return fmt.Errorf("locating legacy deal %s: %w", proposalCid, err)
			}
			// If not found, check for Boost deals by proposal CID
			deal, err := napi.BoostDealBySignedProposalCid(ctx, proposalCid)
			if err != nil {
				return fmt.Errorf("locating Boost or legacy deal by proposal CID %s: %w", proposalCid, err)
			}
			// Announce Boost deal
			ad, err := napi.BoostIndexerAnnounceDeal(ctx, deal)
			if err != nil {
				return fmt.Errorf("failed to announce the deal: %w", err)
			}
			if ad.Defined() {
				fmt.Printf("Announced the deal with Ad cid %s\n", ad)
				return nil
			}
			fmt.Printf("Deal already announced\n")
			return nil
		}
		// Announce legacy deal
		ad, err := napi.BoostIndexerAnnounceLegacyDeal(ctx, proposalCid)
		if err != nil {
			return fmt.Errorf("announcing legacy deal with proposal CID %s: %w", proposalCid, err)
		}
		if ad.Defined() {
			fmt.Printf("Announced the legacy deal with Ad cid %s\n", ad)
			return nil
		}
		fmt.Printf("Legacy deal already announced\n")
		return nil
	},
}

var indexProvRemoveAllCmd = &cli.Command{
	Name:  "remove-all",
	Usage: "Announce all removal ad for all contextIDs",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)

		// get boost api
		napi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		// announce markets and boost deals
		cids, err := napi.BoostIndexerRemoveAll(ctx)
		if err != nil {
			return err
		}
		for _, c := range cids {
			fmt.Println("Published the removal ad with CID", c.String())
		}
		return nil
	},
}
