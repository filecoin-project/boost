package modules

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/boost-gfm/retrievalmarket"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/boost/storagemarket/dealfilter"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/build"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
)

func BasicDealFilter(userCmd dtypes.StorageDealFilter) func(onlineOk dtypes.ConsiderOnlineStorageDealsConfigFunc,
	offlineOk dtypes.ConsiderOfflineStorageDealsConfigFunc,
	verifiedOk dtypes.ConsiderVerifiedStorageDealsConfigFunc,
	unverifiedOk dtypes.ConsiderUnverifiedStorageDealsConfigFunc,
	blocklistFunc dtypes.StorageDealPieceCidBlocklistConfigFunc,
	expectedSealTimeFunc dtypes.GetExpectedSealDurationFunc,
	startDelay dtypes.GetMaxDealStartDelayFunc,
	fullNodeApi v1api.FullNode,
	r lotus_repo.LockedRepo,
) dtypes.StorageDealFilter {
	return func(onlineOk dtypes.ConsiderOnlineStorageDealsConfigFunc,
		offlineOk dtypes.ConsiderOfflineStorageDealsConfigFunc,
		verifiedOk dtypes.ConsiderVerifiedStorageDealsConfigFunc,
		unverifiedOk dtypes.ConsiderUnverifiedStorageDealsConfigFunc,
		blocklistFunc dtypes.StorageDealPieceCidBlocklistConfigFunc,
		expectedSealTimeFunc dtypes.GetExpectedSealDurationFunc,
		startDelay dtypes.GetMaxDealStartDelayFunc,
		fullNodeApi v1api.FullNode,
		r lotus_repo.LockedRepo,
	) dtypes.StorageDealFilter {
		return func(ctx context.Context, params dealfilter.DealFilterParams) (bool, string, error) {
			deal := params.DealParams
			pr := deal.ClientDealProposal.Proposal

			// TODO: maybe handle in userCmd?
			b, err := onlineOk()
			if err != nil {
				return false, "miner error", err
			}

			if !deal.IsOffline && !b {
				log.Warnf("online storage deal consideration disabled; rejecting storage deal proposal from client: %s", deal.ClientDealProposal.Proposal.Client.String())
				return false, "miner is not considering online storage deals", nil
			}

			// TODO: maybe handle in userCmd?
			b, err = offlineOk()
			if err != nil {
				return false, "miner error", err
			}

			if deal.IsOffline && !b {
				log.Warnf("offline storage deal consideration disabled; rejecting storage deal proposal from client: %s", deal.ClientDealProposal.Proposal.Client.String())
				return false, "miner is not accepting offline storage deals", nil
			}

			// TODO: maybe handle in userCmd?
			b, err = verifiedOk()
			if err != nil {
				return false, "miner error", err
			}

			if pr.VerifiedDeal && !b {
				log.Warnf("verified storage deal consideration disabled; rejecting storage deal proposal from client: %s", pr.Client.String())
				return false, "miner is not accepting verified storage deals", nil
			}

			// TODO: maybe handle in userCmd?
			b, err = unverifiedOk()
			if err != nil {
				return false, "miner error", err
			}

			if !pr.VerifiedDeal && !b {
				log.Warnf("unverified storage deal consideration disabled; rejecting storage deal proposal from client: %s", pr.Client.String())
				return false, "miner is not accepting unverified storage deals", nil
			}

			// TODO: maybe handle in userCmd?
			blocklist, err := blocklistFunc()
			if err != nil {
				return false, "miner error", err
			}

			for idx := range blocklist {
				if deal.ClientDealProposal.Proposal.PieceCID.Equals(blocklist[idx]) {
					log.Warnf("piece CID in proposal %s is blocklisted; rejecting storage deal proposal from client: %s", pr.PieceCID, pr.Client.String())
					return false, fmt.Sprintf("miner has blocklisted piece CID %s", pr.PieceCID), nil
				}
			}

			// Reject if start epoch is less than the minimum time to be taken to seal the deal
			sealDuration, err := expectedSealTimeFunc()
			if err != nil {
				return false, "miner error", err
			}

			sealEpochs := sealDuration / (time.Duration(build.BlockDelaySecs) * time.Second)
			ts, err := fullNodeApi.ChainHead(ctx)
			if err != nil {
				return false, "failed to get chain head", err
			}
			ht := ts.Height()
			earliest := abi.ChainEpoch(sealEpochs) + ht
			if deal.ClientDealProposal.Proposal.StartEpoch < earliest {
				log.Warnw("proposed deal would start before sealing can be completed; rejecting storage deal proposal from client", "piece_cid", deal.ClientDealProposal.Proposal.PieceCID, "client", deal.ClientDealProposal.Proposal.Client.String(), "seal_duration", sealDuration, "earliest", earliest, "curepoch", ht)
				return false, fmt.Sprintf("cannot seal a sector before %s", deal.ClientDealProposal.Proposal.StartEpoch), nil
			}

			// Reject if the start epoch is too far in the future and is online
			// We probably do not want to reject offline deal based on this criteria
			if !deal.IsOffline {
				sd, err := startDelay()
				if err != nil {
					return false, "miner error", err
				}

				maxStartEpoch := earliest + abi.ChainEpoch(uint64(sd.Seconds())/build.BlockDelaySecs)
				if deal.ClientDealProposal.Proposal.StartEpoch > maxStartEpoch {
					return false, fmt.Sprintf("deal start epoch is too far in the future: %s > %s", deal.ClientDealProposal.Proposal.StartEpoch, maxStartEpoch), nil
				}
			}

			if userCmd != nil {
				return userCmd(ctx, params)
			}

			return true, "", nil
		}
	}
}

func RetrievalDealFilter(userFilter dtypes.RetrievalDealFilter) func(onlineOk dtypes.ConsiderOnlineRetrievalDealsConfigFunc,
	offlineOk dtypes.ConsiderOfflineRetrievalDealsConfigFunc) dtypes.RetrievalDealFilter {
	return func(onlineOk dtypes.ConsiderOnlineRetrievalDealsConfigFunc,
		offlineOk dtypes.ConsiderOfflineRetrievalDealsConfigFunc) dtypes.RetrievalDealFilter {
		return func(ctx context.Context, state retrievalmarket.ProviderDealState) (bool, string, error) {
			b, err := onlineOk()
			if err != nil {
				return false, "miner error", err
			}

			if !b {
				log.Warn("online retrieval deal consideration disabled; rejecting retrieval deal proposal from client")
				return false, "miner is not accepting online retrieval deals", nil
			}

			b, err = offlineOk()
			if err != nil {
				return false, "miner error", err
			}

			if !b {
				log.Info("offline retrieval has not been implemented yet")
			}

			if userFilter != nil {
				return userFilter(ctx, state)
			}

			return true, "", nil
		}
	}
}
