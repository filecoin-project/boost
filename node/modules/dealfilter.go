package modules

import (
	"context"
	"fmt"

	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/boost/storagemarket/types"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
)

func BasicDealFilter(cfg config.DealmakingConfig, userCmd dtypes.StorageDealFilter) func(onlineOk dtypes.ConsiderOnlineStorageDealsConfigFunc,
	offlineOk dtypes.ConsiderOfflineStorageDealsConfigFunc,
	verifiedOk dtypes.ConsiderVerifiedStorageDealsConfigFunc,
	unverifiedOk dtypes.ConsiderUnverifiedStorageDealsConfigFunc,
	blocklistFunc dtypes.StorageDealPieceCidBlocklistConfigFunc,
	expectedSealTimeFunc dtypes.GetExpectedSealDurationFunc,
	startDelay dtypes.GetMaxDealStartDelayFunc,
	r lotus_repo.LockedRepo,
) dtypes.StorageDealFilter {
	return func(onlineOk dtypes.ConsiderOnlineStorageDealsConfigFunc,
		offlineOk dtypes.ConsiderOfflineStorageDealsConfigFunc,
		verifiedOk dtypes.ConsiderVerifiedStorageDealsConfigFunc,
		unverifiedOk dtypes.ConsiderUnverifiedStorageDealsConfigFunc,
		blocklistFunc dtypes.StorageDealPieceCidBlocklistConfigFunc,
		expectedSealTimeFunc dtypes.GetExpectedSealDurationFunc,
		startDelay dtypes.GetMaxDealStartDelayFunc,
		r lotus_repo.LockedRepo,
	) dtypes.StorageDealFilter {
		return func(ctx context.Context, deal types.DealParams) (bool, string, error) {
			pr := deal.ClientDealProposal.Proposal

			// TODO: maybe handle in userCmd?
			b, err := onlineOk()
			if err != nil {
				return false, "miner error", err
			}

			if deal.Transfer.Type != "manual" && !b {
				log.Warnf("online storage deal consideration disabled; rejecting storage deal proposal from client: %s", deal.ClientDealProposal.Proposal.Client.String())
				return false, "miner is not considering online storage deals", nil
			}

			// TODO: maybe handle in userCmd?
			b, err = offlineOk()
			if err != nil {
				return false, "miner error", err
			}

			if deal.Transfer.Type == "manual" && !b {
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

			if userCmd != nil {
				return userCmd(ctx, deal)
			}

			return true, "", nil
		}
	}
}
