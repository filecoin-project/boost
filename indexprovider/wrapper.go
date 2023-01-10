package indexprovider

import (
	"context"
	"errors"
	"fmt"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"path/filepath"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	lotus_storagemarket "github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/lotus/markets/idxprov"
	lotus_config "github.com/filecoin-project/lotus/node/config"
	"github.com/filecoin-project/lotus/node/repo"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	provider "github.com/ipni/index-provider"
	"github.com/ipni/index-provider/metadata"
	"go.uber.org/fx"
)

var log = logging.Logger("index-provider-wrapper")
var defaultDagStoreDir = "dagstore"

type Wrapper struct {
	marketsHost    host.Host
	cfg            lotus_config.DAGStoreConfig
	enabled        bool
	dealsDB        *db.DealsDB
	legacyProv     lotus_storagemarket.StorageProvider
	prov           provider.Interface
	piecedirectory *piecedirectory.PieceDirectory
	meshCreator    idxprov.MeshCreator
	bitswapEnabled bool
}

func NewWrapper(cfg *config.Boost) func(lc fx.Lifecycle, r repo.LockedRepo, marketsHost host.Host, dealsDB *db.DealsDB,
	legacyProv lotus_storagemarket.StorageProvider, prov provider.Interface,
	piecedirectory *piecedirectory.PieceDirectory, meshCreator idxprov.MeshCreator) *Wrapper {

	return func(lc fx.Lifecycle, r repo.LockedRepo, marketsHost host.Host, dealsDB *db.DealsDB,
		legacyProv lotus_storagemarket.StorageProvider, prov provider.Interface,
		piecedirectory *piecedirectory.PieceDirectory,
		meshCreator idxprov.MeshCreator) *Wrapper {

		if cfg.DAGStore.RootDir == "" {
			cfg.DAGStore.RootDir = filepath.Join(r.Path(), defaultDagStoreDir)
		}

		_, isDisabled := prov.(*DisabledIndexProvider)
		w := &Wrapper{
			marketsHost:    marketsHost,
			dealsDB:        dealsDB,
			legacyProv:     legacyProv,
			prov:           prov,
			meshCreator:    meshCreator,
			cfg:            cfg.DAGStore,
			bitswapEnabled: cfg.Dealmaking.BitswapPeerID != "",
			enabled:        !isDisabled,
			piecedirectory: piecedirectory,
		}
		// announce all deals on startup in case of a config change
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				go func() {
					_ = w.IndexerAnnounceAllDeals(ctx)
				}()
				return nil
			},
		})
		return w
	}
}

func (w *Wrapper) Enabled() bool {
	return w.enabled
}

func (w *Wrapper) IndexerAnnounceAllDeals(ctx context.Context) error {
	if !w.enabled {
		return errors.New("cannot announce all deals: index provider is disabled")
	}

	log.Info("will announce all Markets deals to Indexer")
	err := w.legacyProv.AnnounceAllDealsToIndexer(ctx)
	if err != nil {
		log.Warnw("some errors while announcing legacy deals to index provider", "err", err)
	}
	log.Infof("finished announcing markets deals to indexer")

	log.Info("will announce all Boost deals to Indexer")
	deals, err := w.dealsDB.ListActive(ctx)
	if err != nil {
		return fmt.Errorf("failed to list deals: %w", err)
	}

	shards := make(map[string]struct{})
	var nSuccess int
	var merr error

	for _, d := range deals {
		// filter out deals that will announce automatically at a later
		// point in their execution, as well as deals that are not processing at all
		// (i.e. in an error state or expired)
		// (note technically this is only one check point state IndexedAndAnnounced but is written so
		// it will work if we ever introduce additional states between IndexedAndAnnounced & Complete)
		if d.Checkpoint < dealcheckpoints.IndexedAndAnnounced || d.Checkpoint >= dealcheckpoints.Complete {
			continue
		}

		if _, err := w.AnnounceBoostDeal(ctx, d); err != nil {
			// don't log already advertised errors as errors - just skip them
			if !errors.Is(err, provider.ErrAlreadyAdvertised) {
				merr = multierror.Append(merr, err)
				log.Errorw("failed to announce boost deal to Index provider", "dealId", d.DealUuid, "err", err)
			}
			continue
		}
		shards[d.ClientDealProposal.Proposal.PieceCID.String()] = struct{}{}
		nSuccess++
	}

	log.Infow("finished announcing boost deals to index provider", "number of deals", nSuccess, "number of shards", len(shards))
	return merr
}

func (w *Wrapper) Start(ctx context.Context) {
	w.prov.RegisterMultihashLister(func(ctx context.Context, prov peer.ID, contextID []byte) (provider.MultihashIterator, error) {
		provideF := func(pieceCid cid.Cid) (provider.MultihashIterator, error) {
			ii, err := w.piecedirectory.GetIterableIndex(ctx, pieceCid)
			if err != nil {
				return nil, fmt.Errorf("failed to get iterable index: %w", err)
			}

			mhi, err := provider.CarMultihashIterator(ii)
			if err != nil {
				return nil, fmt.Errorf("failed to get mhiterator: %w", err)
			}
			return mhi, nil
		}

		// convert context ID to proposal Cid
		proposalCid, err := cid.Cast(contextID)
		if err != nil {
			return nil, fmt.Errorf("failed to cast context ID to a cid")
		}

		// go from proposal cid -> piece cid by looking up deal in boost and if we can't find it there -> then markets
		// check Boost deals DB
		pds, boostErr := w.dealsDB.BySignedProposalCID(ctx, proposalCid)
		if boostErr == nil {
			pieceCid := pds.ClientDealProposal.Proposal.PieceCID
			return provideF(pieceCid)
		}

		// check in legacy markets
		md, legacyErr := w.legacyProv.GetLocalDeal(proposalCid)
		if legacyErr == nil {
			return provideF(md.Proposal.PieceCID)
		}

		return nil, fmt.Errorf("failed to look up deal in Boost, err=%s and Legacy Markets, err=%s", boostErr, legacyErr)
	})
}

func (w *Wrapper) AnnounceBoostDeal(ctx context.Context, pds *types.ProviderDealState) (cid.Cid, error) {
	if !w.enabled {
		return cid.Undef, errors.New("cannot announce deal: index provider is disabled")
	}

	// Announce deal to network Indexer
	protocols := []metadata.Protocol{
		&metadata.GraphsyncFilecoinV1{
			PieceCID:      pds.ClientDealProposal.Proposal.PieceCID,
			FastRetrieval: pds.FastRetrieval,
			VerifiedDeal:  pds.ClientDealProposal.Proposal.VerifiedDeal,
		},
	}
	if w.bitswapEnabled {
		protocols = append(protocols, metadata.Bitswap{})
	}

	fm := metadata.Default.New(protocols...)

	// ensure we have a connection with the full node host so that the index provider gossip sub announcements make their
	// way to the filecoin bootstrapper network
	if err := w.meshCreator.Connect(ctx); err != nil {
		log.Errorw("failed to connect boost node to full daemon node", "err", err)
	}

	propCid, err := pds.SignedProposalCid()
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to get proposal cid from deal: %w", err)
	}

	addrInfo := &peer.AddrInfo{
		ID:    w.marketsHost.ID(),
		Addrs: w.marketsHost.Addrs(),
	}
	annCid, err := w.prov.NotifyPut(ctx, addrInfo, propCid.Bytes(), fm)
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to announce deal to index provider: %w", err)
	}
	return annCid, err
}
