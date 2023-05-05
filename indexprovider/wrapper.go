package indexprovider

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"

	gfm_storagemarket "github.com/filecoin-project/boost-gfm/storagemarket"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/markets/idxprov"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	dst "github.com/filecoin-project/dagstore"
	"github.com/filecoin-project/lotus/markets/dagstore"
	lotus_modules "github.com/filecoin-project/lotus/node/modules"
	"github.com/filecoin-project/lotus/node/repo"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	provider "github.com/ipni/index-provider"
	"github.com/ipni/index-provider/engine/xproviders"
	"github.com/ipni/index-provider/metadata"
	"github.com/libp2p/go-libp2p/core/crypto"
	host "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/fx"
)

var log = logging.Logger("index-provider-wrapper")
var shardRegMarker = ".boost-shard-registration-complete"
var defaultDagStoreDir = "dagstore"

type Wrapper struct {
	cfg         *config.Boost
	enabled     bool
	dealsDB     *db.DealsDB
	legacyProv  gfm_storagemarket.StorageProvider
	prov        provider.Interface
	dagStore    *dagstore.Wrapper
	meshCreator idxprov.MeshCreator
	h           host.Host
	usm         *UnsealedStateManager
	// bitswapEnabled records whether to announce bitswap as an available
	// protocol to the network indexer
	bitswapEnabled bool
	stop           context.CancelFunc
}

func NewWrapper(cfg *config.Boost) func(lc fx.Lifecycle, h host.Host, r repo.LockedRepo, dealsDB *db.DealsDB,
	ssDB *db.SectorStateDB, legacyProv gfm_storagemarket.StorageProvider, prov provider.Interface, dagStore *dagstore.Wrapper,
	meshCreator idxprov.MeshCreator, storageService lotus_modules.MinerStorageService) (*Wrapper, error) {

	return func(lc fx.Lifecycle, h host.Host, r repo.LockedRepo, dealsDB *db.DealsDB,
		ssDB *db.SectorStateDB, legacyProv gfm_storagemarket.StorageProvider, prov provider.Interface, dagStore *dagstore.Wrapper,
		meshCreator idxprov.MeshCreator, storageService lotus_modules.MinerStorageService) (*Wrapper, error) {
		if cfg.DAGStore.RootDir == "" {
			cfg.DAGStore.RootDir = filepath.Join(r.Path(), defaultDagStoreDir)
		}

		_, isDisabled := prov.(*DisabledIndexProvider)

		// bitswap is enabled if there is a bitswap peer id
		bitswapEnabled := cfg.Dealmaking.BitswapPeerID != ""

		// setup bitswap extended provider if there is a public multi addr for bitswap
		w := &Wrapper{
			h:              h,
			dealsDB:        dealsDB,
			legacyProv:     legacyProv,
			prov:           prov,
			dagStore:       dagStore,
			meshCreator:    meshCreator,
			cfg:            cfg,
			bitswapEnabled: bitswapEnabled,
			enabled:        !isDisabled,
		}
		w.usm = NewUnsealedStateManager(w, legacyProv, dealsDB, ssDB, storageService, w.cfg.Storage)
		return w, nil
	}
}

func (w *Wrapper) Stop() {
	w.stop()
}

func (w *Wrapper) Enabled() bool {
	return w.enabled
}

// AnnounceExtendedProviders announces changes to Boost configuration in the context of retrieval
// methods.
//
// The advertisement published by this function covers 3 cases:
//
// 1. bitswap is completely disabled: in which case an advertisement is
// published with empty extended providers that should wipe previous
// support on indexer side.
//
// 2. bitswap is enabled with public addresses: in which case publish an
// advertisement with extended providers records corresponding to the
// public addresses. Note, according the the IPNI spec, the host ID will
// also be added to the extended providers for signing reasons with empty
// metadata making a total of 2 extended provider records.
//
// 3. bitswap with boostd address: in which case public an advertisement
// with one extended provider record that just adds bitswap metadata.
//
// Note that in any case one advertisement is published by boost on startup
// to reflect on bitswap configuration, even if the config remains the
// same. Future work should detect config change and only publish ads when
// config changes.
func (w *Wrapper) AnnounceExtendedProviders(ctx context.Context) error {
	if !w.enabled {
		return errors.New("cannot announce all deals: index provider is disabled")
	}
	// for now, only generate an indexer provider announcement if bitswap announcements
	// are enabled -- all other graphsync announcements are context ID specific

	// build the extended providers announcement
	key := w.h.Peerstore().PrivKey(w.h.ID())
	adBuilder := xproviders.NewAdBuilder(w.h.ID(), key, w.h.Addrs())

	if !w.bitswapEnabled {
		// If bitswap is completely disabled, publish an advertisement with empty extended providers
		// which should override previously published extended providers associated to w.h.ID().
		log.Info("bitswap is not enabled - announcing bitswap disabled to Indexer")
	} else {
		// if we're exposing bitswap publicly, we announce bitswap as an extended provider. If we're not
		// we announce it as metadata on the main provider

		// marshal bitswap metadata
		meta := metadata.Default.New(metadata.Bitswap{})
		mbytes, err := meta.MarshalBinary()
		if err != nil {
			return err
		}
		var ep xproviders.Info
		if len(w.cfg.Dealmaking.BitswapPublicAddresses) > 0 {
			if w.cfg.Dealmaking.BitswapPrivKeyFile == "" {
				return fmt.Errorf("missing required configuration key BitswapPrivKeyFile: " +
					"boost is configured with BitswapPublicAddresses but the BitswapPrivKeyFile configuration key is empty")
			}

			// we need the private key for bitswaps peerID in order to announce publicly
			keyFile, err := os.ReadFile(w.cfg.Dealmaking.BitswapPrivKeyFile)
			if err != nil {
				return fmt.Errorf("opening BitswapPrivKeyFile %s: %w", w.cfg.Dealmaking.BitswapPrivKeyFile, err)
			}
			privKey, err := crypto.UnmarshalPrivateKey(keyFile)
			if err != nil {
				return fmt.Errorf("unmarshalling BitswapPrivKeyFile %s: %w", w.cfg.Dealmaking.BitswapPrivKeyFile, err)
			}
			// setup an extended provider record, containing the booster-bitswap multi addr,
			// peer ID, private key for signing, and metadata
			ep = xproviders.Info{
				ID:       w.cfg.Dealmaking.BitswapPeerID,
				Addrs:    w.cfg.Dealmaking.BitswapPublicAddresses,
				Priv:     privKey,
				Metadata: mbytes,
			}
			log.Infof("bitswap is enabled and endpoint is public - "+
				"announcing bitswap endpoint to indexer as extended provider: %s %s",
				ep.ID, ep.Addrs)
		} else {
			log.Infof("bitswap is enabled with boostd as proxy - "+
				"announcing boostd as endpoint for bitswap to indexer: %s %s",
				w.h.ID(), w.h.Addrs())

			addrs := make([]string, 0, len(w.h.Addrs()))
			for _, addr := range w.h.Addrs() {
				addrs = append(addrs, addr.String())
			}

			ep = xproviders.Info{
				ID:       w.h.ID().String(),
				Addrs:    addrs,
				Priv:     key,
				Metadata: mbytes,
			}
		}
		adBuilder.WithExtendedProviders(ep)
	}

	last, _, err := w.prov.GetLatestAdv(ctx)
	if err != nil {
		return err
	}
	adBuilder.WithLastAdID(last)
	ad, err := adBuilder.BuildAndSign()
	if err != nil {
		return err
	}

	// make sure we're connected to the mesh so that the message will go through
	// pubsub and reach the indexer
	err = w.meshCreator.Connect(ctx)
	if err != nil {
		log.Warnf("could not connect to pubsub mesh before announcing extended provider: %w", err)
	}

	// publish the extended providers announcement
	adCid, err := w.prov.Publish(ctx, *ad)
	if err != nil {
		return err
	}

	log.Infof("announced endpoint to indexer with advertisement cid %s", adCid)

	return nil
}

func (w *Wrapper) IndexerAnnounceAllDeals(ctx context.Context) error {
	if !w.enabled {
		return errors.New("cannot announce all deals: index provider is disabled")
	}

	log.Info("announcing all legacy deals to Indexer")
	err := w.legacyProv.AnnounceAllDealsToIndexer(ctx)
	if err == nil {
		log.Infof("finished announcing all legacy deals to Indexer")
	} else {
		log.Warnw("failed to announce legacy deals to Indexer", "err", err)
	}

	log.Info("announcing all Boost deals to Indexer")
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
				log.Errorw("failed to announce boost deal to Indexer", "dealId", d.DealUuid, "err", err)
			}
			continue
		}
		shards[d.ClientDealProposal.Proposal.PieceCID.String()] = struct{}{}
		nSuccess++
	}

	log.Infow("finished announcing all boost deals to Indexer", "number of deals", nSuccess, "number of shards", len(shards))
	return merr
}

func (w *Wrapper) Start(ctx context.Context) {
	// re-init dagstore shards for Boost deals if needed
	if _, err := w.DagstoreReinitBoostDeals(ctx); err != nil {
		log.Errorw("failed to migrate dagstore indices for Boost deals", "err", err)
	}

	w.prov.RegisterMultihashLister(func(ctx context.Context, pid peer.ID, contextID []byte) (provider.MultihashIterator, error) {
		provideF := func(pieceCid cid.Cid) (provider.MultihashIterator, error) {
			ii, err := w.dagStore.GetIterableIndexForPiece(pieceCid)
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

	runCtx, runCancel := context.WithCancel(context.Background())
	w.stop = runCancel

	// Watch for changes in sector unseal state and update the
	// indexer when there are changes
	go w.usm.Run(runCtx)

	// Announce all deals on startup in case of a config change
	go func() {
		err := w.AnnounceExtendedProviders(runCtx)
		if err != nil {
			log.Warnf("announcing extended providers: %w", err)
		}
	}()
}

func (w *Wrapper) AnnounceBoostDeal(ctx context.Context, deal *types.ProviderDealState) (cid.Cid, error) {
	// Filter out deals that should not be announced
	if !deal.AnnounceToIPNI {
		return cid.Undef, nil
	}

	propCid, err := deal.SignedProposalCid()
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to get proposal cid from deal: %w", err)
	}
	md := metadata.GraphsyncFilecoinV1{
		PieceCID:      deal.ClientDealProposal.Proposal.PieceCID,
		FastRetrieval: deal.FastRetrieval,
		VerifiedDeal:  deal.ClientDealProposal.Proposal.VerifiedDeal,
	}
	return w.announceBoostDealMetadata(ctx, md, propCid)
}

func (w *Wrapper) announceBoostDealMetadata(ctx context.Context, md metadata.GraphsyncFilecoinV1, propCid cid.Cid) (cid.Cid, error) {
	if !w.enabled {
		return cid.Undef, errors.New("cannot announce deal: index provider is disabled")
	}

	// Ensure we have a connection with the full node host so that the index provider gossip sub announcements make their
	// way to the filecoin bootstrapper network
	if err := w.meshCreator.Connect(ctx); err != nil {
		log.Errorw("failed to connect boost node to full daemon node", "err", err)
	}

	// Announce deal to network Indexer
	fm := metadata.Default.New(&md)
	annCid, err := w.prov.NotifyPut(ctx, nil, propCid.Bytes(), fm)
	if err != nil {
		// Check if the error is because the deal was already advertised
		// (we can safely ignore this error)
		if !errors.Is(err, provider.ErrAlreadyAdvertised) {
			return cid.Undef, fmt.Errorf("failed to announce deal to index provider: %w", err)
		}
	}
	return annCid, nil
}

func (w *Wrapper) AnnounceBoostDealRemoved(ctx context.Context, propCid cid.Cid) (cid.Cid, error) {
	if !w.enabled {
		return cid.Undef, errors.New("cannot announce deal removal: index provider is disabled")
	}

	// Ensure we have a connection with the full node host so that the index provider gossip sub announcements make their
	// way to the filecoin bootstrapper network
	if err := w.meshCreator.Connect(ctx); err != nil {
		log.Errorw("failed to connect boost node to full daemon node", "err", err)
	}

	// Announce deal removal to network Indexer
	annCid, err := w.prov.NotifyRemove(ctx, "", propCid.Bytes())
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to announce deal removal to index provider: %w", err)
	}
	return annCid, err
}

func (w *Wrapper) DagstoreReinitBoostDeals(ctx context.Context) (bool, error) {
	deals, err := w.dealsDB.ListActive(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to list active Boost deals: %w", err)
	}

	log := log.Named("boost-migrator")
	log.Infof("dagstore root is %s", w.cfg.DAGStore.RootDir)

	// Check if all deals have already been registered as shards
	isComplete, err := w.boostRegistrationComplete()
	if err != nil {
		return false, fmt.Errorf("failed to get boost dagstore migration status: %w", err)
	}
	if isComplete {
		// All deals have been registered as shards, bail out
		log.Info("no boost shard migration necessary; already marked complete")
		return false, nil
	}

	log.Infow("registering shards for all active boost deals in sealing subsystem", "count", len(deals))

	// channel where results will be received, and channel where the total
	// number of registered shards will be sent.
	resch := make(chan dst.ShardResult, 32)
	totalCh := make(chan int)
	doneCh := make(chan struct{})

	// Start making progress consuming results. We won't know how many to
	// actually consume until we register all shards.
	//
	// If there are any problems registering shards, just log an error
	go func() {
		defer close(doneCh)

		var total = math.MaxInt64
		var res dst.ShardResult
		for rcvd := 0; rcvd < total; {
			select {
			case total = <-totalCh:
				// we now know the total number of registered shards
				// nullify so that we no longer consume from it after closed.
				close(totalCh)
				totalCh = nil
			case res = <-resch:
				rcvd++
				if res.Error == nil {
					log.Infow("async boost shard registration completed successfully", "shard_key", res.Key)
				} else {
					log.Warnw("async boost shard registration failed", "shard_key", res.Key, "error", res.Error)
				}
			}
		}
	}()

	var registered int
	for _, deal := range deals {
		pieceCid := deal.ClientDealProposal.Proposal.PieceCID

		// enrich log statements in this iteration with deal ID and piece CID.
		log := log.With("deal_id", deal.ChainDealID, "piece_cid", pieceCid)

		// Filter out deals that have not yet been indexed and announced as they will be re-indexed anyways
		if deal.Checkpoint < dealcheckpoints.IndexedAndAnnounced {
			continue
		}

		log.Infow("registering boost deal in dagstore with lazy init")

		// Register the deal as a shard with the DAG store with lazy initialization.
		// The index will be populated the first time the deal is retrieved, or
		// through the bulk initialization script.
		err = w.dagStore.RegisterShard(ctx, pieceCid, "", false, resch)
		if err != nil {
			log.Warnw("failed to register boost shard", "error", err)
			continue
		}
		registered++
	}

	log.Infow("finished registering all boost shards", "total", registered)
	totalCh <- registered
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-doneCh:
	}

	log.Infow("confirmed registration of all boost shards")

	// Completed registering all shards, so mark the migration as complete
	err = w.markBoostRegistrationComplete()
	if err != nil {
		log.Errorf("failed to mark boost shards as registered: %s", err)
	} else {
		log.Info("successfully marked boost migration as complete")
	}

	log.Infow("boost dagstore migration complete")

	return true, nil
}

// Check for the existence of a "marker" file indicating that the migration
// has completed
func (w *Wrapper) boostRegistrationComplete() (bool, error) {
	path := filepath.Join(w.cfg.DAGStore.RootDir, shardRegMarker)
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// Create a "marker" file indicating that the migration has completed
func (w *Wrapper) markBoostRegistrationComplete() error {
	path := filepath.Join(w.cfg.DAGStore.RootDir, shardRegMarker)
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	return file.Close()
}
