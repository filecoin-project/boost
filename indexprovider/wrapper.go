package indexprovider

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/filecoin-project/boost-gfm/storagemarket"
	gfm_storagemarket "github.com/filecoin-project/boost-gfm/storagemarket"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/markets/idxprov"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/boost/sectorstatemgr"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-state-types/abi"
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
	"github.com/multiformats/go-multihash"
	"go.uber.org/fx"
)

var log = logging.Logger("index-provider-wrapper")
var defaultDagStoreDir = "dagstore"

type Wrapper struct {
	enabled bool

	cfg            *config.Boost
	dealsDB        *db.DealsDB
	legacyProv     gfm_storagemarket.StorageProvider
	prov           provider.Interface
	piecedirectory *piecedirectory.PieceDirectory
	ssm            *sectorstatemgr.SectorStateMgr
	meshCreator    idxprov.MeshCreator
	h              host.Host
	// bitswapEnabled records whether to announce bitswap as an available
	// protocol to the network indexer
	bitswapEnabled bool
	stop           context.CancelFunc
}

func NewWrapper(cfg *config.Boost) func(lc fx.Lifecycle, h host.Host, r repo.LockedRepo, dealsDB *db.DealsDB,
	ssDB *db.SectorStateDB, legacyProv gfm_storagemarket.StorageProvider, prov provider.Interface,
	piecedirectory *piecedirectory.PieceDirectory, ssm *sectorstatemgr.SectorStateMgr, meshCreator idxprov.MeshCreator, storageService lotus_modules.MinerStorageService) (*Wrapper, error) {

	return func(lc fx.Lifecycle, h host.Host, r repo.LockedRepo, dealsDB *db.DealsDB,
		ssDB *db.SectorStateDB, legacyProv gfm_storagemarket.StorageProvider, prov provider.Interface,
		piecedirectory *piecedirectory.PieceDirectory,
		ssm *sectorstatemgr.SectorStateMgr,
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
			meshCreator:    meshCreator,
			cfg:            cfg,
			enabled:        !isDisabled,
			piecedirectory: piecedirectory,
			bitswapEnabled: bitswapEnabled,
			ssm:            ssm,
		}
		return w, nil
	}
}

func (w *Wrapper) Start(_ context.Context) {
	w.prov.RegisterMultihashLister(w.MultihashLister)

	runCtx, runCancel := context.WithCancel(context.Background())
	w.stop = runCancel

	// Announce all deals on startup in case of a config change
	go func() {
		err := w.AnnounceExtendedProviders(runCtx)
		if err != nil {
			log.Warnf("announcing extended providers: %w", err)
		}
	}()

	log.Info("starting index provider")

	go func() {
		updates := w.ssm.PubSub.Subscribe()

		for {
			select {
			case u, ok := <-updates:
				if !ok {
					log.Debugw("state updates subscription closed")
					return
				}
				log.Debugw("got state updates from SectorStateMgr", "u", len(u.Updates))

				err := w.handleUpdates(runCtx, u.Updates)
				if err != nil {
					log.Errorw("error while handling state updates", "err", err)
				}
			case <-runCtx.Done():
				return
			}
		}
	}()
}

func (w *Wrapper) handleUpdates(ctx context.Context, sus map[abi.SectorID]db.SealState) error {
	legacyDeals, err := w.legacyDealsBySectorID(sus)
	if err != nil {
		return fmt.Errorf("getting legacy deals from datastore: %w", err)
	}

	log.Debugf("checking for sector state updates for %d states", len(sus))

	// For each sector
	for sectorID, sectorSealState := range sus {
		// Get the deals in the sector
		deals, err := w.dealsBySectorID(ctx, legacyDeals, sectorID)
		if err != nil {
			return fmt.Errorf("getting deals for miner %d / sector %d: %w", sectorID.Miner, sectorID.Number, err)
		}
		log.Debugf("sector %d has %d deals, seal status %s", sectorID, len(deals), sectorSealState)

		// For each deal in the sector
		for _, deal := range deals {
			if !deal.AnnounceToIPNI {
				continue
			}

			propnd, err := cborutil.AsIpld(&deal.DealProposal)
			if err != nil {
				return fmt.Errorf("failed to compute signed deal proposal ipld node: %w", err)
			}
			propCid := propnd.Cid()

			if sectorSealState == db.SealStateRemoved {
				// Announce deals that are no longer unsealed to indexer
				announceCid, err := w.AnnounceBoostDealRemoved(ctx, propCid)
				if err != nil {
					// Check if the error is because the deal wasn't previously announced
					if !errors.Is(err, provider.ErrContextIDNotFound) {
						// There was some other error, write it to the log
						log.Errorw("announcing deal removed to index provider",
							"deal id", deal.DealID, "error", err)
						continue
					}
				} else {
					log.Infow("announced to index provider that deal has been removed",
						"deal id", deal.DealID, "sector id", deal.SectorID.Number, "announce cid", announceCid.String())
				}
			} else if sectorSealState != db.SealStateCache {
				// Announce deals that have changed seal state to indexer
				md := metadata.GraphsyncFilecoinV1{
					PieceCID:      deal.DealProposal.Proposal.PieceCID,
					FastRetrieval: sectorSealState == db.SealStateUnsealed,
					VerifiedDeal:  deal.DealProposal.Proposal.VerifiedDeal,
				}
				announceCid, err := w.AnnounceBoostDealMetadata(ctx, md, propCid)
				if err == nil {
					log.Infow("announced deal seal state to index provider",
						"deal id", deal.DealID, "sector id", deal.SectorID.Number,
						"seal state", sectorSealState, "announce cid", announceCid.String())
				} else {
					log.Errorf("announcing deal %s to index provider: %w", deal.DealID, err)
				}
			}
		}
	}

	return nil
}

// Get deals by sector ID, whether they're legacy or boost deals
func (w *Wrapper) dealsBySectorID(ctx context.Context, legacyDeals map[abi.SectorID][]storagemarket.MinerDeal, sectorID abi.SectorID) ([]basicDealInfo, error) {
	// First query the boost database
	deals, err := w.dealsDB.BySectorID(ctx, sectorID)
	if err != nil {
		return nil, fmt.Errorf("getting deals from boost database: %w", err)
	}

	basicDeals := make([]basicDealInfo, 0, len(deals))
	for _, dl := range deals {
		basicDeals = append(basicDeals, basicDealInfo{
			AnnounceToIPNI: dl.AnnounceToIPNI,
			DealID:         dl.DealUuid.String(),
			SectorID:       sectorID,
			DealProposal:   dl.ClientDealProposal,
		})
	}

	// Then check the legacy deals
	legDeals, ok := legacyDeals[sectorID]
	if ok {
		for _, dl := range legDeals {
			basicDeals = append(basicDeals, basicDealInfo{
				AnnounceToIPNI: true,
				DealID:         dl.ProposalCid.String(),
				SectorID:       sectorID,
				DealProposal:   dl.ClientDealProposal,
			})
		}
	}

	return basicDeals, nil
}

// Iterate over all legacy deals and make a map of sector ID -> legacy deal.
// To save memory, only include legacy deals with a sector ID that we know
// we're going to query, ie the set of sector IDs in the stateUpdates map.
func (w *Wrapper) legacyDealsBySectorID(stateUpdates map[abi.SectorID]db.SealState) (map[abi.SectorID][]storagemarket.MinerDeal, error) {
	legacyDeals, err := w.legacyProv.ListLocalDeals()
	if err != nil {
		return nil, err
	}

	bySectorID := make(map[abi.SectorID][]storagemarket.MinerDeal, len(legacyDeals))
	for _, deal := range legacyDeals {
		minerID, err := address.IDFromAddress(deal.Proposal.Provider)
		if err != nil {
			// just skip the deal if we can't convert its address to an ID address
			continue
		}
		sectorID := abi.SectorID{
			Miner:  abi.ActorID(minerID),
			Number: deal.SectorNumber,
		}
		_, ok := stateUpdates[sectorID]
		if ok {
			bySectorID[sectorID] = append(bySectorID[sectorID], deal)
		}
	}

	return bySectorID, nil
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

func (w *Wrapper) MultihashLister(ctx context.Context, prov peer.ID, contextID []byte) (provider.MultihashIterator, error) {
	provideF := func(pieceCid cid.Cid) (provider.MultihashIterator, error) {
		ii, err := w.piecedirectory.GetIterableIndex(ctx, pieceCid)
		if err != nil {
			return nil, fmt.Errorf("failed to get iterable index: %w", err)
		}

		// Check if there are any records in the iterator. If there are no
		// records, the multihash lister expects us to return an error.
		hasRecords := ii.ForEach(func(_ multihash.Multihash, _ uint64) error {
			return fmt.Errorf("has at least one record")
		})
		if hasRecords == nil {
			return nil, fmt.Errorf("no records found for piece %s", pieceCid)
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
	return w.AnnounceBoostDealMetadata(ctx, md, propCid)
}

func (w *Wrapper) AnnounceBoostDealMetadata(ctx context.Context, md metadata.GraphsyncFilecoinV1, propCid cid.Cid) (cid.Cid, error) {
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

type basicDealInfo struct {
	AnnounceToIPNI bool
	DealID         string
	SectorID       abi.SectorID
	DealProposal   storagemarket.ClientDealProposal
}
