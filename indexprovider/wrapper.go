package indexprovider

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"go.uber.org/fx"

	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"

	"github.com/filecoin-project/boost-gfm/storagemarket"
	"github.com/filecoin-project/boost/db"
	bdtypes "github.com/filecoin-project/boost/extern/boostd-data/svc/types"
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
	"github.com/ipni/go-libipni/metadata"
	provider "github.com/ipni/index-provider"
	"github.com/ipni/index-provider/engine"
	"github.com/ipni/index-provider/engine/xproviders"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

var log = logging.Logger("index-provider-wrapper")
var defaultDagStoreDir = "dagstore"

type Wrapper struct {
	enabled bool

	cfg            *config.Boost
	dealsDB        *db.DealsDB
	directDealsDB  *db.DirectDealsDB
	legacyProv     storagemarket.StorageProvider
	prov           provider.Interface
	piecedirectory *piecedirectory.PieceDirectory
	ssm            *sectorstatemgr.SectorStateMgr
	meshCreator    idxprov.MeshCreator
	h              host.Host
	// bitswapEnabled records whether to announce bitswap as an available
	// protocol to the network indexer
	bitswapEnabled bool
	httpEnabled    bool
	stop           context.CancelFunc
}

func NewWrapper(cfg *config.Boost) func(lc fx.Lifecycle, h host.Host, r repo.LockedRepo, dealsDB *db.DealsDB,
	ssDB *db.SectorStateDB, legacyProv storagemarket.StorageProvider, prov provider.Interface,
	piecedirectory *piecedirectory.PieceDirectory, ssm *sectorstatemgr.SectorStateMgr, meshCreator idxprov.MeshCreator, storageService lotus_modules.MinerStorageService) (*Wrapper, error) {

	return func(lc fx.Lifecycle, h host.Host, r repo.LockedRepo, dealsDB *db.DealsDB,
		ssDB *db.SectorStateDB, legacyProv storagemarket.StorageProvider, prov provider.Interface,
		piecedirectory *piecedirectory.PieceDirectory,
		ssm *sectorstatemgr.SectorStateMgr,
		meshCreator idxprov.MeshCreator, storageService lotus_modules.MinerStorageService) (*Wrapper, error) {

		if cfg.DAGStore.RootDir == "" {
			cfg.DAGStore.RootDir = filepath.Join(r.Path(), defaultDagStoreDir)
		}

		_, isDisabled := prov.(*DisabledIndexProvider)

		// bitswap is enabled if there is a bitswap peer id
		bitswapEnabled := cfg.Dealmaking.BitswapPeerID != ""
		// http is considered enabled if there is an http retrieval multiaddr set
		httpEnabled := cfg.Dealmaking.HTTPRetrievalMultiaddr != ""

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
			httpEnabled:    httpEnabled,
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
			log.Warnf("announcing extended providers: %s", err)
		}
	}()

	log.Info("starting index provider")

	go w.checkForUpdates(runCtx)
}

func (w *Wrapper) checkForUpdates(ctx context.Context) {
	updates := w.ssm.PubSub.Subscribe()

	for {
		select {
		case u, ok := <-updates:
			if !ok {
				log.Debugw("state updates subscription closed")
				return
			}
			log.Debugw("got state updates from SectorStateMgr", "u", len(u.Updates))

			err := w.handleUpdates(ctx, u.Updates)
			if err != nil {
				log.Errorw("error while handling state updates", "err", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (w *Wrapper) handleUpdates(ctx context.Context, sectorUpdates map[abi.SectorID]db.SealState) error {
	legacyDeals, err := w.legacyDealsBySectorID(sectorUpdates)
	if err != nil {
		return fmt.Errorf("getting legacy deals from datastore: %w", err)
	}

	log.Debugf("checking for sector state updates for %d states", len(sectorUpdates))

	for sectorID, sectorSealState := range sectorUpdates {
		// for all updated sectors, get all deals (legacy and boost) in the sector
		deals, err := w.dealsBySectorID(ctx, legacyDeals, sectorID)
		if err != nil {
			return fmt.Errorf("getting deals for miner %d / sector %d: %w", sectorID.Miner, sectorID.Number, err)
		}
		log.Debugf("sector %d has %d deals, seal status %s", sectorID, len(deals), sectorSealState)

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
				// announce deals that are no longer unsealed as removed to indexer
				announceCid, err := w.AnnounceBoostDealRemoved(ctx, propCid)
				if err != nil {
					// check if the error is because the deal wasn't previously announced
					if !errors.Is(err, provider.ErrContextIDNotFound) {
						log.Errorw("announcing deal removed to index provider",
							"deal id", deal.DealID, "error", err)
						continue
					}
				} else {
					log.Infow("announced to index provider that deal has been removed",
						"deal id", deal.DealID, "sector id", deal.SectorID.Number, "announce cid", announceCid.String())
				}
			} else if sectorSealState != db.SealStateCache {
				// announce deals that have changed seal state to indexer
				md := metadata.GraphsyncFilecoinV1{
					PieceCID:      deal.DealProposal.Proposal.PieceCID,
					FastRetrieval: sectorSealState == db.SealStateUnsealed,
					VerifiedDeal:  deal.DealProposal.Proposal.VerifiedDeal,
				}
				announceCid, err := w.AnnounceBoostDealMetadata(ctx, md, propCid.Bytes())
				if err != nil {
					log.Errorf("announcing deal %s to index provider: %w", deal.DealID, err)
				} else {
					log.Infow("announced deal seal state to index provider",
						"deal id", deal.DealID, "sector id", deal.SectorID.Number,
						"seal state", sectorSealState, "announce cid", announceCid.String())
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
// The advertisement published by this function covers 2 protocols:
//
// Bitswap:
//
//  1. bitswap is completely disabled: in which case an advertisement is
//     published with http(or empty if http is disabled) extended providers
//     that should wipe previous support on indexer side.
//
//  2. bitswap is enabled with public addresses: in which case publish an
//     advertisement with extended providers records corresponding to the
//     public addresses. Note, according the IPNI spec, the host ID will
//     also be added to the extended providers for signing reasons with empty
//     metadata making a total of 2 extended provider records.
//
//  3. bitswap with boostd address: in which case public an advertisement
//     with one extended provider record that just adds bitswap metadata.
//
// HTTP:
//
//  1. http is completely disabled: in which case an advertisement is
//     published with bitswap(or empty if bitswap is disabled) extended providers
//     that should wipe previous support on indexer side
//
//  2. http is enabled: in which case an advertisement is published with
//     bitswap and http(or only http if bitswap is disabled) extended providers
//     that should wipe previous support on indexer side
//
//     Note that in any case one advertisement is published by boost on startup
//     to reflect on extended provider configuration, even if the config remains the
//     same. Future work should detect config change and only publish ads when
//     config changes.
func (w *Wrapper) AnnounceExtendedProviders(ctx context.Context) error {
	if !w.enabled {
		return errors.New("cannot announce all deals: index provider is disabled")
	}
	// for now, only generate an indexer provider announcement if bitswap announcements
	// are enabled -- all other graphsync announcements are context ID specific

	// build the extended providers announcement
	key := w.h.Peerstore().PrivKey(w.h.ID())
	adBuilder := xproviders.NewAdBuilder(w.h.ID(), key, w.h.Addrs())

	err := w.appendExtendedProviders(ctx, adBuilder, key)
	if err != nil {
		return err
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

func (w *Wrapper) appendExtendedProviders(ctx context.Context, adBuilder *xproviders.AdBuilder, key crypto.PrivKey) error {

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

	if !w.httpEnabled {
		log.Info("Dealmaking.HTTPRetrievalMultiaddr is not set - announcing http disabled to Indexer")
	} else {
		// marshal http metadata
		meta := metadata.Default.New(metadata.IpfsGatewayHttp{})
		mbytes, err := meta.MarshalBinary()
		if err != nil {
			return err
		}
		var ep = xproviders.Info{
			ID:       w.h.ID().String(),
			Addrs:    []string{w.cfg.Dealmaking.HTTPRetrievalMultiaddr},
			Metadata: mbytes,
			Priv:     key,
		}

		log.Infof("announcing http endpoint to indexer as extended provider: %s", ep.Addrs)

		adBuilder.WithExtendedProviders(ep)
	}

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

// ErrStringSkipAdIngest - While ingesting cids for each piece, if there is an error the indexer
// checks if the error contains the string "content not found":
// - if so, the indexer skips the piece and continues ingestion
// - if not, the indexer pauses ingestion
var ErrStringSkipAdIngest = "content not found"

func skipError(err error) error {
	return fmt.Errorf("%s: %s: %w", ErrStringSkipAdIngest, err.Error(), ipld.ErrNotExists{})
}

func (w *Wrapper) IndexerAnnounceLatest(ctx context.Context) (cid.Cid, error) {
	e, ok := w.prov.(*engine.Engine)
	if !ok {
		return cid.Undef, fmt.Errorf("index provider is disabled")
	}
	return e.PublishLatest(ctx)
}

func (w *Wrapper) IndexerAnnounceLatestHttp(ctx context.Context, announceUrls []string) (cid.Cid, error) {
	e, ok := w.prov.(*engine.Engine)
	if !ok {
		return cid.Undef, fmt.Errorf("index provider is disabled")
	}

	if len(announceUrls) == 0 {
		announceUrls = w.cfg.IndexProvider.Announce.DirectAnnounceURLs
	}

	urls := make([]*url.URL, 0, len(announceUrls))
	for _, us := range announceUrls {
		u, err := url.Parse(us)
		if err != nil {
			return cid.Undef, fmt.Errorf("parsing url %s: %w", us, err)
		}
		urls = append(urls, u)
	}
	return e.PublishLatestHTTP(ctx, urls...)
}

func (w *Wrapper) MultihashLister(ctx context.Context, prov peer.ID, contextID []byte) (provider.MultihashIterator, error) {
	provideF := func(identifier string, isDD bool, pieceCid cid.Cid) (provider.MultihashIterator, error) {
		idName := "propCid"
		if isDD {
			idName = "UUID"
		}
		llog := log.With(idName, identifier, "piece", pieceCid)
		ii, err := w.piecedirectory.GetIterableIndex(ctx, pieceCid)
		if err != nil {
			e := fmt.Errorf("failed to get iterable index: %w", err)
			if bdtypes.IsNotFound(err) {
				// If it's a not found error, skip over this piece and continue ingesting
				llog.Infow("skipping ingestion: piece not found", "err", e)
				return nil, skipError(e)
			}

			// Some other error, pause ingestion
			llog.Infow("pausing ingestion: error getting piece", "err", e)
			return nil, e
		}

		// Check if there are any records in the iterator.
		hasRecords := ii.ForEach(func(_ multihash.Multihash, _ uint64) error {
			return fmt.Errorf("has at least one record")
		})
		if hasRecords == nil {
			// If there are no records, it's effectively the same as a not
			// found error. Skip over this piece and continue ingesting.
			e := fmt.Errorf("no records found for piece %s", pieceCid)
			llog.Infow("skipping ingestion: piece has no records", "err", e)
			return nil, skipError(e)
		}

		mhi, err := provider.CarMultihashIterator(ii)
		if err != nil {
			// Bad index, skip over this piece and continue ingesting
			err = fmt.Errorf("failed to get mhiterator: %w", err)
			llog.Infow("skipping ingestion", "err", err)
			return nil, skipError(err)
		}

		llog.Debugw("returning piece iterator", "err", err)
		return mhi, nil
	}

	// Try to cast the context to a proposal CID for Boost deals and legacy deals
	proposalCid, err := cid.Cast(contextID)
	if err == nil {
		// Look up deal by proposal cid in the boost database.
		// If we can't find it there check legacy markets DB.
		pds, boostErr := w.dealsDB.BySignedProposalCID(ctx, proposalCid)
		if boostErr == nil {
			// Found the deal, get an iterator over the piece
			pieceCid := pds.ClientDealProposal.Proposal.PieceCID
			return provideF(proposalCid.String(), false, pieceCid)
		}

		// Check if it's a "not found" error
		if !errors.Is(boostErr, sql.ErrNoRows) {
			// It's not a "not found" error: there was a problem accessing the
			// database. Pause ingestion until the user can fix the DB.
			e := fmt.Errorf("getting deal with proposal cid %s from boost database: %w", proposalCid, boostErr)
			log.Infow("pausing ingestion", "proposalCid", proposalCid, "err", e)
			return nil, e
		}

		// Deal was not found in boost DB - check in legacy markets
		md, legacyErr := w.legacyProv.GetLocalDeal(proposalCid)
		if legacyErr == nil {
			// Found the deal, get an interator over the piece
			return provideF(proposalCid.String(), false, md.Proposal.PieceCID)
		}

		// Check if it's a "not found" error
		if !errors.Is(legacyErr, datastore.ErrNotFound) {
			// It's not a "not found" error: there was a problem accessing the
			// legacy database. Pause ingestion until the user can fix the legacy DB.
			e := fmt.Errorf("getting deal with proposal cid %s from Legacy Markets: %w", proposalCid, legacyErr)
			log.Infow("pausing ingestion", "proposalCid", proposalCid, "err", e)
			return nil, e
		}

		// The deal was not found in the boost or legacy database.
		// Skip this deal and continue ingestion.
		err = fmt.Errorf("deal with proposal cid %s not found", proposalCid)
		log.Infow("skipping ingestion", "proposalCid", proposalCid, "err", err)
		return nil, skipError(err)
	}

	dealUUID, err := uuid.FromBytes(contextID)
	if err == nil {
		// Look up deal by dealUUID in the direct deals database
		entry, dderr := w.directDealsDB.ByID(ctx, dealUUID)
		if dderr == nil {
			// Found the deal, get an iterator over the piece
			return provideF(dealUUID.String(), true, entry.PieceCID)
		}

		// Check if it's a "not found" error
		if !errors.Is(dderr, sql.ErrNoRows) {
			// It's not a "not found" error: there was a problem accessing the
			// database. Pause ingestion until the user can fix the DB.
			e := fmt.Errorf("getting deal with UUID %s from direct deal database: %w", dealUUID, dderr)
			log.Infow("pausing ingestion", "deal UUID", dealUUID, "err", e)
			return nil, e
		}

		// The deal was not found in the boost, legacy or direct deal database.
		// Skip this deal and continue ingestion.
		err = fmt.Errorf("deal with UUID %s not found", dealUUID)
		log.Infow("skipping ingestion", "deal UUID", dealUUID, "err", err)
		return nil, skipError(err)
	}

	// Bad contextID or UUID skip over this piece and continue ingesting
	err = fmt.Errorf("failed to cast context ID to a cid and UUID")
	log.Infow("skipping ingestion", "context ID", string(contextID), "err", err)
	return nil, skipError(err)
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
	return w.AnnounceBoostDealMetadata(ctx, md, propCid.Bytes())
}

func (w *Wrapper) AnnounceBoostDealMetadata(ctx context.Context, md metadata.GraphsyncFilecoinV1, contextID []byte) (cid.Cid, error) {
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
	annCid, err := w.prov.NotifyPut(ctx, nil, contextID, fm)
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

func (w *Wrapper) AnnounceBoostDirectDeal(ctx context.Context, entry *types.DirectDeal) (cid.Cid, error) {
	// Filter out deals that should not be announced
	if !entry.AnnounceToIPNI {
		return cid.Undef, nil
	}

	contextID, err := entry.ID.MarshalBinary()
	if err != nil {
		return cid.Undef, fmt.Errorf("marshalling the deal UUID: %w", err)
	}

	md := metadata.GraphsyncFilecoinV1{
		PieceCID:      entry.PieceCID,
		FastRetrieval: entry.KeepUnsealedCopy,
		VerifiedDeal:  true,
	}
	return w.AnnounceBoostDealMetadata(ctx, md, contextID)
}

func (w *Wrapper) AnnounceBoostDirectDealRemoved(ctx context.Context, dealUUID uuid.UUID) (cid.Cid, error) {
	if !w.enabled {
		return cid.Undef, errors.New("cannot announce deal removal: index provider is disabled")
	}

	// Ensure we have a connection with the full node host so that the index provider gossip sub announcements make their
	// way to the filecoin bootstrapper network
	if err := w.meshCreator.Connect(ctx); err != nil {
		log.Errorw("failed to connect boost node to full daemon node", "err", err)
	}

	contextID, err := dealUUID.MarshalBinary()
	if err != nil {
		return cid.Undef, fmt.Errorf("marshalling the deal UUID: %w", err)
	}

	// Announce deal removal to network Indexer
	annCid, err := w.prov.NotifyRemove(ctx, "", contextID)
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to announce deal removal to index provider: %w", err)
	}
	return annCid, err
}
