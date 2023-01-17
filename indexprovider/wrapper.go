package indexprovider

import (
	"context"
	"errors"
	"fmt"
	"os"
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
	cfg            lotus_config.DAGStoreConfig
	enabled        bool
	dealsDB        *db.DealsDB
	legacyProv     lotus_storagemarket.StorageProvider
	prov           provider.Interface
	piecedirectory *piecedirectory.PieceDirectory
	meshCreator    idxprov.MeshCreator
	h              host.Host
	// bitswapEnabled records whether to announce bitswap as an available
	// protocol to the network indexer
	bitswapEnabled bool
	// when booster bitswap is exposed on a public address, extendedProvider
	// holds the information needed to announce that multiaddr to the network indexer
	// as the provider of bitswap
	extendedProvider *xproviders.Info
}

func NewWrapper(cfg *config.Boost) func(lc fx.Lifecycle, h host.Host, r repo.LockedRepo, dealsDB *db.DealsDB,
	legacyProv lotus_storagemarket.StorageProvider, prov provider.Interface,
	piecedirectory *piecedirectory.PieceDirectory, meshCreator idxprov.MeshCreator) (*Wrapper, error) {

	return func(lc fx.Lifecycle, h host.Host, r repo.LockedRepo, dealsDB *db.DealsDB,
		legacyProv lotus_storagemarket.StorageProvider, prov provider.Interface,
		piecedirectory *piecedirectory.PieceDirectory,
		meshCreator idxprov.MeshCreator) (*Wrapper, error) {

		if cfg.DAGStore.RootDir == "" {
			cfg.DAGStore.RootDir = filepath.Join(r.Path(), defaultDagStoreDir)
		}

		_, isDisabled := prov.(*DisabledIndexProvider)

		// bitswap is enabled if there is a bitswap peer id
		bitswapEnabled := cfg.Dealmaking.BitswapPeerID != ""

		// setup bitswap extended provider if there is a public multi addr for bitswap
		var ep *xproviders.Info
		if bitswapEnabled && len(cfg.Dealmaking.BitswapPublicAddresses) > 0 {
			// marshal bitswap metadata
			meta := metadata.Default.New(metadata.Bitswap{})
			mbytes, err := meta.MarshalBinary()
			if err != nil {
				return nil, err
			}
			// we need the private key for bitswaps peerID in order to announce publicly
			keyFile, err := os.ReadFile(cfg.Dealmaking.BitswapPrivKeyFile)
			if err != nil {
				return nil, err
			}
			privKey, err := crypto.UnmarshalPrivateKey(keyFile)
			if err != nil {
				return nil, err
			}
			// setup an extended provider record, containing the booster-bitswap multi addr,
			// peer ID, private key for signing, and metadata
			ep = &xproviders.Info{
				ID:       cfg.Dealmaking.BitswapPeerID,
				Addrs:    cfg.Dealmaking.BitswapPublicAddresses,
				Priv:     privKey,
				Metadata: mbytes,
			}
		}

		w := &Wrapper{
			h:                h,
			dealsDB:          dealsDB,
			legacyProv:       legacyProv,
			prov:             prov,
			meshCreator:      meshCreator,
			cfg:              cfg.DAGStore,
			enabled:          !isDisabled,
			piecedirectory:   piecedirectory,
			bitswapEnabled:   bitswapEnabled,
			extendedProvider: ep,
		}
		// announce all deals on startup in case of a config change
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				go func() {
					err := w.AnnounceExtendedProviders(ctx)
					if err != nil {
						log.Warnf("announcing extended providers: %w", err)
					}
				}()
				return nil
			},
		})
		return w, nil
	}
}

func (w *Wrapper) Enabled() bool {
	return w.enabled
}

func (w *Wrapper) AnnounceExtendedProviders(ctx context.Context) error {
	if !w.enabled {
		return errors.New("cannot announce all deals: index provider is disabled")
	}
	// for now, only generate an indexer provider announcement if bitswap announcements
	// are enabled -- all other graphsync announcements are context ID specific
	if !w.bitswapEnabled {
		log.Info("bitswap is not enabled - skipping bitswap announcements to Indexer")
		return nil
	}

	// build the extended providers announcement
	adBuilder := xproviders.NewAdBuilder(w.h.ID(), w.h.Peerstore().PrivKey(w.h.ID()), w.h.Addrs())
	// if we're exposing bitswap publicly, we announce bitswap as an extended provider. If we're not
	// we announce it as metadata on the main provider
	if w.extendedProvider != nil {
		log.Infof("bitswap is enabled and endpoint is public - "+
			"announcing bitswap endpoint to indexer as extended provider: %s %s",
			w.extendedProvider.ID, w.extendedProvider.Addrs)

		adBuilder.WithExtendedProviders(*w.extendedProvider)
	} else {
		log.Infof("bitswap is enabled with boostd as proxy - "+
			"announcing boostd as endpoint for bitswap to indexer: %s %s",
			w.h.ID(), w.h.Addrs())

		meta := metadata.Default.New(metadata.Bitswap{})
		mbytes, err := meta.MarshalBinary()
		if err != nil {
			return err
		}
		adBuilder.WithMetadata(mbytes)
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
	w.prov.RegisterMultihashLister(w.MultihashLister)
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

	annCid, err := w.prov.NotifyPut(ctx, nil, propCid.Bytes(), fm)
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to announce deal to index provider: %w", err)
	}
	return annCid, err
}
