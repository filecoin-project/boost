package modules

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/filecoin-project/boost/cmd/booster-bitswap/bitswap"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/protocolproxy"
	"github.com/filecoin-project/boost/retrievalmarket/lp2pimpl"
	"github.com/filecoin-project/boost/retrievalmarket/rtvllog"
	"github.com/filecoin-project/boost/retrievalmarket/types"
	lotus_retrievalmarket "github.com/filecoin-project/go-fil-markets/retrievalmarket"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/node/repo"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
	"go.uber.org/fx"
	"path"
	"time"
)

func NewTransportsListener(cfg *config.Boost) func(h host.Host) (*lp2pimpl.TransportsListener, error) {
	return func(h host.Host) (*lp2pimpl.TransportsListener, error) {
		protos := []types.Protocol{}

		// Get the libp2p addresses from the Host
		if len(h.Addrs()) > 0 {
			protos = append(protos, types.Protocol{
				Name:      "libp2p",
				Addresses: h.Addrs(),
			})
		}

		// If there's an http retrieval address specified, add HTTP to the list
		// of supported protocols
		if cfg.Dealmaking.HTTPRetrievalMultiaddr != "" {
			maddr, err := multiaddr.NewMultiaddr(cfg.Dealmaking.HTTPRetrievalMultiaddr)
			if err != nil {
				msg := "HTTPRetrievalURL must be in multi-address format. "
				msg += "Could not parse '%s' as multiaddr: %w"
				return nil, fmt.Errorf(msg, cfg.Dealmaking.HTTPRetrievalMultiaddr, err)
			}

			protos = append(protos, types.Protocol{
				Name:      "http",
				Addresses: []multiaddr.Multiaddr{maddr},
			})
		}
		if cfg.Dealmaking.BitswapPeerID != "" {
			addrs := h.Addrs()
			if len(cfg.Dealmaking.BitswapPublicAddresses) > 0 {
				addrs = nil
				for _, addrString := range cfg.Dealmaking.BitswapPublicAddresses {
					addr, err := multiaddr.NewMultiaddr(addrString)
					if err != nil {
						return nil, fmt.Errorf("Could not parse bitswap address '%s' as multiaddr: %w", addrString, err)
					}
					addrs = append(addrs, addr)
				}
			}
			protos = append(protos, types.Protocol{
				Name:      "bitswap",
				Addresses: addrs,
			})
		}

		return lp2pimpl.NewTransportsListener(h, protos), nil
	}
}

func HandleRetrievalTransports(lc fx.Lifecycle, l *lp2pimpl.TransportsListener) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			log.Debug("starting retrieval transports listener")
			l.Start()
			return nil
		},
		OnStop: func(context.Context) error {
			log.Debug("stopping retrieval transports listener")
			l.Stop()
			return nil
		},
	})
}

type RetrievalSqlDB struct {
	db *sql.DB
}

func NewRetrievalSqlDB(r repo.LockedRepo) (*RetrievalSqlDB, error) {
	dbPath := path.Join(r.Path(), "boost.retrieval.db?cache=shared")
	d, err := db.SqlDB(dbPath)
	if err != nil {
		return nil, err
	}
	return &RetrievalSqlDB{d}, nil
}

func CreateRetrievalTables(lc fx.Lifecycle, db *RetrievalSqlDB) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return rtvllog.CreateTables(ctx, db.db)
		},
	})
}

func NewRetrievalLogDB(db *RetrievalSqlDB) *rtvllog.RetrievalLogDB {
	return rtvllog.NewRetrievalLogDB(db.db)
}

// Write graphsync retrieval updates to the database
func HandleRetrievalGraphsyncUpdates(duration time.Duration) func(lc fx.Lifecycle, db *rtvllog.RetrievalLogDB, m lotus_retrievalmarket.RetrievalProvider, dt lotus_dtypes.ProviderDataTransfer) {
	return func(lc fx.Lifecycle, db *rtvllog.RetrievalLogDB, m lotus_retrievalmarket.RetrievalProvider, dt lotus_dtypes.ProviderDataTransfer) {
		rel := rtvllog.NewRetrievalLog(db, duration)

		relctx, cancel := context.WithCancel(context.Background())
		type unsubFn func()
		var unsubs []unsubFn
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				unsubs = append(unsubs, unsubFn(m.SubscribeToEvents(rel.OnRetrievalEvent)))
				unsubs = append(unsubs, unsubFn(m.SubscribeToQueryEvents(rel.OnQueryEvent)))
				unsubs = append(unsubs, unsubFn(m.SubscribeToValidationEvents(rel.OnValidationEvent)))
				unsubs = append(unsubs, unsubFn(dt.SubscribeToEvents(rel.OnDataTransferEvent)))
				rel.Start(relctx)
				return nil
			},
			OnStop: func(context.Context) error {
				cancel()
				for _, unsub := range unsubs {
					unsub()
				}
				return nil
			},
		})
	}
}

func NewProtocolProxy(cfg *config.Boost) func(h host.Host) (*protocolproxy.ProtocolProxy, error) {
	return func(h host.Host) (*protocolproxy.ProtocolProxy, error) {
		peerConfig := map[peer.ID][]protocol.ID{}
		// add bitswap if a peer id is set AND the peer is only private
		if cfg.Dealmaking.BitswapPeerID != "" && len(cfg.Dealmaking.BitswapPublicAddresses) == 0 {
			bsPeerID, err := peer.Decode(cfg.Dealmaking.BitswapPeerID)
			if err != nil {
				return nil, err
			}
			peerConfig[bsPeerID] = bitswap.Protocols
		}
		return protocolproxy.NewProtocolProxy(h, peerConfig)
	}
}

func HandleProtocolProxy(lc fx.Lifecycle, pp *protocolproxy.ProtocolProxy) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			log.Info("starting libp2p protocol proxy")
			pp.Start(ctx)
			return nil
		},
		OnStop: func(context.Context) error {
			log.Info("stopping libp2p protocol proxy")
			pp.Close()
			return nil
		},
	})
}
