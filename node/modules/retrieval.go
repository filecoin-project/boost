package modules

import (
	"context"
	"database/sql"
	"fmt"
	"path"
	"time"

	"github.com/filecoin-project/boost/cmd/booster-bitswap/bitswap"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/protocolproxy"
	"github.com/filecoin-project/boost/retrievalmarket/lp2pimpl"
	"github.com/filecoin-project/boost/retrievalmarket/rtvllog"
	"github.com/filecoin-project/boost/retrievalmarket/server"
	"github.com/filecoin-project/boost/retrievalmarket/types"
	"github.com/filecoin-project/lotus/node/repo"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
	"go.uber.org/fx"
)

// bitswapMultiAddrs is a utility function to determine the multi addrs for bitswap
// based off the host and boost config
func bitswapMultiAddrs(cfg *config.Boost, h host.Host) ([]multiaddr.Multiaddr, error) {
	// if BitswapPublicAddresses is empty, that means we'll be serving bitswap directly from this host, so just return host multiaddresses
	if len(cfg.Retrievals.Bitswap.BitswapPublicAddresses) == 0 {
		maddr, err := peer.AddrInfoToP2pAddrs(&peer.AddrInfo{
			ID:    h.ID(),
			Addrs: h.Addrs(),
		})
		if err != nil {
			return nil, fmt.Errorf("could not parse bitswap addresses: %w", err)
		}
		return maddr, nil
	}

	// otherwise bitswap protocols are exposed publicly directly from booster bitswap.
	// in this case, we want our multiaddrs to point to public booster-bitswap

	// parse all of the public multiaddrs
	var addrs []multiaddr.Multiaddr
	for _, addrString := range cfg.Retrievals.Bitswap.BitswapPublicAddresses {
		addr, err := multiaddr.NewMultiaddr(addrString)
		if err != nil {
			return nil, fmt.Errorf("could not parse bitswap address '%s' as multiaddr: %w", addrString, err)
		}
		addrs = append(addrs, addr)
	}

	// in order to make these multiaddrs fully dialable, we encapsulate the bitswap peer id inside of them
	bsPeerID, err := peer.Decode(cfg.Retrievals.Bitswap.BitswapPeerID)
	if err != nil {
		return nil, fmt.Errorf("could not parse bitswap peer id '%s': %w", cfg.Retrievals.Bitswap.BitswapPeerID, err)
	}
	return peer.AddrInfoToP2pAddrs(&peer.AddrInfo{
		ID:    bsPeerID,
		Addrs: addrs,
	})
}

func NewTransportsListener(cfg *config.Boost) func(h host.Host) (*lp2pimpl.TransportsListener, error) {
	return func(h host.Host) (*lp2pimpl.TransportsListener, error) {
		protos := []types.Protocol{}

		// Get the libp2p addresses from the Host
		if len(h.Addrs()) > 0 {
			maddr, err := peer.AddrInfoToP2pAddrs(&peer.AddrInfo{
				ID:    h.ID(),
				Addrs: h.Addrs(),
			})
			if err != nil {
				return nil, fmt.Errorf("could not parse libp2p addresses: %w", err)
			}
			protos = append(protos, types.Protocol{
				Name:      "libp2p",
				Addresses: maddr,
			})
		}

		// If there's an http retrieval address specified, add HTTP to the list
		// of supported protocols
		if cfg.Retrievals.HTTP.HTTPRetrievalMultiaddr != "" {
			maddr, err := multiaddr.NewMultiaddr(cfg.Retrievals.HTTP.HTTPRetrievalMultiaddr)
			if err != nil {
				msg := "HTTPRetrievalURL must be in multi-address format. "
				msg += "Could not parse '%s' as multiaddr: %w"
				return nil, fmt.Errorf(msg, cfg.Retrievals.HTTP.HTTPRetrievalMultiaddr, err)
			}
			protos = append(protos, types.Protocol{
				Name:      "http",
				Addresses: []multiaddr.Multiaddr{maddr},
			})
		}

		// If there's a bitswap peer address specified, add bitswap to the list
		// of supported protocols
		if cfg.Retrievals.Bitswap.BitswapPeerID != "" {
			addrs, err := bitswapMultiAddrs(cfg, h)
			if err != nil {
				return nil, err
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
func HandleRetrievalGraphsyncUpdates(duration time.Duration, stalledDuration time.Duration) func(lc fx.Lifecycle, db *rtvllog.RetrievalLogDB, gsur *server.GraphsyncUnpaidRetrieval) {
	return func(lc fx.Lifecycle, db *rtvllog.RetrievalLogDB, gsur *server.GraphsyncUnpaidRetrieval) {
		rel := rtvllog.NewRetrievalLog(db, duration, stalledDuration, gsur)

		relctx, cancel := context.WithCancel(context.Background())
		type unsubFn func()
		var unsubs []unsubFn
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				unsubs = append(unsubs, unsubFn(gsur.SubscribeToDataTransferEvents(rel.OnDataTransferEvent)))
				unsubs = append(unsubs, unsubFn(gsur.SubscribeToMarketsEvents(rel.OnRetrievalEvent)))
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
		if cfg.Retrievals.Bitswap.BitswapPeerID != "" && len(cfg.Retrievals.Bitswap.BitswapPublicAddresses) == 0 {
			bsPeerID, err := peer.Decode(cfg.Retrievals.Bitswap.BitswapPeerID)
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
