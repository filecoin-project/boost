package api

import (
	"context"
	"time"

	lotus_api "github.com/filecoin-project/lotus/api"
	lotus_net "github.com/filecoin-project/lotus/node/impl/net"
	metrics "github.com/libp2p/go-libp2p/core/metrics"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

//                       MODIFYING THE API INTERFACE
//
// When adding / changing methods in this file:
// * Do the change here
// * Adjust implementation in `node/impl/`
// * Run `make gen` - this will:
//  * Generate proxy structs
//  * Generate mocks
//  * Generate markdown docs
//  * Generate openrpc blobs

// This interface is a direct copy of the lotus equivalent.
// We can't just include the lotus equivalent directly because of the way that
// the generator works (it doesn't pull in types that are outside this directory)
type Net interface {
	// MethodGroup: Net
	NetConnectedness(context.Context, peer.ID) (network.Connectedness, error)  //perm:read
	NetPeers(context.Context) ([]peer.AddrInfo, error)                         //perm:read
	NetPing(context.Context, peer.ID) (time.Duration, error)                   //perm:read
	NetConnect(context.Context, peer.AddrInfo) error                           //perm:write
	NetAddrsListen(context.Context) (peer.AddrInfo, error)                     //perm:read
	NetDisconnect(context.Context, peer.ID) error                              //perm:write
	NetFindPeer(context.Context, peer.ID) (peer.AddrInfo, error)               //perm:read
	NetPubsubScores(context.Context) ([]lotus_api.PubsubScore, error)          //perm:read
	NetAutoNatStatus(context.Context) (lotus_api.NatInfo, error)               //perm:read
	NetAgentVersion(ctx context.Context, p peer.ID) (string, error)            //perm:read
	NetPeerInfo(context.Context, peer.ID) (*lotus_api.ExtendedPeerInfo, error) //perm:read

	// NetBandwidthStats returns statistics about the nodes total bandwidth
	// usage and current rate across all peers and protocols.
	NetBandwidthStats(ctx context.Context) (metrics.Stats, error) //perm:read

	// NetBandwidthStatsByPeer returns statistics about the nodes bandwidth
	// usage and current rate per peer
	NetBandwidthStatsByPeer(ctx context.Context) (map[string]metrics.Stats, error) //perm:read

	// NetBandwidthStatsByProtocol returns statistics about the nodes bandwidth
	// usage and current rate per protocol
	NetBandwidthStatsByProtocol(ctx context.Context) (map[protocol.ID]metrics.Stats, error) //perm:read

	// ConnectionGater API
	NetBlockAdd(ctx context.Context, acl lotus_api.NetBlockList) error    //perm:admin
	NetBlockRemove(ctx context.Context, acl lotus_api.NetBlockList) error //perm:admin
	NetBlockList(ctx context.Context) (lotus_api.NetBlockList, error)     //perm:read
	NetProtectAdd(ctx context.Context, acl []peer.ID) error               //perm:admin
	NetProtectRemove(ctx context.Context, acl []peer.ID) error            //perm:admin
	NetProtectList(ctx context.Context) ([]peer.ID, error)                //perm:read

	// ID returns peerID of libp2p node backing this API
	ID(context.Context) (peer.ID, error) //perm:read

	// ResourceManager API
	NetStat(ctx context.Context, scope string) (lotus_api.NetStat, error)          //perm:read
	NetLimit(ctx context.Context, scope string) (lotus_api.NetLimit, error)        //perm:read
	NetSetLimit(ctx context.Context, scope string, limit lotus_api.NetLimit) error //perm:admin
}

type CommonNet interface {
	Common
	Net
}

var _ Net = (*lotus_net.NetAPI)(nil)
