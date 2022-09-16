package loadbalancer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/filecoin-project/boost/loadbalancer/messages"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/multiformats/go-multiaddr"
)

const DefaultDuration = time.Hour

var log = logging.Logger("loadbalancer")

type LoadBalancer struct {
	ctx            context.Context
	h              host.Host
	peerConfig     map[peer.ID][]protocol.ID
	activeRoutesLk sync.RWMutex
	activeRoutes   map[protocol.ID]peer.ID
}

func NewLoadBalancer(h host.Host, peerConfig map[peer.ID][]protocol.ID) (*LoadBalancer, error) {
	err := validatePeerConfig(peerConfig)
	if err != nil {
		return nil, err
	}
	return &LoadBalancer{
		h:            h,
		activeRoutes: make(map[protocol.ID]peer.ID),
		peerConfig:   peerConfig,
	}, nil
}

func validatePeerConfig(peerConfig map[peer.ID][]protocol.ID) error {
	// for now, double check no peers overlap in config
	// TODO: support multiple peers owning a config
	routesSet := map[protocol.ID]struct{}{}
	for _, protocols := range peerConfig {
		for _, protocol := range protocols {
			_, existing := routesSet[protocol]
			if existing {
				return errors.New("Route registered for multiple peers")
			}
		}
	}
	return nil
}

func (lb *LoadBalancer) Start(ctx context.Context) {
	lb.ctx = ctx
	lb.h.SetStreamHandler(ForwardingProtocolID, lb.handleForwarding)
	lb.h.Network().Notify(lb)
}

func (lb *LoadBalancer) Close() error {
	lb.h.RemoveStreamHandler(ForwardingProtocolID)

	lb.activeRoutesLk.Lock()
	for id := range lb.activeRoutes {
		lb.h.RemoveStreamHandler(id)
	}
	lb.activeRoutes = map[protocol.ID]peer.ID{}
	lb.activeRoutesLk.Unlock()
	lb.h.Network().StopNotify(lb)
	return nil
}

// UpdatePeerConfig updates the load balancer with a new peer config
// existing nodes will have to disconnect and reconnect
func (lb *LoadBalancer) UpdatePeerConfig(peerConfig map[peer.ID][]protocol.ID) error {
	err := validatePeerConfig(peerConfig)
	if err != nil {
		return err
	}
	err = lb.Close()
	if err != nil {
		return err
	}
	lb.peerConfig = peerConfig
	lb.Start(lb.ctx)
	return nil
}

// Listen satifies the network.Notifee interface but does nothing
func (lb *LoadBalancer) Listen(network.Network, multiaddr.Multiaddr) {} // called when network starts listening on an addr

// ListenClose satifies the network.Notifee interface but does nothing
func (lb *LoadBalancer) ListenClose(network.Network, multiaddr.Multiaddr) {} // called when network stops listening on an addr

// Connected checks the peersConfig and begins listening any time a service node connects
func (lb *LoadBalancer) Connected(n network.Network, c network.Conn) {
	// read the peer that just connected
	p := c.RemotePeer()

	// check if they are in the peer config
	protocols, isServiceNode := lb.peerConfig[p]

	if !isServiceNode {
		return
	}

	log.Infow("service node connected activating peer protocols", "peerID", p, "protocols", protocols)

	// if they are in the peer config, listen on all protocols they are setup for
	lb.activeRoutesLk.Lock()
	defer lb.activeRoutesLk.Unlock()
	for _, id := range protocols {
		// check if we already registered this protocol
		if _, ok := lb.activeRoutes[id]; ok {
			continue
		}
		lb.activeRoutes[id] = p
		lb.h.SetStreamHandler(id, lb.handleIncoming)
	}
}

// Disconnected checks the peersConfig and removes listening when a service node disconnects
func (lb *LoadBalancer) Disconnected(n network.Network, c network.Conn) { // called when a connection closed
	// read the peer that just connected
	p := c.RemotePeer()

	// check if they are in the peer config
	protocols, isServiceNode := lb.peerConfig[p]

	if !isServiceNode {
		return
	}

	log.Infow("service node disconnected deactivating peer protocols", "peerID", p, "protocols", protocols)

	// if they are in the peer config, 'un'-listen on all protocols they are setup for
	lb.activeRoutesLk.Lock()
	defer lb.activeRoutesLk.Unlock()
	for _, id := range protocols {
		// check if we already de-registered this protocol
		if _, ok := lb.activeRoutes[id]; !ok {
			continue
		}
		delete(lb.activeRoutes, id)
		lb.h.RemoveStreamHandler(id)
	}
}

// handle a request from a routed peer to make an external ougoing connection
func (lb *LoadBalancer) handleForwarding(s network.Stream) {
	defer s.Close()
	p := s.Conn().RemotePeer()
	request, err := messages.ReadForwardingRequest(s)
	if err != nil {
		log.Warnf("reading forwarding request: %s", err)
		_ = s.Reset()
		return
	}

	// only accept outbound requests
	if request.Kind != messages.ForwardingOutbound {
		err = messages.WriteForwardingResponseError(s, ErrNoInboundRequests)
		if err != nil {
			log.Warnf("writing forwarding response: %s", err)
		}
		return
	}

	log.Debugw("outgoing forwarding stream", "protocols", request.Protocols, "routed peer", p, "remote peer", request.Remote)

	// open the forwarding stream
	outgoingStream, streamErr := lb.processForwardingRequest(p, request.Remote, request.Protocols)

	// if we failed to open the stream, write the response and return
	if streamErr != nil {
		err = messages.WriteForwardingResponseError(s, streamErr)
		if err != nil {
			log.Warnf("writing forwarding response: %s", err)
		}
		return
	}

	defer outgoingStream.Close()

	// write accept response
	err = messages.WriteOutboundForwardingResponseSuccess(s, outgoingStream.Conn().RemotePublicKey(), outgoingStream.Protocol())
	if err != nil {
		log.Warnf("writing forwarding response: %s", err)
		return
	}

	// bridge the streams together
	lb.bridgeStreams(s, outgoingStream)
}

func (lb *LoadBalancer) processForwardingRequest(p peer.ID, remote peer.ID, protocols []protocol.ID) (network.Stream, error) {
	lb.activeRoutesLk.RLock()
	// check routes to verify ownership
	for _, id := range protocols {
		registeredPeer, ok := lb.activeRoutes[id]
		if !ok || p != registeredPeer {
			lb.activeRoutesLk.RUnlock()
			// error if this protocol not registered to this peer
			return nil, ErrNotRegistered{p, id}
		}
	}
	lb.activeRoutesLk.RUnlock()
	s, err := lb.h.NewStream(lb.ctx, remote, protocols...)
	if err != nil {
		return nil, fmt.Errorf("remote peer: %w", err)
	}
	return s, nil
}

// pipe a stream through the LB
func (lb *LoadBalancer) bridgeStreams(s1, s2 network.Stream) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		// pipe reads on s1 to writes on s2
		defer wg.Done()
		_, err := io.Copy(s2, s1)
		if err != nil {
			_ = s1.Reset()
			return
		}
		err = s2.CloseWrite()
		if err != nil {
			_ = s1.Reset()
		}
	}()
	go func() {
		// pipe reads on s2 to writes on s1
		defer wg.Done()
		_, err := io.Copy(s1, s2)
		if err != nil {
			_ = s2.Reset()
		}
		err = s1.CloseWrite()
		if err != nil {
			_ = s2.Reset()
		}
	}()
	wg.Wait()
}

func (lb *LoadBalancer) handleIncoming(s network.Stream) {
	defer s.Close()

	// check routed peer for this stream
	lb.activeRoutesLk.RLock()
	routedPeer, ok := lb.activeRoutes[s.Protocol()]
	lb.activeRoutesLk.RUnlock()

	log.Debugw("incoming stream, reforwarding to peer", "protocol", s.Protocol(), "routed peer", routedPeer, "remote peer", s.Conn().RemotePeer())

	if !ok {
		// if none exists, return
		log.Warnf("received protocol request for protocol '%s' with no router peer", s.Protocol())
		_ = s.Reset()
		return
	}

	// open a forwarding stream
	routedStream, err := lb.h.NewStream(lb.ctx, routedPeer, ForwardingProtocolID)
	if err != nil {
		log.Warnf("unable to open forwarding stream for protocol '%s' with peer %s", s.Protocol(), routedPeer)
		_ = s.Reset()
		return
	}

	defer routedStream.Close()
	// write an inbound forwarding request with the remote peer and protoocol
	err = messages.WriteInboundForwardingRequest(routedStream, s.Conn().RemotePeer(), s.Protocol())
	if err != nil {
		log.Warnf("writing forwarding request: %s", err)
		_ = routedStream.Reset()
		_ = s.Reset()
		return
	}

	lb.bridgeStreams(s, routedStream)
}
