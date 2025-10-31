package protocolproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/filecoin-project/boost/protocolproxy/messages"
	"github.com/filecoin-project/boost/safe"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
)

var log = logging.Logger("protocolproxy")

type ProtocolProxy struct {
	ctx                context.Context
	h                  host.Host
	peerConfig         map[peer.ID][]protocol.ID
	supportedProtocols map[protocol.ID]peer.ID
	activeRoutesLk     sync.RWMutex
	activeRoutes       map[protocol.ID]peer.ID
}

func NewProtocolProxy(h host.Host, peerConfig map[peer.ID][]protocol.ID) (*ProtocolProxy, error) {
	// for now, double check no peers overlap in config
	// TODO: support multiple peers owning a config
	routesSet := map[protocol.ID]peer.ID{}
	for p, protocols := range peerConfig {
		for _, protocol := range protocols {
			_, existing := routesSet[protocol]
			if existing {
				return nil, errors.New("route registered for multiple peers")
			}
			routesSet[protocol] = p
		}
	}
	return &ProtocolProxy{
		h:                  h,
		activeRoutes:       make(map[protocol.ID]peer.ID),
		supportedProtocols: routesSet,
		peerConfig:         peerConfig,
	}, nil
}

func (pp *ProtocolProxy) Start(ctx context.Context) {
	pp.ctx = ctx
	pp.h.SetStreamHandler(ForwardingProtocolID, safe.Handle(pp.handleForwarding))
	msg := ""
	for id, pid := range pp.supportedProtocols {
		pp.h.SetStreamHandler(id, safe.Handle(pp.handleIncoming))
		msg += "  " + pid.String() + ": " + string(id) + "\n"
	}
	pp.h.Network().Notify(pp)
	log.Infof("started protocol proxy with protocols:\n%s", msg)
}

const routedHostTag = "protocol-proxy-routed-host"

func (pp *ProtocolProxy) Close() {
	pp.h.RemoveStreamHandler(ForwardingProtocolID)
	for id, pid := range pp.supportedProtocols {
		pp.h.RemoveStreamHandler(id)
		pp.h.ConnManager().Unprotect(pid, routedHostTag)
	}
	pp.activeRoutesLk.Lock()
	pp.activeRoutes = map[protocol.ID]peer.ID{}
	pp.activeRoutesLk.Unlock()
	pp.h.Network().StopNotify(pp)
}

// Listen satifies the network.Notifee interface but does nothing
func (pp *ProtocolProxy) Listen(network.Network, multiaddr.Multiaddr) {} // called when network starts listening on an addr

// ListenClose satifies the network.Notifee interface but does nothing
func (pp *ProtocolProxy) ListenClose(network.Network, multiaddr.Multiaddr) {} // called when network stops listening on an addr

// Connected checks the peersConfig and begins listening any time a service node connects
func (pp *ProtocolProxy) Connected(n network.Network, c network.Conn) {
	// read the peer that just connected
	p := c.RemotePeer()

	// check if they are in the peer config
	protocols, isServiceNode := pp.peerConfig[p]

	if !isServiceNode {
		return
	}

	log.Infow("service node connected activating peer protocols", "peerID", p, "protocols", protocols)

	// if they are in the peer config, listen on all protocols they are setup for
	pp.activeRoutesLk.Lock()
	defer pp.activeRoutesLk.Unlock()
	for _, id := range protocols {
		pp.activeRoutes[id] = p
	}

	pp.h.ConnManager().Protect(p, routedHostTag)
}

// Disconnected checks the peersConfig and removes listening when a service node disconnects
func (pp *ProtocolProxy) Disconnected(n network.Network, c network.Conn) { // called when a connection closed
	// read the peer that just connected
	p := c.RemotePeer()

	// check if they are in the peer config
	protocols, isServiceNode := pp.peerConfig[p]

	if !isServiceNode {
		return
	}

	log.Infow("service node disconnected deactivating peer protocols", "peerID", p, "protocols", protocols)

	// if they are in the peer config, 'un'-listen on all protocols they are setup for
	pp.activeRoutesLk.Lock()
	defer pp.activeRoutesLk.Unlock()
	for _, id := range protocols {
		delete(pp.activeRoutes, id)
	}
}

// handle a request from a routed peer to make an external ougoing connection
func (pp *ProtocolProxy) handleForwarding(s network.Stream) {
	defer func() {
		_ = s.Close()
	}()
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
	outgoingStream, streamErr := pp.processForwardingRequest(p, request.Remote, request.Protocols)

	// if we failed to open the stream, write the response and return
	if streamErr != nil {
		err = messages.WriteForwardingResponseError(s, streamErr)
		if err != nil {
			log.Warnf("writing forwarding response: %s", err)
		}
		return
	}

	defer func() {
		_ = outgoingStream.Close()
	}()

	// write accept response
	err = messages.WriteOutboundForwardingResponseSuccess(s, outgoingStream.Conn().RemotePublicKey(), outgoingStream.Protocol())
	if err != nil {
		log.Warnf("writing forwarding response: %s", err)
		return
	}

	// bridge the streams together
	pp.bridgeStreams(s, outgoingStream)
}

func (pp *ProtocolProxy) processForwardingRequest(p peer.ID, remote peer.ID, protocols []protocol.ID) (network.Stream, error) {
	pp.activeRoutesLk.RLock()
	// check routes to verify ownership
	for _, id := range protocols {
		registeredPeer, ok := pp.activeRoutes[id]
		if !ok || p != registeredPeer {
			pp.activeRoutesLk.RUnlock()
			// error if this protocol not registered to this peer
			return nil, ErrNotRegistered{p, id}
		}
	}
	pp.activeRoutesLk.RUnlock()
	s, err := pp.h.NewStream(pp.ctx, remote, protocols...)
	if err != nil {
		return nil, fmt.Errorf("remote peer: %w", err)
	}
	return s, nil
}

// pipe a stream through the PP
func (pp *ProtocolProxy) bridgeStreams(s1, s2 network.Stream) {
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
			return
		}
		err = s1.CloseWrite()
		if err != nil {
			_ = s2.Reset()
		}
	}()
	wg.Wait()
}

func (pp *ProtocolProxy) handleIncoming(s network.Stream) {
	defer func() {
		_ = s.Close()
	}()

	// check routed peer for this stream
	pp.activeRoutesLk.RLock()
	routedPeer, ok := pp.activeRoutes[s.Protocol()]
	pp.activeRoutesLk.RUnlock()

	if !ok {
		// if none exists, return
		log.Infof("received protocol request for protocol '%s' with no active peer", s.Protocol())
		_ = s.Reset()
		return
	}

	log.Debugw("incoming stream, reforwarding to peer", "protocol", s.Protocol(), "routed peer", routedPeer, "remote peer", s.Conn().RemotePeer())

	// open a forwarding stream
	routedStream, err := pp.h.NewStream(pp.ctx, routedPeer, ForwardingProtocolID)
	if err != nil {
		log.Warnf("unable to open forwarding stream for protocol '%s' with peer %s", s.Protocol(), routedPeer)
		_ = s.Reset()
		return
	}

	defer func() {
		_ = routedStream.Close()
	}()
	// write an inbound forwarding request with the remote peer and protoocol
	err = messages.WriteInboundForwardingRequest(routedStream, s.Conn().RemotePeer(), s.Protocol())
	if err != nil {
		log.Warnf("writing forwarding request: %s", err)
		_ = routedStream.Reset()
		_ = s.Reset()
		return
	}

	pp.bridgeStreams(s, routedStream)
}
