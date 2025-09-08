package lp2pimpl

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/boost/markets/shared"
	"github.com/filecoin-project/boost/retrievalmarket/types"
	"github.com/filecoin-project/boost/safe"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

var clog = logging.Logger("boost:lp2p:tspt:client")
var slog = logging.Logger("boost:lp2p:tspt")

// TransportsProtocolID is the protocol for querying which retrieval transports
// the Storage Provider supports (http, libp2p, etc)
const TransportsProtocolID = protocol.ID("/fil/retrieval/transports/1.0.0")

// TransportsListener listens for incoming queries over libp2p
type TransportsListener struct {
	host      host.Host
	protocols []types.Protocol
}

const streamReadDeadline = 30 * time.Second
const streamWriteDeadline = 30 * time.Second

// QueryClientOption is an option for configuring the libp2p storage deal client
type QueryClientOption func(*TransportsClient)

// RetryParameters changes the default parameters around connection reopening
func RetryParameters(minDuration time.Duration, maxDuration time.Duration, attempts float64, backoffFactor float64) QueryClientOption {
	return func(c *TransportsClient) {
		c.retryStream.SetOptions(shared.RetryParameters(minDuration, maxDuration, attempts, backoffFactor))
	}
}

// TransportsClient sends retrieval queries over libp2p
type TransportsClient struct {
	retryStream *shared.RetryStream
}

func NewTransportsClient(h host.Host, options ...QueryClientOption) *TransportsClient {
	c := &TransportsClient{
		retryStream: shared.NewRetryStream(h),
	}
	for _, option := range options {
		option(c)
	}
	return c
}

// SendQuery sends a retrieval query over a libp2p stream to the peer
func (c *TransportsClient) SendQuery(ctx context.Context, id peer.ID) (*types.QueryResponse, error) {
	clog.Debugw("query", "peer", id)

	// Create a libp2p stream to the provider
	s, err := c.retryStream.OpenStream(ctx, id, []protocol.ID{TransportsProtocolID})
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = s.Close()
	}()

	// Set a deadline on reading from the stream so it doesn't hang
	_ = s.SetReadDeadline(time.Now().Add(streamReadDeadline))
	defer func() {
		_ = s.SetReadDeadline(time.Time{})
	}()

	// Read the response from the stream
	queryResponsei, err := types.BindnodeRegistry.TypeFromReader(s, (*types.QueryResponse)(nil), dagcbor.Decode)
	if err != nil {
		return nil, fmt.Errorf("reading query response: %w", err)
	}
	queryResponse := queryResponsei.(*types.QueryResponse)

	clog.Debugw("response", "peer", id)

	return queryResponse, nil
}

func NewTransportsListener(h host.Host, protos []types.Protocol) *TransportsListener {
	return &TransportsListener{
		host:      h,
		protocols: protos,
	}
}

func (p *TransportsListener) Start() {
	p.host.SetStreamHandler(TransportsProtocolID, safe.Handle(p.handleNewQueryStream))
}

func (p *TransportsListener) Stop() {
	p.host.RemoveStreamHandler(TransportsProtocolID)
}

// Called when the client opens a libp2p stream
func (l *TransportsListener) handleNewQueryStream(s network.Stream) {
	defer func() {
		_ = s.Close()
	}()

	slog.Debugw("query", "peer", s.Conn().RemotePeer())

	response := types.QueryResponse{Protocols: l.protocols}

	// Set a deadline on writing to the stream so it doesn't hang
	_ = s.SetWriteDeadline(time.Now().Add(streamWriteDeadline))
	defer func() {
		_ = s.SetWriteDeadline(time.Time{})
	}()

	// Write the response to the client
	err := types.BindnodeRegistry.TypeToWriter(&response, s, dagcbor.Encode)
	if err != nil {
		slog.Infow("error writing query response", "peer", s.Conn().RemotePeer(), "err", err)
		return
	}
}
