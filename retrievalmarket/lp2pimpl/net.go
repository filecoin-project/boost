package lp2pimpl

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/boost/retrievalmarket"
	"github.com/filecoin-project/boost/retrievalmarket/types"
	"github.com/filecoin-project/go-fil-markets/shared"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	host "github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	peer "github.com/libp2p/go-libp2p-core/peer"
	protocol "github.com/libp2p/go-libp2p-core/protocol"
)

var log = logging.Logger("boost-net")

// QueryV2ProtocolID is the protocol for querying information about retrieval
// deal parameters
const QueryV2ProtocolID = protocol.ID("/fil/retrieval/qry/2.0.0")

// QueryProvider listens for incoming deal proposals over libp2p
type QueryProvider struct {
	ctx  context.Context
	host host.Host
	prov *retrievalmarket.Provider
}

const providerReadDeadline = 10 * time.Second
const providerWriteDeadline = 10 * time.Second
const clientReadDeadline = 10 * time.Second
const clientWriteDeadline = 10 * time.Second

// QueryClientOption is an option for configuring the libp2p storage deal client
type QueryClientOption func(*QueryClient)

// RetryParameters changes the default parameters around connection reopening
func RetryParameters(minDuration time.Duration, maxDuration time.Duration, attempts float64, backoffFactor float64) QueryClientOption {
	return func(c *QueryClient) {
		c.retryStream.SetOptions(shared.RetryParameters(minDuration, maxDuration, attempts, backoffFactor))
	}
}

// QueryClient sends retrieval queries over libp2p
type QueryClient struct {
	retryStream *shared.RetryStream
}

func NewQueryClient(h host.Host, options ...QueryClientOption) *QueryClient {
	c := &QueryClient{
		retryStream: shared.NewRetryStream(h),
	}
	for _, option := range options {
		option(c)
	}
	return c
}

// SendQuery sends a retrieval query over a libp2p stream to the peer
func (c *QueryClient) SendQuery(ctx context.Context, id peer.ID, query types.Query) (*types.QueryResponse, error) {
	log.Debugw("send query", "pieceCID", query.PieceCID, "payloadCID", query.PayloadCID, "provider-peer", id)

	// Create a libp2p stream to the provider
	s, err := c.retryStream.OpenStream(ctx, id, []protocol.ID{QueryV2ProtocolID})
	if err != nil {
		return nil, err
	}

	defer s.Close() // nolint

	// Set a deadline on writing to the stream so it doesn't hang
	_ = s.SetWriteDeadline(time.Now().Add(clientWriteDeadline))
	defer s.SetWriteDeadline(time.Time{}) // nolint

	// Write the retrieval query to the stream
	// Write the re to the client
	err = types.BindnodeRegistry.TypeToWriter(query, s, dagcbor.Encode)
	if err != nil {

		return nil, fmt.Errorf("sending query: %w", err)
	}

	// Set a deadline on reading from the stream so it doesn't hang
	_ = s.SetReadDeadline(time.Now().Add(clientReadDeadline))
	defer s.SetReadDeadline(time.Time{}) // nolint

	// Read the response from the stream
	queryResponsei, err := types.BindnodeRegistry.TypeFromReader(s, (*types.QueryResponse)(nil), dagcbor.Decode)
	if err != nil {
		return nil, fmt.Errorf("reading query response: %w", err)
	}
	queryResponse := queryResponsei.(*types.QueryResponse)

	log.Debugw("received queryresponse", "pieceCID", query.PieceCID, "payloadCID", query.PayloadCID, "status", queryResponse.Status, "error", queryResponse.Error)

	return queryResponse, nil
}

func NewQueryProvider(h host.Host, prov *retrievalmarket.Provider) *QueryProvider {
	p := &QueryProvider{
		host: h,
		prov: prov,
	}
	return p
}

func (p *QueryProvider) Start(ctx context.Context) {
	p.ctx = ctx
	p.host.SetStreamHandler(QueryV2ProtocolID, p.handleNewQueryStream)
}

func (p *QueryProvider) Stop() {
	p.host.RemoveStreamHandler(QueryV2ProtocolID)
}

// Called when the client opens a libp2p stream with a new deal proposal
func (p *QueryProvider) handleNewQueryStream(s network.Stream) {
	defer s.Close()

	// Set a deadline on reading from the stream so it doesn't hang
	_ = s.SetReadDeadline(time.Now().Add(providerReadDeadline))
	defer s.SetReadDeadline(time.Time{}) // nolint

	// Read the query from the stream
	queryi, err := types.BindnodeRegistry.TypeFromReader(s, (*types.Query)(nil), dagcbor.Decode)
	if err != nil {
		log.Warnw("reading query from stream", "err", err)
		return
	}
	query := queryi.(*types.Query)

	// run the query to generate a response
	queryResponse := p.prov.ExecuteQuery(query, s.Conn().RemotePeer())

	// Set a deadline on writing to the stream so it doesn't hang
	_ = s.SetWriteDeadline(time.Now().Add(providerWriteDeadline))
	defer s.SetWriteDeadline(time.Time{}) // nolint

	// Write the response to the client
	err = types.BindnodeRegistry.TypeToWriter(queryResponse, s, dagcbor.Encode)
	if err != nil {
		log.Warnw("writing query response", "pieceCID", query.PieceCID, "payloadCID", query.PayloadCID, "err", err)
		return
	}
}
