package rep

import (
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

type Phase string

const (
	// QueryPhase involves a connect, query-ask, success|failure
	QueryPhase Phase = "query"
	// RetrievalPhase involves the full data retrieval: connect, proposed, accepted, first-byte-received, success|failure
	RetrievalPhase Phase = "retrieval"
)

type Code string

const (
	ConnectedCode  Code = "connected"
	QueryAskedCode Code = "query-asked"
	ProposedCode   Code = "proposed"
	AcceptedCode   Code = "accepted"
	FirstByteCode  Code = "first-byte-received"
	FailureCode    Code = "failure"
	SuccessCode    Code = "success"
)

type RetrievalEvent interface {
	// Code returns the type of event this is
	Code() Code
	// Phase returns what phase of a retrieval this even occurred on
	Phase() Phase
	// PayloadCid returns the CID being requested
	PayloadCid() cid.Cid
	// StorageProviderId returns the peer ID of the storage provider if this
	// retrieval was requested via peer ID
	StorageProviderId() peer.ID
	// StorageProviderAddr returns the peer address of the storage provider if
	// this retrieval was requested via peer address
	StorageProviderAddr() address.Address
}

var (
	_ RetrievalEvent = RetrievalEventConnect{}
	_ RetrievalEvent = RetrievalEventQueryAsk{}
	_ RetrievalEvent = RetrievalEventProposed{}
	_ RetrievalEvent = RetrievalEventAccepted{}
	_ RetrievalEvent = RetrievalEventFirstByte{}
	_ RetrievalEvent = RetrievalEventFailure{}
	_ RetrievalEvent = RetrievalEventSuccess{}
)

type RetrievalEventConnect struct {
	phase               Phase
	payloadCid          cid.Cid
	storageProviderId   peer.ID
	storageProviderAddr address.Address
}

func NewRetrievalEventConnect(phase Phase, payloadCid cid.Cid, storageProviderId peer.ID, storageProviderAddr address.Address) RetrievalEventConnect {
	return RetrievalEventConnect{phase, payloadCid, storageProviderId, storageProviderAddr}
}

type RetrievalEventQueryAsk struct {
	phase               Phase
	payloadCid          cid.Cid
	storageProviderId   peer.ID
	storageProviderAddr address.Address
	queryResponse       retrievalmarket.QueryResponse
}

func NewRetrievalEventQueryAsk(phase Phase, payloadCid cid.Cid, storageProviderId peer.ID, storageProviderAddr address.Address, queryResponse retrievalmarket.QueryResponse) RetrievalEventQueryAsk {
	return RetrievalEventQueryAsk{phase, payloadCid, storageProviderId, storageProviderAddr, queryResponse}
}

type RetrievalEventProposed struct {
	phase               Phase
	payloadCid          cid.Cid
	storageProviderId   peer.ID
	storageProviderAddr address.Address
}

func NewRetrievalEventProposed(phase Phase, payloadCid cid.Cid, storageProviderId peer.ID, storageProviderAddr address.Address) RetrievalEventProposed {
	return RetrievalEventProposed{phase, payloadCid, storageProviderId, storageProviderAddr}
}

type RetrievalEventAccepted struct {
	phase               Phase
	payloadCid          cid.Cid
	storageProviderId   peer.ID
	storageProviderAddr address.Address
}

func NewRetrievalEventAccepted(phase Phase, payloadCid cid.Cid, storageProviderId peer.ID, storageProviderAddr address.Address) RetrievalEventAccepted {
	return RetrievalEventAccepted{phase, payloadCid, storageProviderId, storageProviderAddr}
}

type RetrievalEventFirstByte struct {
	phase               Phase
	payloadCid          cid.Cid
	storageProviderId   peer.ID
	storageProviderAddr address.Address
}

func NewRetrievalEventFirstByte(phase Phase, payloadCid cid.Cid, storageProviderId peer.ID, storageProviderAddr address.Address) RetrievalEventFirstByte {
	return RetrievalEventFirstByte{phase, payloadCid, storageProviderId, storageProviderAddr}
}

type RetrievalEventFailure struct {
	phase               Phase
	payloadCid          cid.Cid
	storageProviderId   peer.ID
	storageProviderAddr address.Address
	errorMessage        string
}

func NewRetrievalEventFailure(phase Phase, payloadCid cid.Cid, storageProviderId peer.ID, storageProviderAddr address.Address, errorMessage string) RetrievalEventFailure {
	return RetrievalEventFailure{phase, payloadCid, storageProviderId, storageProviderAddr, errorMessage}
}

type RetrievalEventSuccess struct {
	phase               Phase
	payloadCid          cid.Cid
	storageProviderId   peer.ID
	storageProviderAddr address.Address
	receivedSize        uint64
	receivedCids        int64
	duration            time.Duration
	totalPayment        big.Int
}

func NewRetrievalEventSuccess(phase Phase, payloadCid cid.Cid, storageProviderId peer.ID, storageProviderAddr address.Address, receivedSize uint64, receivedCids int64, duration time.Duration, totalPayment big.Int) RetrievalEventSuccess {
	return RetrievalEventSuccess{phase, payloadCid, storageProviderId, storageProviderAddr, receivedSize, receivedCids, duration, totalPayment}
}

func (r RetrievalEventConnect) Code() Code                            { return ConnectedCode }
func (r RetrievalEventConnect) Phase() Phase                          { return r.phase }
func (r RetrievalEventConnect) PayloadCid() cid.Cid                   { return r.payloadCid }
func (r RetrievalEventConnect) StorageProviderId() peer.ID            { return r.storageProviderId }
func (r RetrievalEventConnect) StorageProviderAddr() address.Address  { return r.storageProviderAddr }
func (r RetrievalEventQueryAsk) Code() Code                           { return QueryAskedCode }
func (r RetrievalEventQueryAsk) Phase() Phase                         { return r.phase }
func (r RetrievalEventQueryAsk) PayloadCid() cid.Cid                  { return r.payloadCid }
func (r RetrievalEventQueryAsk) StorageProviderId() peer.ID           { return r.storageProviderId }
func (r RetrievalEventQueryAsk) StorageProviderAddr() address.Address { return r.storageProviderAddr }

// QueryResponse returns the response from a storage provider to a query-ask
func (r RetrievalEventQueryAsk) QueryResponse() retrievalmarket.QueryResponse { return r.queryResponse }
func (r RetrievalEventProposed) Code() Code                                   { return ProposedCode }
func (r RetrievalEventProposed) Phase() Phase                                 { return r.phase }
func (r RetrievalEventProposed) PayloadCid() cid.Cid                          { return r.payloadCid }
func (r RetrievalEventProposed) StorageProviderId() peer.ID                   { return r.storageProviderId }
func (r RetrievalEventProposed) StorageProviderAddr() address.Address         { return r.storageProviderAddr }
func (r RetrievalEventAccepted) Code() Code                                   { return AcceptedCode }
func (r RetrievalEventAccepted) Phase() Phase                                 { return r.phase }
func (r RetrievalEventAccepted) PayloadCid() cid.Cid                          { return r.payloadCid }
func (r RetrievalEventAccepted) StorageProviderId() peer.ID                   { return r.storageProviderId }
func (r RetrievalEventAccepted) StorageProviderAddr() address.Address         { return r.storageProviderAddr }
func (r RetrievalEventFirstByte) Code() Code                                  { return FirstByteCode }
func (r RetrievalEventFirstByte) Phase() Phase                                { return r.phase }
func (r RetrievalEventFirstByte) PayloadCid() cid.Cid                         { return r.payloadCid }
func (r RetrievalEventFirstByte) StorageProviderId() peer.ID                  { return r.storageProviderId }
func (r RetrievalEventFirstByte) StorageProviderAddr() address.Address        { return r.storageProviderAddr }
func (r RetrievalEventFailure) Code() Code                                    { return FailureCode }
func (r RetrievalEventFailure) Phase() Phase                                  { return r.phase }
func (r RetrievalEventFailure) PayloadCid() cid.Cid                           { return r.payloadCid }
func (r RetrievalEventFailure) StorageProviderId() peer.ID                    { return r.storageProviderId }
func (r RetrievalEventFailure) StorageProviderAddr() address.Address          { return r.storageProviderAddr }

// ErrorMessage returns a string form of the error that caused the retrieval
// failure
func (r RetrievalEventFailure) ErrorMessage() string                 { return r.errorMessage }
func (r RetrievalEventSuccess) Code() Code                           { return SuccessCode }
func (r RetrievalEventSuccess) Phase() Phase                         { return r.phase }
func (r RetrievalEventSuccess) PayloadCid() cid.Cid                  { return r.payloadCid }
func (r RetrievalEventSuccess) StorageProviderId() peer.ID           { return r.storageProviderId }
func (r RetrievalEventSuccess) StorageProviderAddr() address.Address { return r.storageProviderAddr }
func (r RetrievalEventSuccess) Duration() time.Duration              { return r.duration }
func (r RetrievalEventSuccess) TotalPayment() big.Int                { return r.totalPayment }

// ReceivedSize returns the number of bytes received
func (r RetrievalEventSuccess) ReceivedSize() uint64 { return r.receivedSize }

// ReceivedCids returns the number of (non-unique) CIDs received so far - note
// that a block can exist in more than one place in the DAG so this may not
// equal the total number of blocks transferred
func (r RetrievalEventSuccess) ReceivedCids() int64 { return r.receivedCids }
