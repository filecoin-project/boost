package types

import (
	"github.com/google/uuid"
)

const DataTransferProtocol = "/fil/storage/transfer/1.0.0"

type HttpRequest struct {
	URL     string
	Headers map[string]string
}

type TransportDealInfo struct {
	OutputFile string
	DealUuid   uuid.UUID
	DealSize   int64
}

type TransportEvent struct {
	NBytesReceived int64
	Error          error
}

type TransferStatus string

const (
	TransferStatusStarted   TransferStatus = "TransferStatusStarted"
	TransferStatusOngoing   TransferStatus = "TransferStatusOngoing"
	TransferStatusCompleted TransferStatus = "TransferStatusCompleted"
	TransferStatusFailed    TransferStatus = "TransferStatusFailed"
)

type TransferState struct {
	LocalAddr  string
	RemoteAddr string
	Status     TransferStatus
	Sent       uint64
	Received   uint64
	Message    string
}
