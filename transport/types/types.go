package types

import (
	"github.com/filecoin-project/boost/transport/types/transferstatus"
	"github.com/google/uuid"
)

type TransportDealInfo struct {
	OutputFile string
	DealUuid   uuid.UUID
	DealSize   int64
}

type TransportEvent struct {
	NBytesReceived  int64
	Status          transferstatus.TransferStatus
	IsTerminalState bool
	Error           error
}
