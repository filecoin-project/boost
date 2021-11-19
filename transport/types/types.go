package types

import (
	"github.com/google/uuid"
)

type HttpRequest struct {
	URL string
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
