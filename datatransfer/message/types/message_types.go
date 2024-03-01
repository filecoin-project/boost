package types

type MessageType uint64

// Always append at the end to avoid breaking backward compatibility for cbor messages
const (
	NewMessage MessageType = iota
	UpdateMessage
	CancelMessage
	CompleteMessage
	VoucherMessage
	VoucherResultMessage

	RestartMessage
	RestartExistingChannelRequestMessage
)
