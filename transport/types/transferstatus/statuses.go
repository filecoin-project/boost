package transferstatus

type TransferStatus int

const (
	Initiated TransferStatus = iota
	DataReceived
	Finished
	Cancelled
	Errored
)
