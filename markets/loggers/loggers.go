package marketevents

import (
	datatransfer2 "github.com/filecoin-project/boost/datatransfer"
	logging "github.com/ipfs/go-log/v2"

	"github.com/filecoin-project/boost/retrievalmarket/types/legacyretrievaltypes"
	"github.com/filecoin-project/go-state-types/abi"
)

var log = logging.Logger("markets")

// RetrievalClientLogger logs events from the retrieval client
func RetrievalClientLogger(event legacyretrievaltypes.ClientEvent, deal legacyretrievaltypes.ClientDealState) {
	method := log.Infow
	if event == legacyretrievaltypes.ClientEventBlocksReceived {
		method = log.Debugw
	}
	method("retrieval client event", "name", legacyretrievaltypes.ClientEvents[event], "deal ID", deal.ID, "state", legacyretrievaltypes.DealStatuses[deal.Status], "message", deal.Message)
}

// RetrievalProviderLogger logs events from the retrieval provider
func RetrievalProviderLogger(event legacyretrievaltypes.ProviderEvent, deal legacyretrievaltypes.ProviderDealState) {
	method := log.Infow
	if event == legacyretrievaltypes.ProviderEventBlockSent {
		method = log.Debugw
	}
	method("retrieval provider event", "name", legacyretrievaltypes.ProviderEvents[event], "deal ID", deal.ID, "receiver", deal.Receiver, "state", legacyretrievaltypes.DealStatuses[deal.Status], "message", deal.Message)
}

// DataTransferLogger logs events from the data transfer module
func DataTransferLogger(event datatransfer2.Event, state datatransfer2.ChannelState) {
	log.Debugw("data transfer event",
		"name", datatransfer2.Events[event.Code],
		"status", datatransfer2.Statuses[state.Status()],
		"transfer ID", state.TransferID(),
		"channel ID", state.ChannelID(),
		"sent", state.Sent(),
		"received", state.Received(),
		"queued", state.Queued(),
		"received count", state.ReceivedCidsTotal(),
		"total size", state.TotalSize(),
		"remote peer", state.OtherPeer(),
		"event message", event.Message,
		"channel message", state.Message())
}

// ReadyLogger returns a function to log the results of module initialization
func ReadyLogger(module string) func(error) {
	return func(err error) {
		if err != nil {
			log.Errorw("module initialization error", "module", module, "err", err)
		} else {
			log.Infow("module ready", "module", module)
		}
	}
}

type RetrievalEvent struct {
	Event         legacyretrievaltypes.ClientEvent
	Status        legacyretrievaltypes.DealStatus
	BytesReceived uint64
	FundsSpent    abi.TokenAmount
	Err           string
}
