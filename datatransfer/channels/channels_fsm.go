package channels

import (
	logging "github.com/ipfs/go-log/v2"
	cbg "github.com/whyrusleeping/cbor-gen"

	"github.com/filecoin-project/go-statemachine/fsm"

	"github.com/filecoin-project/boost/datatransfer"
	"github.com/filecoin-project/boost/datatransfer/channels/internal"
)

var log = logging.Logger("data-transfer")

var transferringStates = []fsm.StateKey{
	datatransfer.Requested,
	datatransfer.Ongoing,
	datatransfer.InitiatorPaused,
	datatransfer.ResponderPaused,
	datatransfer.BothPaused,
	datatransfer.ResponderCompleted,
	datatransfer.ResponderFinalizing,
}

// ChannelEvents describe the events taht can
var ChannelEvents = fsm.Events{
	// Open a channel
	fsm.Event(datatransfer.Open).FromAny().To(datatransfer.Requested).Action(func(chst *internal.ChannelState) error {
		chst.AddLog("")
		return nil
	}),
	// Remote peer has accepted the Open channel request
	fsm.Event(datatransfer.Accept).From(datatransfer.Requested).To(datatransfer.Ongoing).Action(func(chst *internal.ChannelState) error {
		chst.AddLog("")
		return nil
	}),

	fsm.Event(datatransfer.TransferRequestQueued).FromAny().ToJustRecord().Action(func(chst *internal.ChannelState) error {
		chst.Message = ""
		chst.AddLog("")
		return nil
	}),

	fsm.Event(datatransfer.Restart).FromAny().ToJustRecord().Action(func(chst *internal.ChannelState) error {
		chst.Message = ""
		chst.AddLog("")
		return nil
	}),
	fsm.Event(datatransfer.Cancel).FromAny().To(datatransfer.Cancelling).Action(func(chst *internal.ChannelState) error {
		chst.AddLog("")
		return nil
	}),

	// When a channel is Opened, clear any previous error message.
	// (eg if the channel is opened after being restarted due to a connection
	// error)
	fsm.Event(datatransfer.Opened).FromAny().ToJustRecord().Action(func(chst *internal.ChannelState) error {
		chst.Message = ""
		chst.AddLog("")
		return nil
	}),

	fsm.Event(datatransfer.DataReceived).FromAny().ToNoChange().
		Action(func(chst *internal.ChannelState, rcvdBlocksTotal int64) error {
			if rcvdBlocksTotal > chst.ReceivedBlocksTotal {
				chst.ReceivedBlocksTotal = rcvdBlocksTotal
			}
			chst.AddLog("")
			return nil
		}),
	fsm.Event(datatransfer.DataReceivedProgress).FromMany(transferringStates...).ToNoChange().
		Action(func(chst *internal.ChannelState, delta uint64) error {
			chst.Received += delta
			chst.AddLog("received data")
			return nil
		}),

	fsm.Event(datatransfer.DataSent).
		FromMany(transferringStates...).ToNoChange().
		From(datatransfer.TransferFinished).ToNoChange().
		Action(func(chst *internal.ChannelState, sentBlocksTotal int64) error {
			if sentBlocksTotal > chst.SentBlocksTotal {
				chst.SentBlocksTotal = sentBlocksTotal
			}
			chst.AddLog("")
			return nil
		}),

	fsm.Event(datatransfer.DataSentProgress).FromMany(transferringStates...).ToNoChange().
		Action(func(chst *internal.ChannelState, delta uint64) error {
			chst.Sent += delta
			chst.AddLog("sending data")
			return nil
		}),

	fsm.Event(datatransfer.DataQueued).
		FromMany(transferringStates...).ToNoChange().
		From(datatransfer.TransferFinished).ToNoChange().
		Action(func(chst *internal.ChannelState, queuedBlocksTotal int64) error {
			if queuedBlocksTotal > chst.QueuedBlocksTotal {
				chst.QueuedBlocksTotal = queuedBlocksTotal
			}
			chst.AddLog("")
			return nil
		}),
	fsm.Event(datatransfer.DataQueuedProgress).FromMany(transferringStates...).ToNoChange().
		Action(func(chst *internal.ChannelState, delta uint64) error {
			chst.Queued += delta
			chst.AddLog("")
			return nil
		}),
	fsm.Event(datatransfer.Disconnected).FromAny().ToNoChange().Action(func(chst *internal.ChannelState, err error) error {
		chst.Message = err.Error()
		chst.AddLog("data transfer disconnected: %s", chst.Message)
		return nil
	}),
	fsm.Event(datatransfer.SendDataError).FromAny().ToNoChange().Action(func(chst *internal.ChannelState, err error) error {
		chst.Message = err.Error()
		chst.AddLog("data transfer send error: %s", chst.Message)
		return nil
	}),
	fsm.Event(datatransfer.ReceiveDataError).FromAny().ToNoChange().Action(func(chst *internal.ChannelState, err error) error {
		chst.Message = err.Error()
		chst.AddLog("data transfer receive error: %s", chst.Message)
		return nil
	}),
	fsm.Event(datatransfer.RequestCancelled).FromAny().ToNoChange().Action(func(chst *internal.ChannelState, err error) error {
		chst.Message = err.Error()
		chst.AddLog("data transfer request cancelled: %s", chst.Message)
		return nil
	}),
	fsm.Event(datatransfer.Error).FromAny().To(datatransfer.Failing).Action(func(chst *internal.ChannelState, err error) error {
		chst.Message = err.Error()
		chst.AddLog("data transfer erred: %s", chst.Message)
		return nil
	}),

	fsm.Event(datatransfer.NewVoucher).FromAny().ToNoChange().
		Action(func(chst *internal.ChannelState, vtype datatransfer.TypeIdentifier, voucherBytes []byte) error {
			chst.Vouchers = append(chst.Vouchers, internal.EncodedVoucher{Type: vtype, Voucher: &cbg.Deferred{Raw: voucherBytes}})
			chst.AddLog("got new voucher")
			return nil
		}),
	fsm.Event(datatransfer.NewVoucherResult).FromAny().ToNoChange().
		Action(func(chst *internal.ChannelState, vtype datatransfer.TypeIdentifier, voucherResultBytes []byte) error {
			chst.VoucherResults = append(chst.VoucherResults,
				internal.EncodedVoucherResult{Type: vtype, VoucherResult: &cbg.Deferred{Raw: voucherResultBytes}})
			chst.AddLog("got new voucher result")
			return nil
		}),

	fsm.Event(datatransfer.PauseInitiator).
		FromMany(datatransfer.Requested, datatransfer.Ongoing).To(datatransfer.InitiatorPaused).
		From(datatransfer.ResponderPaused).To(datatransfer.BothPaused).
		FromAny().ToJustRecord().Action(func(chst *internal.ChannelState) error {
		chst.AddLog("")
		return nil
	}),

	fsm.Event(datatransfer.PauseResponder).
		FromMany(datatransfer.Requested, datatransfer.Ongoing).To(datatransfer.ResponderPaused).
		From(datatransfer.InitiatorPaused).To(datatransfer.BothPaused).
		FromAny().ToJustRecord().Action(func(chst *internal.ChannelState) error {
		chst.AddLog("")
		return nil
	}),

	fsm.Event(datatransfer.ResumeInitiator).
		From(datatransfer.InitiatorPaused).To(datatransfer.Ongoing).
		From(datatransfer.BothPaused).To(datatransfer.ResponderPaused).
		FromAny().ToJustRecord().Action(func(chst *internal.ChannelState) error {
		chst.AddLog("")
		return nil
	}),

	fsm.Event(datatransfer.ResumeResponder).
		From(datatransfer.ResponderPaused).To(datatransfer.Ongoing).
		From(datatransfer.BothPaused).To(datatransfer.InitiatorPaused).
		From(datatransfer.Finalizing).To(datatransfer.Completing).
		FromAny().ToJustRecord().Action(func(chst *internal.ChannelState) error {
		chst.AddLog("")
		return nil
	}),

	// The transfer has finished on the local node - all data was sent / received
	fsm.Event(datatransfer.FinishTransfer).
		FromAny().To(datatransfer.TransferFinished).
		FromMany(datatransfer.Failing, datatransfer.Cancelling).ToJustRecord().
		From(datatransfer.ResponderCompleted).To(datatransfer.Completing).
		From(datatransfer.ResponderFinalizing).To(datatransfer.ResponderFinalizingTransferFinished).
		// If we are in the requested state, it means the other party simply never responded to our
		// our data transfer, or we never actually contacted them. In any case, it's safe to skip
		// the finalization process and complete the transfer
		From(datatransfer.Requested).To(datatransfer.Completing).
		Action(func(chst *internal.ChannelState) error {
			chst.AddLog("")
			return nil
		}),

	fsm.Event(datatransfer.ResponderBeginsFinalization).
		FromAny().To(datatransfer.ResponderFinalizing).
		FromMany(datatransfer.Failing, datatransfer.Cancelling).ToJustRecord().
		From(datatransfer.TransferFinished).To(datatransfer.ResponderFinalizingTransferFinished).Action(func(chst *internal.ChannelState) error {
		chst.AddLog("")
		return nil
	}),

	// The remote peer sent a Complete message, meaning it has sent / received all data
	fsm.Event(datatransfer.ResponderCompletes).
		FromAny().To(datatransfer.ResponderCompleted).
		FromMany(datatransfer.Failing, datatransfer.Cancelling).ToJustRecord().
		From(datatransfer.ResponderPaused).To(datatransfer.ResponderFinalizing).
		From(datatransfer.TransferFinished).To(datatransfer.Completing).
		From(datatransfer.ResponderFinalizing).To(datatransfer.ResponderCompleted).
		From(datatransfer.ResponderFinalizingTransferFinished).To(datatransfer.Completing).Action(func(chst *internal.ChannelState) error {
		chst.AddLog("")
		return nil
	}),

	fsm.Event(datatransfer.BeginFinalizing).FromAny().To(datatransfer.Finalizing).Action(func(chst *internal.ChannelState) error {
		chst.AddLog("")
		return nil
	}),

	// Both the local node and the remote peer have completed the transfer
	fsm.Event(datatransfer.Complete).FromAny().To(datatransfer.Completing).Action(func(chst *internal.ChannelState) error {
		chst.AddLog("")
		return nil
	}),

	fsm.Event(datatransfer.CleanupComplete).
		From(datatransfer.Cancelling).To(datatransfer.Cancelled).
		From(datatransfer.Failing).To(datatransfer.Failed).
		From(datatransfer.Completing).To(datatransfer.Completed).Action(func(chst *internal.ChannelState) error {
		chst.AddLog("")
		return nil
	}),

	// will kickoff state handlers for channels that were cleaning up
	fsm.Event(datatransfer.CompleteCleanupOnRestart).FromAny().ToNoChange().Action(func(chst *internal.ChannelState) error {
		chst.AddLog("")
		return nil
	}),
}

// ChannelStateEntryFuncs are handlers called as we enter different states
// (currently unused for this fsm)
var ChannelStateEntryFuncs = fsm.StateEntryFuncs{
	datatransfer.Cancelling: cleanupConnection,
	datatransfer.Failing:    cleanupConnection,
	datatransfer.Completing: cleanupConnection,
}

func cleanupConnection(ctx fsm.Context, env ChannelEnvironment, channel internal.ChannelState) error {
	otherParty := channel.Initiator
	if otherParty == env.ID() {
		otherParty = channel.Responder
	}
	env.CleanupChannel(datatransfer.ChannelID{ID: channel.TransferID, Initiator: channel.Initiator, Responder: channel.Responder})
	env.Unprotect(otherParty, datatransfer.ChannelID{ID: channel.TransferID, Initiator: channel.Initiator, Responder: channel.Responder}.String())
	return ctx.Trigger(datatransfer.CleanupComplete)
}

// CleanupStates are the penultimate states for a channel
var CleanupStates = []fsm.StateKey{
	datatransfer.Cancelling,
	datatransfer.Completing,
	datatransfer.Failing,
}

// ChannelFinalityStates are the final states for a channel
var ChannelFinalityStates = []fsm.StateKey{
	datatransfer.Cancelled,
	datatransfer.Completed,
	datatransfer.Failed,
}

// IsChannelTerminated returns true if the channel is in a finality state
func IsChannelTerminated(st datatransfer.Status) bool {
	for _, s := range ChannelFinalityStates {
		if s == st {
			return true
		}
	}

	return false
}

// IsChannelCleaningUp returns true if channel was being cleaned up and finished
func IsChannelCleaningUp(st datatransfer.Status) bool {
	for _, s := range CleanupStates {
		if s == st {
			return true
		}
	}

	return false
}
