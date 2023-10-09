package impl

import (
	"context"
	"errors"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/boost/datatransfer"
	"github.com/filecoin-project/boost/datatransfer/encoding"
	"github.com/filecoin-project/boost/datatransfer/registry"
)

// OnChannelOpened is called when we send a request for data to the other
// peer on the given channel ID
func (m *manager) OnChannelOpened(chid datatransfer.ChannelID) error {
	log.Infof("channel %s: opened", chid)

	// Check if the channel is being tracked
	has, err := m.channels.HasChannel(chid)
	if err != nil {
		return err
	}
	if !has {
		return datatransfer.ErrChannelNotFound
	}

	// Fire an event
	return m.channels.ChannelOpened(chid)
}

// OnDataReceived is called when the transport layer reports that it has
// received some data from the sender.
// It fires an event on the channel, updating the sum of received data and
// calls revalidators so they can pause / resume the channel or send a
// message over the transport.
func (m *manager) OnDataReceived(chid datatransfer.ChannelID, link ipld.Link, size uint64, index int64, unique bool) error {
	ctx, _ := m.spansIndex.SpanForChannel(context.TODO(), chid)
	ctx, span := otel.Tracer("data-transfer").Start(ctx, "dataReceived", trace.WithAttributes(
		attribute.String("channelID", chid.String()),
		attribute.String("link", link.String()),
		attribute.Int64("index", index),
		attribute.Int64("size", int64(size)),
	))
	defer span.End()

	isNew, err := m.channels.DataReceived(chid, link.(cidlink.Link).Cid, size, index, unique)
	if err != nil {
		return err
	}

	// If this block has already been received on the channel, take no further
	// action (this can happen when the data-transfer channel is restarted)
	if !isNew {
		return nil
	}

	// If this node initiated the data transfer, there's nothing more to do
	if chid.Initiator == m.peerID {
		return nil
	}

	// Check each revalidator to see if they want to pause / resume, or send
	// a message over the transport
	var result datatransfer.VoucherResult
	var handled bool
	_ = m.revalidators.Each(func(_ datatransfer.TypeIdentifier, _ encoding.Decoder, processor registry.Processor) error {
		revalidator := processor.(datatransfer.Revalidator)
		handled, result, err = revalidator.OnPushDataReceived(chid, size)
		if handled {
			return errors.New("stop processing")
		}
		return nil
	})
	if err != nil || result != nil {
		msg, err := m.processRevalidationResult(chid, result, err)
		if msg != nil {
			ctx, _ := m.spansIndex.SpanForChannel(context.TODO(), chid)
			if err := m.dataTransferNetwork.SendMessage(ctx, chid.Initiator, msg); err != nil {
				return err
			}
		}
		return err
	}

	return nil
}

// OnDataQueued is called when the transport layer reports that it has queued
// up some data to be sent to the requester.
// It fires an event on the channel, updating the sum of queued data and calls
// revalidators so they can pause / resume or send a message over the transport.
func (m *manager) OnDataQueued(chid datatransfer.ChannelID, link ipld.Link, size uint64, index int64, unique bool) (datatransfer.Message, error) {
	// The transport layer reports that some data has been queued up to be sent
	// to the requester, so fire a DataQueued event on the channels state
	// machine.

	ctx, _ := m.spansIndex.SpanForChannel(context.TODO(), chid)
	ctx, span := otel.Tracer("data-transfer").Start(ctx, "dataQueued", trace.WithAttributes(
		attribute.String("channelID", chid.String()),
		attribute.String("link", link.String()),
		attribute.Int64("size", int64(size)),
	))
	defer span.End()

	isNew, err := m.channels.DataQueued(chid, link.(cidlink.Link).Cid, size, index, unique)
	if err != nil {
		return nil, err
	}

	// If this block has already been queued on the channel, take no further
	// action (this can happen when the data-transfer channel is restarted)
	if !isNew {
		return nil, nil
	}

	// If this node initiated the data transfer, there's nothing more to do
	if chid.Initiator == m.peerID {
		return nil, nil
	}

	// Check each revalidator to see if they want to pause / resume, or send
	// a message over the transport.
	// For example if the data-sender is waiting for the receiver to pay for
	// data they may pause the data-transfer.
	var result datatransfer.VoucherResult
	var handled bool
	_ = m.revalidators.Each(func(_ datatransfer.TypeIdentifier, _ encoding.Decoder, processor registry.Processor) error {
		revalidator := processor.(datatransfer.Revalidator)
		handled, result, err = revalidator.OnPullDataSent(chid, size)
		if handled {
			return errors.New("stop processing")
		}
		return nil
	})
	if err != nil || result != nil {
		return m.processRevalidationResult(chid, result, err)
	}

	return nil, nil
}

func (m *manager) OnDataSent(chid datatransfer.ChannelID, link ipld.Link, size uint64, index int64, unique bool) error {

	ctx, _ := m.spansIndex.SpanForChannel(context.TODO(), chid)
	ctx, span := otel.Tracer("data-transfer").Start(ctx, "dataSent", trace.WithAttributes(
		attribute.String("channelID", chid.String()),
		attribute.String("link", link.String()),
		attribute.Int64("size", int64(size)),
	))
	defer span.End()

	_, err := m.channels.DataSent(chid, link.(cidlink.Link).Cid, size, index, unique)
	return err
}

func (m *manager) OnRequestReceived(chid datatransfer.ChannelID, request datatransfer.Request) (datatransfer.Response, error) {
	if request.IsRestart() {
		return m.receiveRestartRequest(chid, request)
	}

	if request.IsNew() {
		return m.receiveNewRequest(chid, request)
	}
	if request.IsCancel() {
		log.Infof("channel %s: received cancel request, cleaning up channel", chid)

		m.transport.CleanupChannel(chid)
		return nil, m.channels.Cancel(chid)
	}
	if request.IsVoucher() {
		return m.processUpdateVoucher(chid, request)
	}
	if request.IsPaused() {
		return nil, m.pauseOther(chid)
	}
	err := m.resumeOther(chid)
	if err != nil {
		return nil, err
	}
	chst, err := m.channels.GetByID(context.TODO(), chid)
	if err != nil {
		return nil, err
	}
	if chst.Status() == datatransfer.ResponderPaused ||
		chst.Status() == datatransfer.ResponderFinalizing {
		return nil, datatransfer.ErrPause
	}
	return nil, nil
}

func (m *manager) OnTransferQueued(chid datatransfer.ChannelID) {
	m.channels.TransferRequestQueued(chid)
}

func (m *manager) OnResponseReceived(chid datatransfer.ChannelID, response datatransfer.Response) error {
	if response.IsComplete() {
		log.Infow("received complete response", "chid", chid, "isAccepted", response.Accepted())
	}

	if response.IsCancel() {
		log.Infof("channel %s: received cancel response, cancelling channel", chid)
		return m.channels.Cancel(chid)
	}
	if response.IsVoucherResult() {
		if !response.EmptyVoucherResult() {
			vresult, err := m.decodeVoucherResult(response)
			if err != nil {
				return err
			}
			err = m.channels.NewVoucherResult(chid, vresult)
			if err != nil {
				return err
			}
		}
		if !response.Accepted() {
			log.Infof("channel %s: received rejected response, erroring out channel", chid)
			return m.channels.Error(chid, datatransfer.ErrRejected)
		}
		if response.IsNew() {
			log.Infof("channel %s: received new response, accepting channel", chid)
			err := m.channels.Accept(chid)
			if err != nil {
				return err
			}
		}

		if response.IsRestart() {
			log.Infof("channel %s: received restart response, restarting channel", chid)
			err := m.channels.Restart(chid)
			if err != nil {
				return err
			}
		}
	}
	if response.IsComplete() && response.Accepted() {
		if !response.IsPaused() {
			log.Infow("received complete response,responder not paused, completing channel", "chid", chid)
			return m.channels.ResponderCompletes(chid)
		}

		log.Infow("received complete response, responder is paused, not completing channel", "chid", chid)
		err := m.channels.ResponderBeginsFinalization(chid)
		if err != nil {
			return nil
		}
	}
	if response.IsPaused() {
		return m.pauseOther(chid)
	}
	return m.resumeOther(chid)
}

func (m *manager) OnRequestCancelled(chid datatransfer.ChannelID, err error) error {
	log.Warnf("channel %+v was cancelled: %s", chid, err)
	return m.channels.RequestCancelled(chid, err)
}

func (m *manager) OnRequestDisconnected(chid datatransfer.ChannelID, err error) error {
	log.Warnf("channel %+v has stalled or disconnected: %s", chid, err)
	return m.channels.Disconnected(chid, err)
}

func (m *manager) OnSendDataError(chid datatransfer.ChannelID, err error) error {
	log.Debugf("channel %+v had transport send error: %s", chid, err)
	return m.channels.SendDataError(chid, err)
}

func (m *manager) OnReceiveDataError(chid datatransfer.ChannelID, err error) error {
	log.Debugf("channel %+v had transport receive error: %s", chid, err)
	return m.channels.ReceiveDataError(chid, err)
}

// OnChannelCompleted is called
// - by the requester when all data for a transfer has been received
// - by the responder when all data for a transfer has been sent
func (m *manager) OnChannelCompleted(chid datatransfer.ChannelID, completeErr error) error {
	// If the channel completed successfully
	if completeErr == nil {
		// If the channel was initiated by the other peer
		if chid.Initiator != m.peerID {
			log.Infow("received OnChannelCompleted, will send completion message to initiator", "chid", chid)
			msg, err := m.completeMessage(chid)
			if err != nil {
				return err
			}
			if msg != nil {
				// Send the other peer a message that the transfer has completed
				log.Infow("sending completion message to initiator", "chid", chid)
				ctx, _ := m.spansIndex.SpanForChannel(context.Background(), chid)
				if err := m.dataTransferNetwork.SendMessage(ctx, chid.Initiator, msg); err != nil {
					err := xerrors.Errorf("channel %s: failed to send completion message to initiator: %w", chid, err)
					log.Warnw("failed to send completion message to initiator", "chid", chid, "err", err)
					return m.OnRequestDisconnected(chid, err)
				}
				log.Infow("successfully sent completion message to initiator", "chid", chid)
			}
			if msg.Accepted() {
				if msg.IsPaused() {
					return m.channels.BeginFinalizing(chid)
				}
				return m.channels.Complete(chid)
			}
			return m.channels.Error(chid, err)
		}

		// The channel was initiated by this node, so move to the finished state
		log.Infof("channel %s: transfer initiated by local node is complete", chid)
		return m.channels.FinishTransfer(chid)
	}

	// There was an error so fire an Error event
	chst, err := m.channels.GetByID(context.TODO(), chid)
	if err != nil {
		return err
	}
	// send an error, but only if we haven't already errored for some reason
	if chst.Status() != datatransfer.Failing && chst.Status() != datatransfer.Failed {
		err := xerrors.Errorf("data transfer channel %s failed to transfer data: %w", chid, completeErr)
		log.Warnf(err.Error())
		return m.channels.Error(chid, err)
	}
	return nil
}

func (m *manager) OnContextAugment(chid datatransfer.ChannelID) func(context.Context) context.Context {
	return func(ctx context.Context) context.Context {
		ctx, _ = m.spansIndex.SpanForChannel(ctx, chid)
		return ctx
	}
}

func (m *manager) receiveRestartRequest(chid datatransfer.ChannelID, incoming datatransfer.Request) (datatransfer.Response, error) {
	log.Infof("channel %s: received restart request", chid)

	result, err := m.restartRequest(chid, incoming)
	msg, msgErr := m.response(true, false, err, incoming.TransferID(), result)
	if msgErr != nil {
		return nil, msgErr
	}
	return msg, err
}

func (m *manager) receiveNewRequest(chid datatransfer.ChannelID, incoming datatransfer.Request) (datatransfer.Response, error) {
	log.Infof("channel %s: received new channel request from %s", chid, chid.Initiator)

	result, err := m.acceptRequest(chid, incoming)
	msg, msgErr := m.response(false, true, err, incoming.TransferID(), result)
	if msgErr != nil {
		return nil, msgErr
	}
	return msg, err
}

func (m *manager) restartRequest(chid datatransfer.ChannelID,
	incoming datatransfer.Request) (datatransfer.VoucherResult, error) {

	initiator := chid.Initiator
	if m.peerID == initiator {
		return nil, xerrors.New("initiator cannot be manager peer for a restart request")
	}

	if err := m.validateRestartRequest(context.Background(), initiator, chid, incoming); err != nil {
		return nil, xerrors.Errorf("restart request for channel %s failed validation: %w", chid, err)
	}

	stor, err := incoming.Selector()
	if err != nil {
		return nil, err
	}

	voucher, result, err := m.validateVoucher(true, chid, initiator, incoming, incoming.IsPull(), incoming.BaseCid(), stor)
	if err != nil && err != datatransfer.ErrPause {
		return result, xerrors.Errorf("failed to validate voucher: %w", err)
	}
	voucherErr := err

	if result != nil {
		err := m.channels.NewVoucherResult(chid, result)
		if err != nil {
			return result, err
		}
	}
	if err := m.channels.Restart(chid); err != nil {
		return result, xerrors.Errorf("failed to restart channel %s: %w", chid, err)
	}
	processor, has := m.transportConfigurers.Processor(voucher.Type())
	if has {
		transportConfigurer := processor.(datatransfer.TransportConfigurer)
		transportConfigurer(chid, voucher, m.transport)
	}
	m.dataTransferNetwork.Protect(initiator, chid.String())
	if voucherErr == datatransfer.ErrPause {
		err := m.channels.PauseResponder(chid)
		if err != nil {
			return result, err
		}
	}
	return result, voucherErr
}

func (m *manager) acceptRequest(chid datatransfer.ChannelID, incoming datatransfer.Request) (datatransfer.VoucherResult, error) {

	stor, err := incoming.Selector()
	if err != nil {
		return nil, err
	}

	voucher, result, err := m.validateVoucher(false, chid, chid.Initiator, incoming, incoming.IsPull(), incoming.BaseCid(), stor)
	if err != nil && err != datatransfer.ErrPause {
		return result, err
	}
	voucherErr := err

	var dataSender, dataReceiver peer.ID
	if incoming.IsPull() {
		dataSender = m.peerID
		dataReceiver = chid.Initiator
	} else {
		dataSender = chid.Initiator
		dataReceiver = m.peerID
	}

	log.Infow("data-transfer request validated, will create & start tracking channel", "channelID", chid, "payloadCid", incoming.BaseCid())
	_, err = m.channels.CreateNew(m.peerID, incoming.TransferID(), incoming.BaseCid(), stor, voucher, chid.Initiator, dataSender, dataReceiver)
	if err != nil {
		log.Errorw("failed to create and start tracking channel", "channelID", chid, "err", err)
		return result, err
	}
	log.Debugw("successfully created and started tracking channel", "channelID", chid)
	if result != nil {
		err := m.channels.NewVoucherResult(chid, result)
		if err != nil {
			return result, err
		}
	}
	if err := m.channels.Accept(chid); err != nil {
		return result, err
	}
	processor, has := m.transportConfigurers.Processor(voucher.Type())
	if has {
		transportConfigurer := processor.(datatransfer.TransportConfigurer)
		transportConfigurer(chid, voucher, m.transport)
	}
	m.dataTransferNetwork.Protect(chid.Initiator, chid.String())
	if voucherErr == datatransfer.ErrPause {
		err := m.channels.PauseResponder(chid)
		if err != nil {
			return result, err
		}
	}
	return result, voucherErr
}

// validateVoucher converts a voucher in an incoming message to its appropriate
// voucher struct, then runs the validator and returns the results.
// returns error if:
//   - reading voucher fails
//   - deserialization of selector fails
//   - validation fails
func (m *manager) validateVoucher(
	isRestart bool,
	chid datatransfer.ChannelID,
	sender peer.ID,
	incoming datatransfer.Request,
	isPull bool,
	baseCid cid.Cid,
	stor ipld.Node,
) (datatransfer.Voucher, datatransfer.VoucherResult, error) {
	vouch, err := m.decodeVoucher(incoming, m.validatedTypes)
	if err != nil {
		return nil, nil, err
	}
	var validatorFunc func(bool, datatransfer.ChannelID, peer.ID, datatransfer.Voucher, cid.Cid, ipld.Node) (datatransfer.VoucherResult, error)
	processor, _ := m.validatedTypes.Processor(vouch.Type())
	validator := processor.(datatransfer.RequestValidator)
	if isPull {
		validatorFunc = validator.ValidatePull
	} else {
		validatorFunc = validator.ValidatePush
	}

	result, err := validatorFunc(isRestart, chid, sender, vouch, baseCid, stor)
	return vouch, result, err
}

// revalidateVoucher converts a voucher in an incoming message to its appropriate
// voucher struct, then runs the revalidator and returns the results.
// returns error if:
//   - reading voucher fails
//   - deserialization of selector fails
//   - validation fails
func (m *manager) revalidateVoucher(chid datatransfer.ChannelID,
	incoming datatransfer.Request) (datatransfer.Voucher, datatransfer.VoucherResult, error) {
	vouch, err := m.decodeVoucher(incoming, m.revalidators)
	if err != nil {
		return nil, nil, err
	}
	processor, _ := m.revalidators.Processor(vouch.Type())
	validator := processor.(datatransfer.Revalidator)

	result, err := validator.Revalidate(chid, vouch)
	return vouch, result, err
}

func (m *manager) processUpdateVoucher(chid datatransfer.ChannelID, request datatransfer.Request) (datatransfer.Response, error) {
	vouch, result, voucherErr := m.revalidateVoucher(chid, request)
	if vouch != nil {
		err := m.channels.NewVoucher(chid, vouch)
		if err != nil {
			return nil, err
		}
	}
	return m.processRevalidationResult(chid, result, voucherErr)
}

func (m *manager) revalidationResponse(chid datatransfer.ChannelID, result datatransfer.VoucherResult, resultErr error) (datatransfer.Response, error) {
	chst, err := m.channels.GetByID(context.TODO(), chid)
	if err != nil {
		return nil, err
	}
	if chst.Status() == datatransfer.Finalizing {
		return m.completeResponse(resultErr, chid.ID, result)
	}
	return m.response(false, false, resultErr, chid.ID, result)
}

func (m *manager) processRevalidationResult(chid datatransfer.ChannelID, result datatransfer.VoucherResult, resultErr error) (datatransfer.Response, error) {
	vresMessage, err := m.revalidationResponse(chid, result, resultErr)

	if err != nil {
		return nil, err
	}
	if result != nil {
		err := m.channels.NewVoucherResult(chid, result)
		if err != nil {
			return nil, err
		}
	}

	if resultErr == nil {
		return vresMessage, nil
	}

	if resultErr == datatransfer.ErrPause {
		err := m.pause(chid)
		if err != nil {
			return nil, err
		}
		return vresMessage, datatransfer.ErrPause
	}

	if resultErr == datatransfer.ErrResume {
		err = m.resume(chid)
		if err != nil {
			return nil, err
		}
		return vresMessage, datatransfer.ErrResume
	}
	return vresMessage, resultErr
}

func (m *manager) completeMessage(chid datatransfer.ChannelID) (datatransfer.Response, error) {
	var result datatransfer.VoucherResult
	var resultErr error
	var handled bool
	_ = m.revalidators.Each(func(_ datatransfer.TypeIdentifier, _ encoding.Decoder, processor registry.Processor) error {
		revalidator := processor.(datatransfer.Revalidator)
		handled, result, resultErr = revalidator.OnComplete(chid)
		if handled {
			return errors.New("stop processing")
		}
		return nil
	})
	if result != nil {
		err := m.channels.NewVoucherResult(chid, result)
		if err != nil {
			return nil, err
		}
	}

	return m.completeResponse(resultErr, chid.ID, result)
}
