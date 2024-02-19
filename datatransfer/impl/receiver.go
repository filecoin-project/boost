package impl

import (
	"context"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/filecoin-project/boost/datatransfer"
	"github.com/filecoin-project/boost/datatransfer/channels"
)

type receiver struct {
	manager *manager
}

// ReceiveRequest takes an incoming data transfer request, validates the voucher and
// processes the message.
func (r *receiver) ReceiveRequest(
	ctx context.Context,
	initiator peer.ID,
	incoming datatransfer.Request) {
	err := r.receiveRequest(ctx, initiator, incoming)
	if err != nil {
		log.Warnf("error processing request from %s: %s", initiator, err)
	}
}

func (r *receiver) receiveRequest(ctx context.Context, initiator peer.ID, incoming datatransfer.Request) error {
	chid := datatransfer.ChannelID{Initiator: initiator, Responder: r.manager.peerID, ID: incoming.TransferID()}
	ctx, _ = r.manager.spansIndex.SpanForChannel(ctx, chid)
	ctx, span := otel.Tracer("data-transfer").Start(ctx, "receiveRequest", trace.WithAttributes(
		attribute.String("channelID", chid.String()),
		attribute.String("baseCid", incoming.BaseCid().String()),
		attribute.Bool("isNew", incoming.IsNew()),
		attribute.Bool("isRestart", incoming.IsRestart()),
		attribute.Bool("isUpdate", incoming.IsUpdate()),
		attribute.Bool("isCancel", incoming.IsCancel()),
		attribute.Bool("isPaused", incoming.IsPaused()),
	))
	defer span.End()
	response, receiveErr := r.manager.OnRequestReceived(chid, incoming)

	if receiveErr == datatransfer.ErrResume {
		chst, err := r.manager.channels.GetByID(ctx, chid)
		if err != nil {
			return err
		}
		if resumeTransportStatesResponder.Contains(chst.Status()) {
			return r.manager.transport.(datatransfer.PauseableTransport).ResumeChannel(ctx, response, chid)
		}
		receiveErr = nil
	}

	if response != nil {
		if (response.IsNew() || response.IsRestart()) && response.Accepted() && !incoming.IsPull() {
			var channel datatransfer.ChannelState
			if response.IsRestart() {
				var err error
				channel, err = r.manager.channels.GetByID(ctx, chid)
				if err != nil {
					return err
				}
			}

			stor, _ := incoming.Selector()
			if err := r.manager.transport.OpenChannel(ctx, initiator, chid, cidlink.Link{Cid: incoming.BaseCid()}, stor, channel, response); err != nil {
				return err
			}
		} else {
			if err := r.manager.dataTransferNetwork.SendMessage(ctx, initiator, response); err != nil {
				return err
			}
		}
	}

	if receiveErr == datatransfer.ErrPause {
		return r.manager.transport.(datatransfer.PauseableTransport).PauseChannel(ctx, chid)
	}

	if receiveErr != nil {
		_ = r.manager.transport.CloseChannel(ctx, chid)
		return receiveErr
	}

	return nil
}

// ReceiveResponse handles responses to our  Push or Pull data transfer request.
// It schedules a transfer only if our Pull Request is accepted.
func (r *receiver) ReceiveResponse(
	ctx context.Context,
	sender peer.ID,
	incoming datatransfer.Response) {
	err := r.receiveResponse(ctx, sender, incoming)
	if err != nil {
		log.Error(err)
	}
}
func (r *receiver) receiveResponse(
	ctx context.Context,
	sender peer.ID,
	incoming datatransfer.Response) error {
	chid := datatransfer.ChannelID{Initiator: r.manager.peerID, Responder: sender, ID: incoming.TransferID()}
	ctx, _ = r.manager.spansIndex.SpanForChannel(ctx, chid)
	ctx, span := otel.Tracer("data-transfer").Start(ctx, "receiveResponse", trace.WithAttributes(
		attribute.String("channelID", chid.String()),
		attribute.Bool("accepted", incoming.Accepted()),
		attribute.Bool("isComplete", incoming.IsComplete()),
		attribute.Bool("isNew", incoming.IsNew()),
		attribute.Bool("isRestart", incoming.IsRestart()),
		attribute.Bool("isUpdate", incoming.IsUpdate()),
		attribute.Bool("isCancel", incoming.IsCancel()),
		attribute.Bool("isPaused", incoming.IsPaused()),
	))
	defer span.End()
	err := r.manager.OnResponseReceived(chid, incoming)
	if err == datatransfer.ErrPause {
		return r.manager.transport.(datatransfer.PauseableTransport).PauseChannel(ctx, chid)
	}
	if err != nil {
		log.Warnf("closing channel %s after getting error processing response from %s: %s",
			chid, sender, err)

		_ = r.manager.transport.CloseChannel(ctx, chid)
		return err
	}
	return nil
}

func (r *receiver) ReceiveError(err error) {
	log.Errorf("received error message on data transfer: %s", err.Error())
}

func (r *receiver) ReceiveRestartExistingChannelRequest(ctx context.Context,
	sender peer.ID,
	incoming datatransfer.Request) {

	ch, err := incoming.RestartChannelId()
	if err != nil {
		log.Errorf("cannot restart channel: failed to fetch channel Id: %w", err)
		return
	}

	ctx, _ = r.manager.spansIndex.SpanForChannel(ctx, ch)
	ctx, span := otel.Tracer("data-transfer").Start(ctx, "receiveRequest", trace.WithAttributes(
		attribute.String("channelID", ch.String()),
	))
	defer span.End()
	log.Infof("channel %s: received restart existing channel request from %s", ch, sender)

	// validate channel exists -> in non-terminal state and that the sender matches
	channel, err := r.manager.channels.GetByID(ctx, ch)
	if err != nil || channel == nil {
		// nothing to do here, we wont handle the request
		return
	}

	// initiator should be me
	if channel.ChannelID().Initiator != r.manager.peerID {
		log.Errorf("cannot restart channel %s: channel initiator is not the manager peer", ch)
		return
	}

	// other peer should be the counter party on the channel
	if channel.OtherPeer() != sender {
		log.Errorf("cannot restart channel %s: channel counterparty is not the sender peer", ch)
		return
	}

	// channel should NOT be terminated
	if channels.IsChannelTerminated(channel.Status()) {
		log.Errorf("cannot restart channel %s: channel already terminated", ch)
		return
	}

	switch r.manager.channelDataTransferType(channel) {
	case ManagerPeerCreatePush:
		if err := r.manager.openPushRestartChannel(ctx, channel); err != nil {
			log.Errorf("failed to open push restart channel %s: %s", ch, err)
		}
	case ManagerPeerCreatePull:
		if err := r.manager.openPullRestartChannel(ctx, channel); err != nil {
			log.Errorf("failed to open pull restart channel %s: %s", ch, err)
		}
	default:
		log.Error("peer is not the creator of the channel")
	}
}
