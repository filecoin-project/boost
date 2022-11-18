package rtvllog

import (
	"context"
	"sync"
	"time"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("rtrvlog")

type RetrievalLog struct {
	db       *RetrievalLogDB
	duration time.Duration
	ctx      context.Context

	lastUpdateLk sync.Mutex
	lastUpdate   map[string]time.Time
	lastUpdateDT map[string]time.Time
}

func NewRetrievalLog(db *RetrievalLogDB, duration time.Duration) *RetrievalLog {
	return &RetrievalLog{
		db:         db,
		duration:   duration,
		lastUpdate: make(map[string]time.Time),
	}
}

func (r *RetrievalLog) Start(ctx context.Context) {
	r.ctx = ctx
	go r.gcUpdateMap(ctx)
	go r.gcDatabase(ctx)
}

func (r *RetrievalLog) OnQueryEvent(evt retrievalmarket.ProviderQueryEvent) {
	log.Debugw("query-event",
		"status", evt.Response.Status,
		"msg", evt.Response.Message,
		"err", evt.Error)
}

func (r *RetrievalLog) OnValidationEvent(evt retrievalmarket.ProviderValidationEvent) {
	log.Debugw("validation-event", "id", evt.Response.ID, "status", evt.Response.Status, "msg", evt.Response.Message, "err", evt.Error)

	if evt.Error == nil || evt.Error == datatransfer.ErrPause || evt.Error == datatransfer.ErrResume {
		return
	}

	st := &RetrievalDealState{
		PeerID:     evt.Receiver,
		PayloadCID: evt.BaseCid,
		Status:     retrievalmarket.DealStatusErrored.String(),
		Message:    evt.Error.Error(),
	}
	if evt.Response != nil {
		st.DealID = evt.Response.ID
		st.Status = evt.Response.Status.String()
		if evt.Response.Message != "" {
			st.Message = evt.Response.Message
		}
	}
	if evt.Proposal != nil {
		st.PieceCID = evt.Proposal.PieceCID
		st.PaymentInterval = evt.Proposal.PaymentInterval
		st.PaymentIntervalIncrease = evt.Proposal.PaymentIntervalIncrease
		st.PricePerByte = evt.Proposal.PricePerByte
		st.UnsealPrice = evt.Proposal.UnsealPrice
	}
	err := r.db.Insert(r.ctx, st)
	if err != nil {
		log.Errorw("failed to update retrieval deal logger db", "err", err)
	}
}

func (r *RetrievalLog) OnDataTransferEvent(event datatransfer.Event, state datatransfer.ChannelState) {
	log.Debugw("dt-event",
		"evt", datatransfer.Events[event.Code],
		"status", datatransfer.Statuses[state.Status()],
		"message", state.Message(),
		"is-pull", state.IsPull())

	if !state.IsPull() {
		// We're only interested in retrievals (not storage deals)
		return
	}

	switch event.Code {
	case datatransfer.DataQueued, datatransfer.DataQueuedProgress, datatransfer.DataSentProgress,
		datatransfer.DataReceived, datatransfer.DataReceivedProgress:
		return
	case datatransfer.DataSent:
		// To prevent too frequent updates, only allow data sent updates if it's
		// been more than half a second since the last one
		if !r.allowUpdate(state.ChannelID().String()) {
			return
		}
	}

	err := r.db.UpdateDataTransferState(r.ctx, event, state)
	if err != nil {
		log.Errorw("failed to update retrieval deal logger db dt state", "err", err)
	}
}

func (r *RetrievalLog) OnRetrievalEvent(event retrievalmarket.ProviderEvent, state retrievalmarket.ProviderDealState) {
	// To prevent too frequent updates, only allow block sent updates if it's
	// been more than half a second since the last one
	if event == retrievalmarket.ProviderEventBlockSent && !r.allowUpdate(state.ChannelID.String()) {
		return
	}

	var transferID datatransfer.TransferID
	if state.ChannelID != nil {
		log.Debugw("event",
			"evt", retrievalmarket.ProviderEvents[event],
			"status", state.Status,
			"initiator", state.ChannelID.Initiator,
			"responder", state.ChannelID.Responder,
			"transfer id", state.ChannelID.ID)
		transferID = state.ChannelID.ID
	} else {
		log.Debugw("event", "evt", retrievalmarket.ProviderEvents[event], "status", state.Status)
	}

	var err error
	if event == retrievalmarket.ProviderEventOpen {
		err = r.db.Insert(r.ctx, &RetrievalDealState{
			PeerID:                  state.Receiver,
			DealID:                  state.ID,
			TransferID:              transferID,
			PayloadCID:              state.PayloadCID,
			PieceCID:                state.PieceCID,
			PaymentInterval:         state.PaymentInterval,
			PaymentIntervalIncrease: state.PaymentIntervalIncrease,
			PricePerByte:            state.PricePerByte,
			UnsealPrice:             state.UnsealPrice,
			Status:                  state.Status.String(),
		})
	} else {
		err = r.db.Update(r.ctx, state)
	}

	if err != nil {
		log.Errorw("failed to update retrieval deal logger db", "err", err)
	}
}

// Some events may be very frequent, so limit events to two per second per retrieval deal
func (r *RetrievalLog) allowUpdate(key string) bool {
	now := time.Now()

	r.lastUpdateLk.Lock()
	defer r.lastUpdateLk.Unlock()

	lastAt, ok := r.lastUpdate[key]
	if !ok {
		r.lastUpdate[key] = now
		return true
	}
	if now.Sub(lastAt) > 500*time.Millisecond {
		r.lastUpdate[key] = now
		return true
	}

	return false
}

// Periodically cleans up the map of last updates, so that it doesn't take up
// too much memory
func (r *RetrievalLog) gcUpdateMap(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			r.lastUpdateLk.Lock()
			for ch, lastAt := range r.lastUpdate {
				if now.Sub(lastAt) > time.Second {
					delete(r.lastUpdate, ch)
				}
			}
			r.lastUpdateLk.Unlock()
		}
	}
}

// Periodically cleans up the retrieval deal state
func (r *RetrievalLog) gcDatabase(ctx context.Context) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			// Delete old retrieval deal state
			count, err := r.db.DeleteOlderThan(ctx, now.Add(-r.duration))
			if err != nil {
				log.Errorw("error trimming retrieval logs", "err", err)
			} else if count > 0 {
				log.Infof("Deleted %d retrieval logs older than %s", count, r.duration)
			}
		}
	}
}
