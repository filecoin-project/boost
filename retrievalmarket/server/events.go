package server

import (
	"errors"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/hannahhoward/go-pubsub"
	"time"
)

func (g *GraphsyncUnpaidRetrieval) SubscribeToEvents(subscriber datatransfer.Subscriber) datatransfer.Unsubscribe {
	return datatransfer.Unsubscribe(g.pubSub.Subscribe(subscriber))
}

type dtEvent struct {
	evt   datatransfer.Event
	state datatransfer.ChannelState
}

func (g *GraphsyncUnpaidRetrieval) publish(evtCode datatransfer.EventCode, msg string, chst datatransfer.ChannelState) {
	evt := datatransfer.Event{
		Code:      evtCode,
		Message:   msg,
		Timestamp: time.Now(),
	}
	err := g.pubSub.Publish(dtEvent{evt, chst})
	if err != nil {
		log.Warnf("err publishing DT event: %s", err.Error())
	}
}

func eventDispatcher(evt pubsub.Event, subscriberFn pubsub.SubscriberFn) error {
	ie, ok := evt.(dtEvent)
	if !ok {
		return errors.New("wrong type of event")
	}
	cb, ok := subscriberFn.(datatransfer.Subscriber)
	if !ok {
		return errors.New("wrong type of subscriber function")
	}
	cb(ie.evt, ie.state)
	return nil
}
