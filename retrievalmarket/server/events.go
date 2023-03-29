package server

import (
	"errors"
	"github.com/filecoin-project/boost-gfm/retrievalmarket"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/hannahhoward/go-pubsub"
	"time"
)

func (g *GraphsyncUnpaidRetrieval) SubscribeToDataTransferEvents(subscriber datatransfer.Subscriber) datatransfer.Unsubscribe {
	return datatransfer.Unsubscribe(g.pubSubDT.Subscribe(subscriber))
}

type dtEvent struct {
	evt   datatransfer.Event
	state datatransfer.ChannelState
}

func (g *GraphsyncUnpaidRetrieval) publishDTEvent(evtCode datatransfer.EventCode, msg string, chst datatransfer.ChannelState) {
	evt := datatransfer.Event{
		Code:      evtCode,
		Message:   msg,
		Timestamp: time.Now(),
	}
	err := g.pubSubDT.Publish(dtEvent{evt, chst})
	if err != nil {
		log.Warnf("err publishing DT event: %s", err.Error())
	}
}

func eventDispatcherDT(evt pubsub.Event, subscriberFn pubsub.SubscriberFn) error {
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

func (g *GraphsyncUnpaidRetrieval) SubscribeToMarketsEvents(subscriber retrievalmarket.ProviderSubscriber) retrievalmarket.Unsubscribe {
	return retrievalmarket.Unsubscribe(g.pubSubMkts.Subscribe(subscriber))
}

type mktsEvent struct {
	evt   retrievalmarket.ProviderEvent
	state retrievalmarket.ProviderDealState
}

func (g *GraphsyncUnpaidRetrieval) publishMktsEvent(evt retrievalmarket.ProviderEvent, state retrievalmarket.ProviderDealState) {
	err := g.pubSubMkts.Publish(mktsEvent{evt: evt, state: state})
	if err != nil {
		log.Warnf("err publishing markets event: %s", err.Error())
	}
}

func eventDispatcherMkts(evt pubsub.Event, subscriberFn pubsub.SubscriberFn) error {
	ie, ok := evt.(mktsEvent)
	if !ok {
		return errors.New("wrong type of event")
	}
	cb, ok := subscriberFn.(retrievalmarket.ProviderSubscriber)
	if !ok {
		return errors.New("wrong type of event")
	}
	cb(ie.evt, ie.state)
	return nil
}
