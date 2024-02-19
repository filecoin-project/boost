package server

import (
	"errors"
	"time"

	datatransfer2 "github.com/filecoin-project/boost/datatransfer"
	"github.com/filecoin-project/boost/retrievalmarket/types/legacyretrievaltypes"
	"github.com/hannahhoward/go-pubsub"
)

func (g *GraphsyncUnpaidRetrieval) SubscribeToDataTransferEvents(subscriber datatransfer2.Subscriber) datatransfer2.Unsubscribe {
	return datatransfer2.Unsubscribe(g.pubSubDT.Subscribe(subscriber))
}

type dtEvent struct {
	evt   datatransfer2.Event
	state datatransfer2.ChannelState
}

func (g *GraphsyncUnpaidRetrieval) publishDTEvent(evtCode datatransfer2.EventCode, msg string, chst datatransfer2.ChannelState) {
	evt := datatransfer2.Event{
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
	cb, ok := subscriberFn.(datatransfer2.Subscriber)
	if !ok {
		return errors.New("wrong type of subscriber function")
	}
	cb(ie.evt, ie.state)
	return nil
}

func (g *GraphsyncUnpaidRetrieval) SubscribeToMarketsEvents(subscriber ProviderSubscriber) legacyretrievaltypes.Unsubscribe {
	return legacyretrievaltypes.Unsubscribe(g.pubSubMkts.Subscribe(subscriber))
}

type mktsEvent struct {
	evt   legacyretrievaltypes.ProviderEvent
	state legacyretrievaltypes.ProviderDealState
}

func (g *GraphsyncUnpaidRetrieval) publishMktsEvent(evt legacyretrievaltypes.ProviderEvent, state legacyretrievaltypes.ProviderDealState) {
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
	cb, ok := subscriberFn.(ProviderSubscriber)
	if !ok {
		return errors.New("wrong type of event")
	}
	cb(ie.evt, ie.state)
	return nil
}

// ProviderSubscriber is a callback that is registered to listen for retrieval events on a provider
type ProviderSubscriber func(event legacyretrievaltypes.ProviderEvent, state legacyretrievaltypes.ProviderDealState)

// ProviderQueryEventSubscriber is a callback that is registered to listen for query message events
type ProviderQueryEventSubscriber func(evt legacyretrievaltypes.ProviderQueryEvent)

// ProviderValidationSubscriber is a callback that is registered to listen for validation events
type ProviderValidationSubscriber func(evt legacyretrievaltypes.ProviderValidationEvent)
