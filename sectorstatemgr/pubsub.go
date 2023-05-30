package sectorstatemgr

import "sync"

type PubSub struct {
	sync.Mutex
	subs   []chan *SectorStateUpdates
	closed bool
}

func NewPubSub() *PubSub {
	return &PubSub{
		subs: nil,
	}
}

func (b *PubSub) Publish(msg *SectorStateUpdates) {
	b.Lock()
	defer b.Unlock()

	if b.closed {
		return
	}

	for _, ch := range b.subs {
		select {
		case ch <- msg:
		default:
			log.Warnw("subscriber is blocked, skipping push")
		}
	}
}

func (b *PubSub) Subscribe() <-chan *SectorStateUpdates {
	b.Lock()
	defer b.Unlock()

	if b.closed {
		return nil
	}

	ch := make(chan *SectorStateUpdates)
	b.subs = append(b.subs, ch)
	return ch
}

func (b *PubSub) Close() {
	b.Lock()
	defer b.Unlock()

	if b.closed {
		return
	}

	b.closed = true

	for _, sub := range b.subs {
		close(sub)
	}
}
