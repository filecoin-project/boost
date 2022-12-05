package bandwidthmeasure

import (
	"sync"
	"time"

	"github.com/benbjohnson/clock"
)

type bytesSent struct {
	sent   uint64
	sentAt time.Time
	next   *bytesSent
}

type BandwidthMeasure struct {
	head        *bytesSent
	tail        *bytesSent
	clock       clock.Clock
	sampleRange time.Duration
	lk          sync.Mutex
}

const DefaultBandwidthSamplePeriod = 10 * time.Second

func NewBandwidthMeasure(sampleRange time.Duration, clock clock.Clock) *BandwidthMeasure {
	return &BandwidthMeasure{
		clock:       clock,
		sampleRange: sampleRange,
	}
}

var bytesSentPool = sync.Pool{
	New: func() interface{} {
		return new(bytesSent)
	},
}

func (bm *BandwidthMeasure) RecordBytesSent(sent uint64) {
	bm.lk.Lock()
	defer bm.lk.Unlock()

	bytesSent := bytesSentPool.Get().(*bytesSent)
	bytesSent.sent = sent
	bytesSent.sentAt = bm.clock.Now()
	bytesSent.next = nil
	if bm.head == nil {
		bm.tail = bytesSent
		bm.head = bm.tail
	} else {
		bm.tail.next = bytesSent
		bm.tail = bm.tail.next
	}
	bm.pruneExpiredRecords()
}

func (bm *BandwidthMeasure) AvgBytesPerSecond() uint64 {
	bm.lk.Lock()
	defer bm.lk.Unlock()
	bm.pruneExpiredRecords()
	current := bm.head
	total := uint64(0)
	for current != nil {
		total += current.sent
		current = current.next
	}
	return total * uint64(time.Second) / uint64(bm.sampleRange)
}

func (bm *BandwidthMeasure) pruneExpiredRecords() {
	current := bm.clock.Now()
	for bm.head != nil {
		if current.Sub(bm.head.sentAt) <= bm.sampleRange {
			return
		}
		first := bm.head
		bm.head = bm.head.next
		bytesSentPool.Put(first)
	}
}
