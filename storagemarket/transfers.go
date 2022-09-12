package storagemarket

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Keep up to 20s of samples
const maxSamples = 20

// A sample of the number of bytes transferred at the given time
type TransferPoint struct {
	// The time at which the sample was taken, truncated to the nearest second
	At time.Time
	// The number of bytes transferred
	Bytes uint64
}

// dealTransfers keeps track of active transfers
type dealTransfers struct {
	// maps from deal UUID -> samples of the number of bytes transferred in
	// the last 60 seconds
	samplesLk sync.RWMutex
	samples   map[uuid.UUID][]TransferPoint

	// Maps from active transfer deal UUID to number of bytes transferred
	activeLk sync.RWMutex
	active   map[uuid.UUID]uint64
}

func newDealTransfers() *dealTransfers {
	return &dealTransfers{
		samples: make(map[uuid.UUID][]TransferPoint),
		active:  make(map[uuid.UUID]uint64),
	}
}

// For each active transfer, sample the number of bytes
// transferred every second
func (dt *dealTransfers) start(ctx context.Context) {
	// Get the current second
	now := time.Now().Truncate(time.Second)

	// Create a ticker with a one second tick
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now = now.Add(time.Second)
			dt.sample(now)

		case <-ctx.Done():
			return
		}
	}
}

func (dt *dealTransfers) sample(now time.Time) {
	dt.samplesLk.Lock()
	defer dt.samplesLk.Unlock()

	dt.activeLk.RLock()
	defer dt.activeLk.RUnlock()

	// For each active deal
	for dealUUID, bytes := range dt.active {
		// Get the array of samples
		points, ok := dt.samples[dealUUID]
		if !ok {
			points = []TransferPoint{}
		}

		// Add a point for the current second
		point := TransferPoint{
			At:    now,
			Bytes: bytes,
		}
		points = append(points, point)

		// If there are too many points, slice off the earliest ones
		if len(points) > maxSamples {
			// We need to make a copy of the array to avoid leaking memory.
			// It may be worth using a ring-buffer if this is too slow.
			newPoints := make([]TransferPoint, maxSamples)
			copy(newPoints, points[len(points)-maxSamples:])
			points = newPoints
		}

		dt.samples[dealUUID] = points
	}

	// For each deal's samples
	for dealUUID, points := range dt.samples {
		// Ignore active transfers
		_, ok := dt.active[dealUUID]
		if ok {
			continue
		}

		// Add a point for non-active samples, with the same transferred bytes
		// as the previous point
		points = append(points, TransferPoint{
			At:    now,
			Bytes: points[len(points)-1].Bytes,
		})

		// If the maximum number of samples has been reached
		if len(points) > maxSamples {
			// If the transfer has been inactive for 20s, delete the samples
			if points[0].Bytes == points[len(points)-1].Bytes {
				delete(dt.samples, dealUUID)
				continue
			}

			// Slice off samples that are too old
			points = points[1:]
		}

		dt.samples[dealUUID] = points
	}
}

func (dt *dealTransfers) transfers() map[uuid.UUID][]TransferPoint {
	dt.samplesLk.RLock()
	defer dt.samplesLk.RUnlock()

	deals := make(map[uuid.UUID][]TransferPoint, len(dt.samples))
	for dealUUID, points := range dt.samples {
		pts := make([]TransferPoint, len(points))
		copy(pts, points)
		deals[dealUUID] = pts
	}
	return deals
}

func (dt *dealTransfers) transfer(dealUUID uuid.UUID) []TransferPoint {
	dt.samplesLk.RLock()
	defer dt.samplesLk.RUnlock()

	points, ok := dt.samples[dealUUID]
	if !ok {
		return nil
	}
	pts := make([]TransferPoint, len(points))
	copy(pts, points)
	return pts
}

func (dt *dealTransfers) setBytes(dealUUID uuid.UUID, bytes uint64) {
	dt.activeLk.Lock()
	defer dt.activeLk.Unlock()

	dt.active[dealUUID] = bytes
}

func (dt *dealTransfers) getBytes(dealUUID uuid.UUID) uint64 {
	dt.activeLk.RLock()
	defer dt.activeLk.RUnlock()

	return dt.active[dealUUID]
}

func (dt *dealTransfers) complete(dealUUID uuid.UUID) {
	dt.activeLk.Lock()
	defer dt.activeLk.Unlock()

	delete(dt.active, dealUUID)
}

// Transfers returns a map of active transfers, sampled for up to 20s
func (p *Provider) Transfers() map[uuid.UUID][]TransferPoint {
	return p.transfers.transfers()
}

// Transfer returns samples of an active transfer, sampled for up to 20s
func (p *Provider) Transfer(dealUuid uuid.UUID) []TransferPoint {
	return p.transfers.transfer(dealUuid)
}

// Get the number of bytes downloaded in total for the given deal
func (p *Provider) NBytesReceived(dealUuid uuid.UUID) uint64 {
	return p.transfers.getBytes(dealUuid)
}

// Indicates if a transfer has been marked as "stalled", ie the transfer is
// not making any progress
func (p *Provider) IsTransferStalled(dealUuid uuid.UUID) bool {
	return p.xferLimiter.isStalled(dealUuid)
}

func (p *Provider) TransferStats() []*HostTransferStats {
	return p.xferLimiter.stats()
}
