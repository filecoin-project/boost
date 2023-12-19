package gql

import (
	"context"
	"sort"
	"time"

	gqltypes "github.com/filecoin-project/boost/gql/types"
	"github.com/filecoin-project/boost/storagemarket"
	"github.com/google/uuid"
	"github.com/graph-gophers/graphql-go"
)

type transferPoint struct {
	At    graphql.Time
	Bytes gqltypes.Uint64
}

// query: transfers: [TransferPoint]
func (r *resolver) Transfers(_ context.Context) []*transferPoint {
	return r.getTransferSamples(r.provider.Transfers(), nil)
}

type transferStats struct {
	HttpMaxConcurrentDownloads int32
	Stats                      []*hostTransferStats
}

type hostTransferStats struct {
	Host            string
	Total           int32
	Started         int32
	Stalled         int32
	TransferSamples []*transferPoint
}

// query: transferStats: TransferStats
func (r *resolver) TransferStats(_ context.Context) *transferStats {
	transfersByDeal := r.provider.Transfers()
	stats := r.provider.TransferStats()
	gqlStats := make([]*hostTransferStats, 0, len(stats))
	for _, s := range stats {
		gqlStats = append(gqlStats, &hostTransferStats{
			Host:            s.Host,
			Total:           int32(s.Total),
			Started:         int32(s.Started),
			Stalled:         int32(s.Stalled),
			TransferSamples: r.getTransferSamples(transfersByDeal, s.DealUuids),
		})
	}
	return &transferStats{
		HttpMaxConcurrentDownloads: int32(r.cfg.HttpDownload.HttpTransferMaxConcurrentDownloads),
		Stats:                      gqlStats,
	}
}

func (r *resolver) getTransferSamples(deals map[uuid.UUID][]storagemarket.TransferPoint, filter []uuid.UUID) []*transferPoint {
	// If filter is nil, include all deals
	if filter == nil {
		for dealUuid := range deals {
			filter = append(filter, dealUuid)
		}
	}

	// We have
	// dealUUID -> [At: <time>, Transferred: <bytes>, At: <time>, Transferred: <bytes>, ...]
	// Convert this to
	// <time> -> <transferred per second>
	totalAt := make(map[time.Time]uint64)
	for _, dealUuid := range filter {
		points, ok := deals[dealUuid]
		if !ok {
			continue
		}

		var prev uint64
		first := true
		for _, pt := range points {
			if first {
				first = false
				prev = pt.Bytes
				continue
			}

			transferredSincePrev := pt.Bytes - prev
			totalAt[pt.At] += transferredSincePrev
			prev = pt.Bytes
		}
	}

	// Convert map into array of transferPoints
	pts := make([]*transferPoint, 0, len(totalAt))
	for at, total := range totalAt {
		pts = append(pts, &transferPoint{
			At:    graphql.Time{Time: at},
			Bytes: gqltypes.Uint64(total),
		})
	}

	// Sort the array
	sort.Slice(pts, func(i, j int) bool {
		return pts[i].At.Before(pts[j].At.Time)
	})

	return pts
}
