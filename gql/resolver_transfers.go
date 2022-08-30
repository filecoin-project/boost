package gql

import (
	"context"
	"sort"
	"time"

	gqltypes "github.com/filecoin-project/boost/gql/types"
	"github.com/graph-gophers/graphql-go"
)

type transferPoint struct {
	At    graphql.Time
	Bytes gqltypes.Uint64
}

// query: transfers: [TransferPoint]
func (r *resolver) Transfers(_ context.Context) ([]*transferPoint, error) {
	deals := r.provider.Transfers()

	// We have
	// dealUUID -> [At: <time>, Transferred: <bytes>, At: <time>, Transferred: <bytes>, ...]
	// Convert this to
	// <time> -> <transferred per second>
	totalAt := make(map[time.Time]uint64)
	for _, points := range deals {
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

	return pts, nil
}

type hostTransferStats struct {
	Host    string
	Total   int32
	Started int32
	Stalled int32
}

// query: transferStats: TransferStats
func (r *resolver) TransferStats(_ context.Context) []*hostTransferStats {
	stats := r.provider.TransferStats()
	sqlStats := make([]*hostTransferStats, 0, len(stats))
	for _, s := range stats {
		sqlStats = append(sqlStats, &hostTransferStats{
			Host:    s.Host,
			Total:   int32(s.Total),
			Started: int32(s.Started),
			Stalled: int32(s.Stalled),
		})
	}
	return sqlStats
}
