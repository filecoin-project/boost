package gql

import (
	"context"
	"sort"
	"time"

	"github.com/graph-gophers/graphql-go"
)

type transferPoint struct {
	At    graphql.Time
	Bytes float64
}

// query: transfers: [TransferPoint]
func (r *resolver) Transfers(ctx context.Context) ([]*transferPoint, error) {
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
			Bytes: float64(total),
		})
	}

	// Sort the array
	sort.Slice(pts, func(i, j int) bool {
		return pts[i].At.Before(pts[j].At.Time)
	})

	return pts, nil
}
