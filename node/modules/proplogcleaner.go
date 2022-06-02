package modules

import (
	"context"
	"time"

	"github.com/filecoin-project/boost/db"
	logging "github.com/ipfs/go-log/v2"
	"go.uber.org/fx"
)

var dplcLog = logging.Logger("dplc")

// Boost inserts a row into the DB for each deal proposal accepted or rejected.
// This method periodically cleans up the rows.
func HandleProposalLogCleaner(duration time.Duration) func(lc fx.Lifecycle, plDB *db.ProposalLogsDB) {
	return func(lc fx.Lifecycle, plDB *db.ProposalLogsDB) {
		var cancel context.CancelFunc
		var cleanerCtx context.Context

		run := func() {
			frequency := time.Minute
			dplcLog.Debugf("Starting deal proposal log cleaner to clean up logs older than %s every %s", duration, frequency)
			timer := time.NewTicker(frequency)
			defer timer.Stop()
			for {
				select {
				case <-cleanerCtx.Done():
					return
				case <-timer.C:
					count, err := plDB.DeleteOlderThan(cleanerCtx, time.Now().Add(-duration))
					if err == nil {
						dplcLog.Debugf("Deleted %d deal proposal logs older than %s", count, duration)
					} else {
						dplcLog.Warnf("Failed to delete old deal proposal logs: %s", err)
					}
				}
			}
		}

		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				cleanerCtx, cancel = context.WithCancel(ctx)
				go run()
				return nil
			},
			OnStop: func(ctx context.Context) error {
				cancel()
				return nil
			},
		})
	}
}
