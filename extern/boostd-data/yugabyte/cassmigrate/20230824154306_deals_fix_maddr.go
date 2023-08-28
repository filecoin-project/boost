package cassmigrate

import (
	"context"
	"fmt"
	"github.com/yugabyte/gocql"
	"golang.org/x/sync/errgroup"
)

// ts20230824154306_dealsFixMinerAddr replaces MinerAddr t1234 with f1234
func ts20230824154306_dealsFixMinerAddr(ctx context.Context, session *gocql.Session) error {
	qry := `SELECT DealUuid, MinerAddr from PieceDeal`
	iter := session.Query(qry).WithContext(ctx).Iter()

	dealAddr := make(map[string]string)
	var id string
	var maddr string
	for iter.Scan(&id, &maddr) {
		if maddr[0] == 't' {
			dealAddr[id] = "f" + maddr[1:]
		}
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("getting deal miner address: %w", err)
	}

	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(64) // only run 64 go routines at a time
	for did, dmaddr := range dealAddr {
		did := did
		dmaddr := dmaddr
		eg.Go(func() error {
			qry := `UPDATE PieceDeal SET MinerAddr = ? WHERE DealUuid = ?`
			err := session.Query(qry, dmaddr, did).WithContext(ctx).Exec()
			if err != nil {
				return fmt.Errorf("setting deal %s miner address to %s: %w", did, dmaddr, err)
			}
			return nil
		})
	}

	return eg.Wait()
}
