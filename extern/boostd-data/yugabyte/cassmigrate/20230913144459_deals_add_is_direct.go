package cassmigrate

import (
	"context"
	"fmt"
	"strings"

	"github.com/yugabyte/gocql"
	"golang.org/x/sync/errgroup"
)

// ts20230913144459_dealsAddIsDirectDealColumn adds a new column isDirectDeal to pieceDeal table
func ts20230913144459_dealsAddIsDirectDealColumn(ctx context.Context, session *gocql.Session) error {
	qry := `ALTER TABLE PieceDeal ADD IsDirectDeal BOOLEAN`
	err := session.Query(qry).WithContext(ctx).Exec()

	if err != nil {
		if strings.Contains(err.Error(), "code=2200") {
			log.Warn("column IsDirectDeal already exists")
		} else {
			return fmt.Errorf("creating new column IsDirectDeal: %w", err)
		}
	}

	qry = `SELECT DealUuid from PieceDeal`
	iter := session.Query(qry).WithContext(ctx).Iter()

	dealIsDirect := make(map[string]bool)
	var id string
	for iter.Scan(&id) {
		dealIsDirect[id] = false
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("getting deal miner address: %w", err)
	}

	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(64) // only run 64 go routines at a time
	for did, isDD := range dealIsDirect {
		i := did
		dd := isDD
		eg.Go(func() error {
			qry1 := `UPDATE PieceDeal SET IsDirectDeal = ? WHERE DealUuid = ?`
			err1 := session.Query(qry1, dd, i).WithContext(ctx).Exec()
			if err1 != nil {
				return fmt.Errorf("setting deal %s miner address to %t: %w", i, dd, err1)
			}
			return nil
		})
	}

	return eg.Wait()
}
