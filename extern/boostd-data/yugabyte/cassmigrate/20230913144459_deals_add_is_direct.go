package cassmigrate

import (
	"context"
	"fmt"

	"github.com/yugabyte/gocql"
)

// ts20230913144459_dealsAddIsDirectDealColumn adds a new column isDirectDeal to pieceDeal table
func ts20230913144459_dealsAddIsDirectDealColumn(ctx context.Context, session *gocql.Session) error {
	qry := `ALTER TABLE idx.PieceDeal ADD IsDirectDeal BOOLEAN`
	err := session.Query(qry).WithContext(ctx).Exec()

	if err != nil {
		return fmt.Errorf("creating new column IsDirectDeal: %w", err)
	}

	qry = `UPDATE idx.PieceDeal SET IsDirectDeal = FALSE`
	err = session.Query(qry).WithContext(ctx).Exec()
	if err != nil {
		return fmt.Errorf("setting IsDirectDeal to false: %w", err)
	}
	return nil
}
