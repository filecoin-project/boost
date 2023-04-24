package yugabyte

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
)

//go:embed create.cql
var createCQL string

func (s *Store) Create(ctx context.Context) error {
	createTablesLines := strings.Split(createCQL, ";")
	for _, line := range createTablesLines {
		line = strings.Trim(line, "\n \t")
		if line == "" {
			continue
		}
		log.Debug(line)
		err := s.session.Query(line).WithContext(ctx).Exec()
		if err != nil {
			return fmt.Errorf("creating tables: executing\n%s\n%w", line, err)
		}
	}

	return nil
}

func (s *Store) Drop(ctx context.Context) error {
	tables := []string{`PayloadToPieces`, `PieceBlockOffsetSize`, `PieceMetadata`, `PieceDeal`}
	for _, tbl := range tables {
		qry := `drop table if exists idx.` + tbl
		log.Debug(qry)
		err := s.session.Query(qry).WithContext(ctx).Exec()
		if err != nil {
			return err
		}
	}
	return nil
}
