package yugabyte

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
)

//go:embed create.cql
var createCQL string

//go:embed create.sql
var createSQL string

func (s *Store) Create(ctx context.Context) error {
	log.Infow("creating cassandra tables")
	err := s.execScript(ctx, createCQL, s.execCQL)
	if err != nil {
		return fmt.Errorf("creating cassandra tables: %w", err)
	}

	log.Infow("creating postgres tables")
	err = s.execScript(ctx, createSQL, s.execSQL)
	if err != nil {
		return fmt.Errorf("creating postgres tables: %w", err)
	}

	return s.migrator.Migrate()
}

//go:embed drop.cql
var dropCQL string

//go:embed drop.sql
var dropSQL string

func (s *Store) Drop(ctx context.Context) error {
	err := s.execScript(ctx, dropCQL, s.execCQL)
	if err != nil {
		return err
	}
	return s.execScript(ctx, dropSQL, s.execSQL)
}

func (s *Store) execScript(ctx context.Context, cqlstr string, exec func(context.Context, string) error) error {
	lines := strings.Split(cqlstr, ";")
	for _, line := range lines {
		line = strings.Trim(line, "\n \t")
		if line == "" {
			continue
		}
		log.Debug(line)
		err := exec(ctx, line)
		if err != nil {
			return fmt.Errorf("executing\n%s\n%w", line, err)
		}
	}

	return nil
}

func (s *Store) execCQL(ctx context.Context, query string) error {
	return s.session.Query(query).WithContext(ctx).Exec()
}

func (s *Store) execSQL(ctx context.Context, query string) error {
	_, err := s.db.Exec(ctx, query)
	return err
}
