package yugabyte

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"time"

	"github.com/yugabyte/gocql"
)

//go:embed create.cql
var createCQL string

//go:embed create.sql
var createSQL string

func (s *Store) CreateKeyspace(ctx context.Context) error {
	// Create a new session using the default keyspace, then use that to create
	// the new keyspace
	log.Infow("creating cassandra keyspace " + s.cluster.Keyspace)
	cluster := gocql.NewCluster(s.settings.Hosts...)
	cluster.Timeout = time.Duration(s.settings.CQLTimeout) * time.Second
	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("creating yugabyte cluster: %w", err)
	}
	query := `CREATE KEYSPACE IF NOT EXISTS ` + s.cluster.Keyspace +
		` WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }`
	log.Debug(query)
	return session.Query(query).WithContext(ctx).Exec()
}

func (s *Store) Create(ctx context.Context) error {
	// Create the cassandra tables using the Store's session, which has been
	// configured to use the new keyspace
	log.Infow("creating cassandra tables")
	err := s.execScript(ctx, createCQL, s.execCQL)
	if err != nil {
		return fmt.Errorf("creating cassandra tables: %w", err)
	}

	// Create the postgres tables
	log.Infow("creating postgres tables")
	err = s.execScript(ctx, createSQL, s.execSQL)
	if err != nil {
		return fmt.Errorf("creating postgres tables: %w", err)
	}

	return s.migrator.Migrate(ctx)
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
