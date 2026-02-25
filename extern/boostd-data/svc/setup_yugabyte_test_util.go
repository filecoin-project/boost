package svc

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/filecoin-project/boost/extern/boostd-data/yugabyte"
	"github.com/filecoin-project/go-address"
	logging "github.com/ipfs/go-log/v2"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"github.com/yugabyte/gocql"
	"github.com/yugabyte/pgx/v5/pgxpool"
)

var tlog = logging.Logger("ybtest")

var TestYugabyteSettings = yugabyte.DBSettings{
	Hosts:         []string{"yugabyte"},
	ConnectString: "postgresql://postgres:postgres@yugabyte:5433?sslmode=disable&load_balance=true",
	CQLTimeout:    yugabyte.CqlTimeout,
}

// Used when testing against a local yugabyte instance.
var TestYugabyteSettingsLocal = yugabyte.DBSettings{
	Hosts:         []string{"localhost"},
	ConnectString: "postgresql://postgres:postgres@localhost:5433?sslmode=disable&load_balance=true",
}

func init() {
	// Uncomment when testing against a local yugabyte instance.
	//TestYugabyteSettings = TestYugabyteSettingsLocal
}

const testSchema = "boosttest"

func SetupYugabyte(t *testing.T) *Service {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	tlog.Info("wait for yugabyte start...")
	awaitYugabyteUp(t, time.Minute)
	tlog.Info("yugabyte started")

	// Create a test schema and modify the connect string to use the new schema
	err := dropTestSchema(ctx)
	require.NoError(t, err)
	err = createTestSchema(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = dropTestSchema(ctx)
	})

	settings := TestYugabyteSettings
	settings.ConnectString = TestYugabyteSettings.ConnectString + "&search_path=" + testSchema

	// Use the test keyspace to create the Cassandra tables
	storeOpts := []yugabyte.StoreOpt{yugabyte.WithCassandraKeyspace(testSchema)}
	migrator := yugabyte.NewMigrator(settings, address.TestAddress, yugabyte.WithCassandraKeyspaceOpt(testSchema))
	return NewYugabyte(settings, migrator, storeOpts...)
}

func RecreateTables(ctx context.Context, t *testing.T, store *yugabyte.Store) {
	err := dropTestSchema(ctx)
	require.NoError(t, err)
	err = createTestSchema(ctx)
	require.NoError(t, err)
	err = store.CreateKeyspace(ctx)
	require.NoError(t, err)
	err = store.Create(ctx)
	require.NoError(t, err)
}

func createTestSchema(ctx context.Context) error {
	c, err := yugabyte.StripLoadBalance(TestYugabyteSettings.ConnectString)
	if err != nil {
		return err
	}
	db, err := sql.Open("postgres", c)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, "CREATE SCHEMA "+testSchema+" AUTHORIZATION postgres")

	return nil
}

func dropTestSchema(ctx context.Context) error {
	c, err := yugabyte.StripLoadBalance(TestYugabyteSettings.ConnectString)
	if err != nil {
		return err
	}
	db, err := sql.Open("postgres", c)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+testSchema+" CASCADE")
	if err != nil {
		return err
	}

	// For the cassandra interface, we need to drop all the objects in the
	// keyspace before we can drop the keyspace itself
	cluster := gocql.NewCluster(TestYugabyteSettings.Hosts...)
	cluster.Timeout = time.Duration(TestYugabyteSettings.CQLTimeout) * time.Second
	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}

	tablesQuery := `SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?`
	iter := session.Query(tablesQuery, testSchema).WithContext(ctx).Iter()
	var tableNames []string
	var tableName string
	for iter.Scan(&tableName) {
		tableNames = append(tableNames, tableName)
	}
	err = iter.Close()
	if err != nil {
		return err
	}

	idxQuery := `SELECT table_name FROM system_schema.indexes WHERE keyspace_name = ?`
	iter = session.Query(idxQuery, testSchema).WithContext(ctx).Iter()
	var idxNames []string
	var idxName string
	for iter.Scan(&idxName) {
		idxNames = append(idxNames, idxName)
	}
	err = iter.Close()
	if err != nil {
		return err
	}

	for _, tableName := range tableNames {
		err = session.Query(`DROP TABLE IF EXISTS ` + testSchema + `.` + tableName).WithContext(ctx).Exec()
		if err != nil {
			return err
		}
	}

	for _, idxName := range idxNames {
		err = session.Query(`DROP INDEX IF EXISTS ` + testSchema + `.` + idxName).WithContext(ctx).Exec()
		if err != nil {
			return err
		}
	}

	return session.Query(`DROP KEYSPACE IF EXISTS ` + testSchema).WithContext(ctx).Exec()
}

func awaitYugabyteUp(t *testing.T, duration time.Duration) {
	start := time.Now()
	cluster := gocql.NewCluster(TestYugabyteSettings.Hosts[0])
	cluster.Timeout = time.Duration(TestYugabyteSettings.CQLTimeout) * time.Second
	for {
		_, err := cluster.CreateSession()
		if err == nil {
			_, err := pgxpool.New(context.Background(), TestYugabyteSettings.ConnectString)
			if err == nil {
				return
			}
		}

		tlog.Debugf("waiting for yugabyte: %s", err)
		if time.Since(start) > duration {
			t.Fatalf("failed to start yugabyte within %s", duration)
		}
		time.Sleep(time.Second)
	}
}
