package cassmigrate

import (
	"context"
	"fmt"
	logging "github.com/ipfs/go-log/v2"
	"github.com/yugabyte/gocql"
	"path"
	"runtime"
	"sort"
)

var log = logging.Logger("migrations")

type migrationFn func(ctx context.Context, session *gocql.Session) error

type migration struct {
	name string
	fn   migrationFn
}

var migrations []migration

func addMigration(fn migrationFn) {
	_, filename, _, _ := runtime.Caller(1)
	migrations = append(migrations, migration{
		name: path.Base(filename),
		fn:   fn,
	})
}

// Migrate migrates the cassandra database
func Migrate(ctx context.Context, session *gocql.Session) error {
	// Create the migration_version table if it doesn't already exist
	qry := `CREATE TABLE IF NOT EXISTS migration_version (Name TEXT PRIMARY KEY)`
	err := session.Query(qry).WithContext(ctx).Exec()
	if err != nil {
		return fmt.Errorf("creating migration_version table: %w", err)
	}

	// Get a list of migrations that have already completed
	qry = `SELECT Name from migration_version`
	iter := session.Query(qry).WithContext(ctx).Iter()
	var dbnames []string
	var name string
	for iter.Scan(&name) {
		dbnames = append(dbnames, name)
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("getting migrations from migration_version: %w", err)
	}

	// Adds each completed migration to the database
	// (extracted here to simplify the tests)
	writeComplete := func(migName string) error {
		err := session.Query("INSERT INTO migration_version (NAME) VALUES (?)", migName).WithContext(ctx).Exec()
		if err != nil {
			return fmt.Errorf("writing migration completion: %w", err)
		}
		return nil
	}

	return executeMigrations(ctx, session, migrations, dbnames, writeComplete)
}

func executeMigrations(ctx context.Context, session *gocql.Session, migs []migration, dbnames []string, writeComplete func(string) error) error {
	sort.Strings(dbnames)
	sort.Slice(migs, func(i, j int) bool {
		return migs[i].name < migs[j].name
	})

	// For each migration
	for i, m := range migs {
		// Skip completed migrations
		if i < len(dbnames) {
			// Check the migrations order matches between the db and the executable
			if m.name != dbnames[i] {
				err := fmt.Errorf("migrations order mismatch with db: at position %d expected %s but got %s", i, dbnames[i], m.name)
				l := err.Error() + "\n"
				l += fmt.Sprintf("migrations to run: %d\n", len(dbnames))
				for mi, mig := range migs {
					l += fmt.Sprintf("  %d.\t%s\n", mi, mig.name)
				}
				l += fmt.Sprintf("completed migrations in db: %d\n", len(dbnames))
				for mi, dbname := range dbnames {
					l += fmt.Sprintf("  %d.\t%s\n", mi, dbname)
				}
				log.Error(l)
				return err
			}
			continue
		}

		// Execute the migration
		log.Info("migrate " + m.name)
		err := m.fn(ctx, session)
		if err != nil {
			err = fmt.Errorf("running migration %s: %s", m.name, err)
			log.Error(err.Error())
			return err
		}

		// Write the completed migration to the migration_version table
		err = writeComplete(m.name)
		if err != nil {
			return err
		}
	}

	return nil
}
