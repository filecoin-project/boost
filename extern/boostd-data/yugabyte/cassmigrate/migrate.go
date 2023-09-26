package cassmigrate

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"strings"

	logging "github.com/ipfs/go-log/v2"
	"github.com/yugabyte/gocql"
)

var log = logging.Logger("migrations")

type migrationFn func(ctx context.Context, session *gocql.Session) error

var migrations = []migrationFn{
	ts20230824154306_dealsFixMinerAddr,
	ts20230913144459_dealsAddIsDirectDealColumn,
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
	var appliedMigrations []string
	var appliedMigration string
	for iter.Scan(&appliedMigration) {
		appliedMigrations = append(appliedMigrations, appliedMigration)
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

	return executeMigrations(ctx, session, migrations, appliedMigrations, writeComplete)
}

func executeMigrations(ctx context.Context, session *gocql.Session, migs []migrationFn, appliedMigrations []string, writeComplete func(string) error) error {
	sort.Strings(appliedMigrations)

	// For each migration
	for i, mfn := range migs {
		migName := fnName(mfn)
		// Skip completed migrations
		if i < len(appliedMigrations) {
			// Check the migrations order matches between the db and the executable
			if migName != appliedMigrations[i] {
				err := fmt.Errorf("migrations order mismatch with db: at position %d expected %s but got %s", i, appliedMigrations[i], migName)
				l := err.Error() + "\n"
				l += fmt.Sprintf("migrations to run: %d\n", len(appliedMigrations))
				for mi, mig := range migs {
					mname := fnName(mig)
					l += fmt.Sprintf("  %d.\t%s\n", mi, mname)
				}
				l += fmt.Sprintf("completed migrations in db: %d\n", len(appliedMigrations))
				for mi, dbname := range appliedMigrations {
					l += fmt.Sprintf("  %d.\t%s\n", mi, dbname)
				}
				log.Error(l)
				return err
			}
			continue
		}

		// Execute the migration
		log.Info("migrate " + migName)
		err := mfn(ctx, session)
		if err != nil {
			err = fmt.Errorf("running migration %s: %s", migName, err)
			log.Error(err.Error())
			return err
		}

		// Write the completed migration to the migration_version table
		err = writeComplete(migName)
		if err != nil {
			return err
		}
	}

	return nil
}

func fnName(mfn migrationFn) string {
	fullName := runtime.FuncForPC(reflect.ValueOf(mfn).Pointer()).Name()
	parts := strings.Split(fullName, ".")
	return parts[len(parts)-1]
}
