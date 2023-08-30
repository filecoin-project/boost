package cassmigrate

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/yugabyte/gocql"
	"testing"
)

var migrateFnCalled int
var migrateFn = func(ctx context.Context, session *gocql.Session) error {
	migrateFnCalled++
	return nil
}

func migrateFn1(ctx context.Context, s *gocql.Session) error { return migrateFn(ctx, s) }
func migrateFn2(ctx context.Context, s *gocql.Session) error { return migrateFn(ctx, s) }

func TestMigrate(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name      string
		migs      []migrationFn
		applied   []string
		calls     int
		expectErr bool
	}{{
		name:    "empty db",
		migs:    []migrationFn{migrateFn1, migrateFn2},
		applied: []string{},
		calls:   2,
	}, {
		name:    "one migration complete, one new",
		migs:    []migrationFn{migrateFn1, migrateFn2},
		applied: []string{"migrateFn1"},
		calls:   1,
	}, {
		name:    "all migrations complete",
		migs:    []migrationFn{migrateFn1, migrateFn2},
		applied: []string{"migrateFn1", "migrateFn2"},
		calls:   0,
	}, {
		// The ordering of records in the DB is not guaranteed so we need to sort them
		name:    "test ordering",
		migs:    []migrationFn{migrateFn1, migrateFn2},
		applied: []string{"migrateFn2", "migrateFn1"},
		calls:   0,
	}, {
		name:      "missing migration in executable",
		migs:      []migrationFn{migrateFn1, migrateFn2},
		applied:   []string{"migrateFn1", "migrateFn3"},
		calls:     0,
		expectErr: true,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			migrateFnCalled = 0
			err := executeMigrations(ctx, nil, tc.migs, tc.applied, func(s string) error { return nil })
			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.calls, migrateFnCalled)
		})
	}
}
