package cassmigrate

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/yugabyte/gocql"
	"testing"
)

func TestMigrate(t *testing.T) {
	ctx := context.Background()

	var migrateFnCalled int
	migrateFn := func(ctx context.Context, session *gocql.Session) error {
		migrateFnCalled++
		return nil
	}
	testCases := []struct {
		name      string
		migs      []migration
		dbnames   []string
		calls     int
		expectErr bool
	}{{
		name: "empty db",
		migs: []migration{{
			name: "1-migrate",
			fn:   migrateFn,
		}, {
			name: "2-migrate",
			fn:   migrateFn,
		}},
		dbnames: []string{},
		calls:   2,
	}, {
		name: "one migration complete, one new",
		migs: []migration{{
			name: "1-migrate",
			fn:   migrateFn,
		}, {
			name: "2-migrate",
			fn:   migrateFn,
		}},
		dbnames: []string{"1-migrate"},
		calls:   1,
	}, {
		name: "all migrations complete",
		migs: []migration{{
			name: "1-migrate",
			fn:   migrateFn,
		}, {
			name: "2-migrate",
			fn:   migrateFn,
		}},
		dbnames: []string{"1-migrate", "2-migrate"},
		calls:   0,
	}, {
		name: "test ordering",
		migs: []migration{{
			name: "2-migrate",
			fn:   migrateFn,
		}, {
			name: "1-migrate",
			fn:   migrateFn,
		}},
		dbnames: []string{"1-migrate"},
		calls:   1,
	}, {
		name: "missing migration in executable",
		migs: []migration{{
			name: "1-migrate",
			fn:   migrateFn,
		}, {
			name: "2-migrate",
			fn:   migrateFn,
		}},
		dbnames:   []string{"1-migrate", "3-migrate"},
		calls:     0,
		expectErr: true,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			migrateFnCalled = 0
			err := executeMigrations(ctx, nil, tc.migs, tc.dbnames, func(s string) error { return nil })
			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.calls, migrateFnCalled)
		})
	}
}
