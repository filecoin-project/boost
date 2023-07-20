package svc

import (
	"github.com/filecoin-project/boostd-data/yugabyte"
	logging "github.com/ipfs/go-log/v2"
	"github.com/stretchr/testify/require"
	"github.com/yugabyte/gocql"
	"github.com/yugabyte/pgx/v4/pgxpool"
	"golang.org/x/net/context"
	"testing"
	"time"
)

var tlog = logging.Logger("ybtest")

var TestYugabyteSettings = yugabyte.DBSettings{
	Hosts:         []string{"yugabyte"},
	ConnectString: "postgresql://postgres:postgres@yugabyte:5433",
}

// Use when testing against a local yugabyte instance.
// Warning: This will delete all tables in the local yugabyte instance.
var TestYugabyteSettingsLocal = yugabyte.DBSettings{
	Hosts:         []string{"localhost"},
	ConnectString: "postgresql://postgres:postgres@localhost:5433",
}

func SetupYugabyte(t *testing.T) {
	ctx := context.Background()

	tlog.Info("wait for yugabyte start...")
	awaitYugabyteUp(t, time.Minute)
	tlog.Info("yugabyte started")

	store := yugabyte.NewStore(TestYugabyteSettings)
	err := store.Start(ctx)
	require.NoError(t, err)

	RecreateTables(ctx, t, store)
}

func RecreateTables(ctx context.Context, t *testing.T, store *yugabyte.Store) {
	err := store.Drop(ctx)
	require.NoError(t, err)
	err = store.Create(ctx)
	require.NoError(t, err)
}

func awaitYugabyteUp(t *testing.T, duration time.Duration) {
	start := time.Now()
	cluster := gocql.NewCluster(TestYugabyteSettings.Hosts[0])
	for {
		_, err := cluster.CreateSession()
		if err == nil {
			_, err = pgxpool.Connect(context.Background(), TestYugabyteSettings.ConnectString)
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
