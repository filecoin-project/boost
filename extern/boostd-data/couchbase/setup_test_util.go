package couchbase

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	logging "github.com/ipfs/go-log/v2"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

var tlog = logging.Logger("cbtest")

func init() {
	logging.SetLogLevel("cbtest", "debug")
}

func SetupTestServer(t *testing.T, dbSettings DBSettings) {
	// first test if couchbase bucket are ready, maybe already service initialized
	tlog.Info("check if couchbase ready...")
	cluster, err := newTestCluster(dbSettings)
	require.NoError(t, err)

	ctx, cf := context.WithTimeout(context.TODO(), 20*time.Second)
	defer cf()
	if err := checkBucketReady(ctx, cluster); err == nil {
		tlog.Info("yes, let's go")
		return
	}

	// otherwise do full setup
	tlog.Info("wait for couchbase start...")
	awaitCouchbaseUp(t, dbSettings, 10*time.Minute)
	tlog.Info("couchbase started")

	tlog.Info("couchbase initialize cluster...")
	initDeadline := 5 * time.Minute
	tlog.Infof("give %s to be ready.", initDeadline)
	ctx, cf2 := context.WithTimeout(context.TODO(), initDeadline)
	defer cf2()
	err = initializeCouchbaseCluster(t, ctx, dbSettings)
	require.NoError(t, err)
	tlog.Info("couchbase initialized cluster")
}

func initializeCouchbaseCluster(t *testing.T, ctx context.Context, settings DBSettings) error {
	couchDir := "/opt/couchbase/var/lib/couchbase"
	apiCall(t, settings, "/nodes/self/controller/settings", url.Values{
		"data_path":     {couchDir + "/data"},
		"index_path":    {couchDir + "/idata"},
		"cbas_path":     {couchDir + "/adata"},
		"eventing_path": {couchDir + "e/edata"},
	})

	apiCall(t, settings, "/node/controller/rename", url.Values{
		"hostname": {"127.0.0.1"},
	})

	apiCall(t, settings, "/node/controller/setupServices", url.Values{
		"services": {"kv,n1ql,index"},
	})

	apiCall(t, settings, "/pools/default", url.Values{
		"memoryQuota":      {"1024"},
		"indexMemoryQuota": {"512"},
		"ftsMemoryQuota":   {"512"},
	})

	apiCall(t, settings, "/settings/indexes", url.Values{
		"storageMode": {"plasma"},
	})

	tlog.Info("wait for services start...")
	awaitServicesReady(t, settings, time.Minute)
	tlog.Info("services started")

	apiCall(t, settings, "/settings/web", url.Values{
		"port":     {"8091"},
		"username": {settings.Auth.Username},
		"password": {settings.Auth.Password},
	})

	tlog.Info("wait for bucket creation and indexing...")
	awaitBucketCreationReady(t, ctx, settings)
	tlog.Info("bucket creation and indexing started")

	return nil
}

func awaitServicesReady(t *testing.T, settings DBSettings, duration time.Duration) {
	start := time.Now()
	cluster, err := gocb.Connect(settings.ConnectString, gocb.ClusterOptions{
		TimeoutsConfig: gocb.TimeoutsConfig{
			ConnectTimeout: duration,
		},
	})
	require.NoError(t, err)

	var res *gocb.DiagnosticsResult
	for res == nil || res.State != gocb.ClusterStateOnline {
		res, err := cluster.Diagnostics(nil)
		require.NoError(t, err)

		allOnline := true
		tlog.Info("Services")
		for name, svc := range res.Services {
			tlog.Info("  " + name + ":")
			for _, endpoint := range svc {
				tlog.Info("    " + ServiceName(endpoint.Type) + ": " + EndpointStateName(endpoint.State))
				if endpoint.State != gocb.EndpointStateConnected {
					allOnline = false
				}
			}
		}

		if allOnline {
			return
		}

		if time.Now().Sub(start) > duration {
			require.Fail(t, "timed out waiting for couchbase services to come up after "+duration.String())
		}
		time.Sleep(time.Second)
	}
}

func awaitBucketCreationReady(t *testing.T, ctx context.Context, settings DBSettings) {
	cluster, err := newTestCluster(settings)
	require.NoError(t, err)

	// Repeatedly try to create a dummy bucket as a way to ensure that the
	// services needed for bucket creation and indexing are up
	for {
		// It may take several seconds to create the index, so we want to use
		// a relatively long timeout here
		ctxInt, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		_, err := CreateBucket(ctxInt, cluster, "dummy", 128)
		if err == nil {
			return
		}
		select {
		case <-ctx.Done():
			require.Fail(t, "timed out trying to create dummy bucket")
		case <-time.After(time.Second):
			tlog.Infow("got err creating dummy bucket - probably services are not all up yet, retrying", "err", err.Error())
		}
	}
}

func newTestCluster(settings DBSettings) (*gocb.Cluster, error) {
	return gocb.Connect(settings.ConnectString, gocb.ClusterOptions{
		TimeoutsConfig: gocb.TimeoutsConfig{
			ConnectTimeout: time.Minute,
		},
		Authenticator: gocb.PasswordAuthenticator{
			Username: settings.Auth.Username,
			Password: settings.Auth.Password,
		},
	})
}

func checkBucketReady(ctx context.Context, cluster *gocb.Cluster) error {
	if _, err := CreateBucket(ctx, cluster, "dummy", 128); err != nil {
		return err
	}
	return nil
}

func awaitCouchbaseUp(t *testing.T, dbSettings DBSettings, duration time.Duration) {
	waitForOkResponse(t, dbSettings, "/internalSettings", duration)
}

func waitForOkResponse(t *testing.T, dbSettings DBSettings, path string, duration time.Duration) {
	start := time.Now()
	for {
		fullPath := getTestURL(t, dbSettings.ConnectString) + path
		resp, err := http.Get(fullPath)
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		if err == nil && resp != nil {
			if resp.StatusCode < 300 {
				return
			} else if resp.StatusCode >= 400 && resp.StatusCode < 500 {
				msg := fmt.Sprintf("failed to GET %s: %d - %s", fullPath, resp.StatusCode, resp.Status)
				require.Fail(t, msg)
			}
		}

		if time.Now().Sub(start) > duration {
			msg := "failed to GET " + fullPath + " after " + duration.String()
			if resp != nil {
				msg += fmt.Sprintf(": %d - %s", resp.StatusCode, resp.Status)
			}
			if err != nil {
				msg += ": " + err.Error()
			}
			require.Fail(t, msg)
		}
		time.Sleep(time.Second)
	}
}

func apiCall(t *testing.T, dbSettings DBSettings, path string, values url.Values) {
	t.Helper()

	fullPath := getTestURL(t, dbSettings.ConnectString) + path
	resp, err := http.PostForm(fullPath, values)
	require.NoError(t, err)
	if resp.StatusCode >= 300 {
		require.Fail(t, fmt.Sprintf("%s: %d %s", fullPath, resp.StatusCode, resp.Status))
	}
	err = resp.Body.Close()
	require.NoError(t, err)
}

// GetConnectionStringForTest get couchbase test instance connection string from env COUCHBASE_HOST
// if not set use localhost
func GetConnectionStringForTest() string {
	en := "COUCHBASE_HOST"
	res := os.Getenv(en)
	if res == "" {
		res = "localhost"
	}
	return fmt.Sprintf("couchbase://%s", res)
}

func getTestURL(t *testing.T, cs string) string {
	t.Helper()
	res, err := url.Parse(cs)
	require.NoError(t, err)
	return fmt.Sprintf("http://%s:8091", res.Host)
}
