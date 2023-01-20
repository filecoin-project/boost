package couchbase

import (
	"fmt"
	"io"
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
	ctx, cf := context.WithTimeout(context.TODO(), 10*time.Minute)
	defer cf()

	tlog.Info("wait for couchbase to start...")
	// 2 min should be enough when running for CI
	// when running from ci it is expected the couchbase image to be downloaded before
	couchbaseStartDeadline := 2 * time.Minute
	// try fail fast if running from local dev machine
	if getTestHost() == "localhost" {
		couchbaseStartDeadline = 20 * time.Second
	}
	tlog.Infof("giving %s to start.", couchbaseStartDeadline)
	ctxInt, cfInt := context.WithTimeout(ctx, couchbaseStartDeadline)
	defer cfInt()

	if err := awaitCouchbaseUp(ctxInt, dbSettings); err != nil {
		if getTestHost() == "localhost" {
			require.NoError(t, err, "Do you run from dev environment? Do not forget to init couchbase with `docker compose up couchbase`")
		}
		require.NoError(t, err)
	}
	tlog.Info("couchbase has started")


	// test if couchbase bucket was initialized before
	tlog.Info("check if couchbase cluster ready...")
	cluster, err := newTestCluster(dbSettings)
	require.NoError(t, err)

	ctxInt, cfInt2 := context.WithTimeout(ctx, 30*time.Second)
	defer cfInt2()
	if err := checkClusterInitialized(ctxInt, cluster); err == nil {
		tlog.Info("yes, let's go")
		return
	}

	// do full setup
	tlog.Info("couchbase initialize cluster...")
	initDeadline := 5 * time.Minute
	tlog.Infof("give %s to be ready.", initDeadline)
	ctxInt, cfInt3 := context.WithTimeout(ctx, initDeadline)
	defer cfInt3()
	err = initializeCouchbaseCluster(t, ctxInt, dbSettings)
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

// ckeck for dummy bucket was created before
func checkClusterInitialized(ctx context.Context, cluster *gocb.Cluster) error {
	// give big timeout, as it is also managed by external context ctx
	return cluster.Bucket("dummy").WaitUntilReady(20*time.Minute, 
		&gocb.WaitUntilReadyOptions{DesiredState: gocb.ClusterStateOnline, Context: ctx})
}

func awaitCouchbaseUp(ctx context.Context, dbSettings DBSettings) error {
	return waitForOkResponse(ctx, dbSettings, "/internalSettings")
}

func waitForOkResponse(ctx context.Context, dbSettings DBSettings, path string) error {
	url, err := getTestURL(dbSettings.ConnectString)
	if err != nil {
		return err
	}
	fullPath := url + path
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullPath, nil)
		if err != nil {
			return err
		}
		req.SetBasicAuth(dbSettings.Auth.Username, dbSettings.Auth.Password)
		resp, err := http.DefaultClient.Do(req)
		if resp != nil && resp.Body != nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
		if err == nil && resp != nil {
			if resp.StatusCode < 300 {
				return nil
			} else if resp.StatusCode >= 400 && resp.StatusCode < 500 {
				return fmt.Errorf("failed to GET %s: %d - %s", fullPath, resp.StatusCode, resp.Status)
			}
		}
		select {
		case <-ctx.Done():
			if resp != nil {
				return fmt.Errorf("timed out trying to wait for couchbase online: %d - %s: %v", resp.StatusCode, resp.Status, err)
			}
			return fmt.Errorf("timed out trying to wait for couchbase online: %v", err)
		case <-time.After(time.Second):
		}
	}
}

func apiCall(t *testing.T, dbSettings DBSettings, path string, values url.Values) {
	t.Helper()

	url, err := getTestURL(dbSettings.ConnectString)
	require.NoError(t, err)
	fullPath := url + path
	resp, err := http.PostForm(fullPath, values)
	require.NoError(t, err)
	if resp.StatusCode >= 300 {
		require.Fail(t, fmt.Sprintf("%s: %d %s", fullPath, resp.StatusCode, resp.Status))
	}
	err = resp.Body.Close()
	require.NoError(t, err)
}

// GetConnectionStringForTest get couchbase test instance connection string from env COUCHBASE_HOST
// by default use localhost
func GetConnectionStringForTest() string {
	return fmt.Sprintf("couchbase://%s", getTestHost())
}

func getTestHost() string {
	res := os.Getenv("COUCHBASE_HOST")
	if res == "" {
		res = "localhost"
	}
	return res
}

func getTestURL(cs string) (string, error) {
	res, err := url.Parse(cs)
	if (err) != nil {
		return "", err
	}
	return fmt.Sprintf("http://%s:8091", res.Host), nil
}
