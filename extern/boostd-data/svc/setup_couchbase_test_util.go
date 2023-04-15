package svc

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/davecgh/go-spew/spew"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	dockercl "github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/filecoin-project/boostd-data/couchbase"
	logging "github.com/ipfs/go-log/v2"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

var tlog = logging.Logger("cbtest")

func init() {
	err := logging.SetLogLevel("cbtest", "debug")
	if err != nil {
		panic(err)
	}
}

func SetupCouchbase(t *testing.T, dbSettings couchbase.DBSettings) {
	ctx := context.Background()
	cli, err := dockercl.NewClientWithOpts(dockercl.FromEnv)
	require.NoError(t, err)

	imageName := "couchbase"
	out, err := cli.ImagePull(ctx, imageName, types.ImagePullOptions{})
	require.NoError(t, err)

	_, err = io.Copy(os.Stdout, out)
	require.NoError(t, err)

	tlog.Info("couchbase docker container create...")
	resp, err := cli.ContainerCreate(ctx, &container.Config{
		Image: imageName,
		ExposedPorts: nat.PortSet{
			"8091":  struct{}{},
			"8092":  struct{}{},
			"8093":  struct{}{},
			"8094":  struct{}{},
			"8095":  struct{}{},
			"8096":  struct{}{},
			"11210": struct{}{},
			"11211": struct{}{},
		},
	}, &container.HostConfig{
		PortBindings: map[nat.Port][]nat.PortBinding{
			nat.Port("8091"):  {{HostIP: "127.0.0.1", HostPort: "8091"}},
			nat.Port("8092"):  {{HostIP: "127.0.0.1", HostPort: "8092"}},
			nat.Port("8093"):  {{HostIP: "127.0.0.1", HostPort: "8093"}},
			nat.Port("8094"):  {{HostIP: "127.0.0.1", HostPort: "8094"}},
			nat.Port("8095"):  {{HostIP: "127.0.0.1", HostPort: "8095"}},
			nat.Port("8096"):  {{HostIP: "127.0.0.1", HostPort: "8096"}},
			nat.Port("11210"): {{HostIP: "127.0.0.1", HostPort: "11210"}},
			nat.Port("11211"): {{HostIP: "127.0.0.1", HostPort: "11211"}},
		},
	}, nil, nil, "")
	require.NoError(t, err)
	tlog.Info("couchbase docker container created")

	tlog.Info("couchbase docker container start...")
	err = cli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{})
	require.NoError(t, err)
	tlog.Info("couchbase docker container started")

	inspect, err := cli.ContainerInspect(ctx, resp.ID)
	require.NoError(t, err)
	spew.Dump(inspect)

	t.Cleanup(func() {
		tlog.Info("couchbase docker container remove...")
		err := cli.ContainerRemove(ctx, resp.ID, types.ContainerRemoveOptions{Force: true})
		require.NoError(t, err)
		tlog.Info("couchbase docker container removed")
	})

	tlog.Info("wait for couchbase start...")
	awaitCouchbaseUp(t, 10*time.Minute)
	tlog.Info("couchbase started")

	tlog.Info("couchbase initialize cluster...")
	err = initializeCouchbaseCluster(t, dbSettings)
	require.NoError(t, err)
	tlog.Info("couchbase initialized cluster")
}

func initializeCouchbaseCluster(t *testing.T, settings couchbase.DBSettings) error {
	couchDir := "/opt/couchbase/var/lib/couchbase"
	apiCall(t, "/nodes/self/controller/settings", url.Values{
		"data_path":     {couchDir + "/data"},
		"index_path":    {couchDir + "/idata"},
		"cbas_path":     {couchDir + "/adata"},
		"eventing_path": {couchDir + "e/edata"},
	})

	apiCall(t, "/node/controller/rename", url.Values{
		"hostname": {"127.0.0.1"},
	})

	apiCall(t, "/node/controller/setupServices", url.Values{
		"services": {"kv,n1ql,index"},
	})

	apiCall(t, "/pools/default", url.Values{
		"memoryQuota":      {"1024"},
		"indexMemoryQuota": {"512"},
		"ftsMemoryQuota":   {"512"},
	})

	apiCall(t, "/settings/indexes", url.Values{
		"storageMode": {"plasma"},
	})

	tlog.Info("wait for services start...")
	awaitServicesReady(t, settings, time.Minute)
	tlog.Info("services started")

	apiCall(t, "/settings/web", url.Values{
		"port":     {"8091"},
		"username": {settings.Auth.Username},
		"password": {settings.Auth.Password},
	})

	tlog.Info("wait for bucket creation and indexing...")
	awaitBucketCreationReady(t, settings, 5*time.Minute)
	tlog.Info("bucket creation and indexing started")

	return nil
}

func awaitServicesReady(t *testing.T, settings couchbase.DBSettings, duration time.Duration) {
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
				tlog.Info("    " + couchbase.ServiceName(endpoint.Type) + ": " + couchbase.EndpointStateName(endpoint.State))
				if endpoint.State != gocb.EndpointStateConnected {
					allOnline = false
				}
			}
		}

		if allOnline {
			return
		}

		if time.Since(start) > duration {
			require.Fail(t, "timed out waiting for couchbase services to come up after "+duration.String())
		}
		time.Sleep(time.Second)
	}
}

func awaitBucketCreationReady(t *testing.T, settings couchbase.DBSettings, duration time.Duration) {
	start := time.Now()
	cluster, err := gocb.Connect(settings.ConnectString, gocb.ClusterOptions{
		TimeoutsConfig: gocb.TimeoutsConfig{
			ConnectTimeout: time.Minute,
		},
		Authenticator: gocb.PasswordAuthenticator{
			Username: settings.Auth.Username,
			Password: settings.Auth.Password,
		},
	})
	require.NoError(t, err)

	// Repeatedly try to create a dummy bucket as a way to ensure that the
	// services needed for bucket creation and indexing are up
	for {
		// It may take several seconds to create the index, so we want to use
		// a relatively long timeout here
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := couchbase.CreateBucket(ctx, cluster, "dummy", 128)
		if err == nil {
			return
		}

		if time.Since(start) > duration {
			require.Fail(t, "timed out trying to create dummy bucket after "+duration.String())
		}

		tlog.Infow("got err creating dummy bucket - probably services are not all up yet, retrying", "err", err.Error())
		time.Sleep(time.Second)
	}
}

func awaitCouchbaseUp(t *testing.T, duration time.Duration) {
	waitForOkResponse(t, "/internalSettings", duration)
}

func waitForOkResponse(t *testing.T, path string, duration time.Duration) {
	start := time.Now()
	for {
		fullPath := "http://127.0.0.1:8091" + path
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

		if time.Since(start) > duration {
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

func apiCall(t *testing.T, path string, values url.Values) {
	fullPath := "http://127.0.0.1:8091" + path
	resp, err := http.PostForm(fullPath, values)
	require.NoError(t, err)
	if resp.StatusCode >= 300 {
		require.Fail(t, fmt.Sprintf("%s: %d %s", fullPath, resp.StatusCode, resp.Status))
	}
	err = resp.Body.Close()
	require.NoError(t, err)
}
