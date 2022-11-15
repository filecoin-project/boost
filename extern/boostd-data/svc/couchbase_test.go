package svc

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	cl "github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/filecoin-project/boostd-data/client"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

func TestCouchbaseService(t *testing.T) {
	removeContainer := setupCouchbase(t)
	defer removeContainer()

	bdsvc := NewCouchbase(testCouchSettings)
	err := bdsvc.Start(context.Background(), 8042)
	if err != nil {
		t.Fatal(err)
	}

	cl := client.NewStore()
	err = cl.Dial(context.Background(), "http://localhost:8042")
	defer cl.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	pieceCid, err := cid.Parse("baga6ea4seaqj2j4zfi2xk7okc7fnuw42pip6vjv2tnc4ojsbzlt3rfrdroa7qly")
	if err != nil {
		t.Fatal(err)
	}
	dealInfo := model.DealInfo{}
	err = cl.AddDealForPiece(context.TODO(), pieceCid, dealInfo)
	if err != nil {
		t.Fatal(err)
	}

	log.Debug("sleeping for a while.. running tests..")
	time.Sleep(2 * time.Second)
}

func setupCouchbase(t *testing.T) func() {
	ctx := context.Background()
	cli, err := cl.NewEnvClient()
	if err != nil {
		t.Fatal(err)
	}

	imageName := "couchbase"

	out, err := cli.ImagePull(ctx, imageName, types.ImagePullOptions{})
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(os.Stdout, out)

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

	if err != nil {
		t.Fatal(err)
	}

	if err := cli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Duration(10) * time.Second)

	err = initializeCouchbaseCluster()
	require.NoError(t, err)

	// Wait for couchbase services to start
	time.Sleep(time.Duration(10) * time.Second)

	cleanup := func() {
		err := cli.ContainerRemove(ctx, resp.ID, types.ContainerRemoveOptions{Force: true})
		if err != nil {
			t.Fatal(err)
		}
	}
	return cleanup
}

func initializeCouchbaseCluster() error {
	setupCommand := "curl -X POST http://127.0.0.1:8091/nodes/self/controller/settings -d 'data_path=%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fdata&' -d 'index_path=%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fidata&' -d 'cbas_path=%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fadata&' -d 'eventing_path=%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fedata&'"
	renameNodeCommand := "curl -u Administrator:password -X POST http://127.0.01:8091/node/controller/rename -d hostname=127.0.0.1"
	startServicesCommand := "curl -u 'Administrator:password' -X POST http://127.0.0.1:8091/node/controller/setupServices -d services=kv%2Cn1ql%2Cindex%2Ceventing"
	setBucketMemoryQuotaCommand := "curl -u 'Administrator:password' -X POST http://127.0.0.1:8091/pools/default -d memoryQuota=512 -d indexMemoryQuota=512 -d ftsMemoryQuota=512"
	createAdminUserCommand := "curl -u 'Administrator:password' -X POST http://127.0.0.1:8091/settings/web -d port=8091 -d username=Administrator -d password=boostdemo"
	setIndexStorageModeCommand := "curl -u 'Administrator:boostdemo' -X POST http://127.0.0.1:8091/settings/indexes -d storageMode=plasma"
	var commands []string
	commands = append(commands, setupCommand, renameNodeCommand, startServicesCommand, setBucketMemoryQuotaCommand, createAdminUserCommand, setIndexStorageModeCommand)
	for _, v := range commands {
		output, err := exec.Command("/bin/bash", "-c", v).CombinedOutput()
		if err != nil {
			return err
			fmt.Println(string(output))
		}
	}
	// Sleep to give enough time for service to start or bucket creation will fail
	time.Sleep(time.Duration(10) * time.Second)
	return nil
}
