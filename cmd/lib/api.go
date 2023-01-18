package lib

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/filecoin-project/boost/api"
	cliutil "github.com/filecoin-project/boost/cli/util"
	"github.com/filecoin-project/boost/markets/dagstore"
	"github.com/filecoin-project/boost/markets/sectoraccessor"
	"github.com/filecoin-project/go-jsonrpc"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/api/v0api"
	"github.com/filecoin-project/lotus/api/v1api"
	lotus_modules "github.com/filecoin-project/lotus/node/modules"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/node/repo"
	"github.com/filecoin-project/lotus/storage/paths"
	"github.com/filecoin-project/lotus/storage/sealer"
	logging "github.com/ipfs/go-log/v2"
)

func GetFullNodeApi(ctx context.Context, ai string, log *logging.ZapEventLogger) (v1api.FullNode, jsonrpc.ClientCloser, error) {
	ai = strings.TrimPrefix(strings.TrimSpace(ai), "FULLNODE_API_INFO=")
	info := cliutil.ParseApiInfo(ai)
	addr, err := info.DialArgs("v1")
	if err != nil {
		return nil, nil, fmt.Errorf("could not get DialArgs: %w", err)
	}

	log.Infof("Using full node API at %s", addr)
	fnapi, closer, err := client.NewFullNodeRPCV1(ctx, addr, info.AuthHeader())
	if err != nil {
		return nil, nil, fmt.Errorf("creating full node service API: %w", err)
	}

	v, err := fnapi.Version(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("checking full node service API version: %w", err)
	}

	if !v.APIVersion.EqMajorMinor(lapi.FullAPIVersion1) {
		return nil, nil, fmt.Errorf("full node service API version didn't match (expected %s, remote %s)", api.FullAPIVersion1, v.APIVersion)
	}

	return fnapi, closer, nil
}

func GetMinerApi(ctx context.Context, ai string, log *logging.ZapEventLogger) (v0api.StorageMiner, jsonrpc.ClientCloser, error) {
	ai = strings.TrimPrefix(strings.TrimSpace(ai), "MINER_API_INFO=")
	info := cliutil.ParseApiInfo(ai)
	addr, err := info.DialArgs("v0")
	if err != nil {
		return nil, nil, fmt.Errorf("could not get DialArgs: %w", err)
	}

	log.Infof("Using storage API at %s", addr)
	api, closer, err := client.NewStorageMinerRPCV0(ctx, addr, info.AuthHeader())
	if err != nil {
		return nil, nil, fmt.Errorf("creating miner service API: %w", err)
	}

	v, err := api.Version(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("checking miner service API version: %w", err)
	}

	if !v.APIVersion.EqMajorMinor(lapi.MinerAPIVersion0) {
		return nil, nil, fmt.Errorf("miner service API version didn't match (expected %s, remote %s)", lapi.MinerAPIVersion0, v.APIVersion)
	}

	return api, closer, nil
}

func StorageAuthWithURL(apiInfo string) (sealer.StorageAuth, error) {
	s := strings.Split(apiInfo, ":")
	if len(s) != 2 {
		return nil, errors.New("unexpected format of `apiInfo`")
	}
	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+s[0])
	return sealer.StorageAuth(headers), nil
}

func CreateSectorAccessor(ctx context.Context, storageApiInfo string, fullnodeApi v1api.FullNode, log *logging.ZapEventLogger) (dagstore.SectorAccessor, jsonrpc.ClientCloser, error) {
	sauth, err := StorageAuthWithURL(storageApiInfo)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing storage API endpoint: %w", err)
	}
	storageService, storageCloser, err := GetMinerApi(ctx, storageApiInfo, log)
	if err != nil {
		return nil, nil, fmt.Errorf("getting miner API: %w", err)
	}

	maddr, err := storageService.ActorAddress(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("getting miner actor address: %w", err)
	}
	log.Infof("Miner address: %s", maddr)

	// Use an in-memory repo because we don't need any functions
	// of a real repo, we just need to supply something that satisfies
	// the LocalStorage interface to the store
	memRepo := repo.NewMemory(nil)

	// passing FullNode, so that we don't pass StorageMiner or Worker and
	// skip initializing of sectorstore.json with random local storage ID
	lr, err := memRepo.Lock(repo.FullNode)
	if err != nil {
		return nil, nil, fmt.Errorf("locking mem repo: %w", err)
	}
	defer lr.Close()

	if err := lr.SetStorage(func(sc *paths.StorageConfig) {
		sc.StoragePaths = []paths.LocalPath{}
	}); err != nil {
		return nil, nil, fmt.Errorf("set storage config: %w", err)
	}

	// Create the store interface
	var urls []string
	lstor, err := paths.NewLocal(ctx, lr, storageService, urls)
	if err != nil {
		return nil, nil, fmt.Errorf("creating new local store: %w", err)
	}
	storage := lotus_modules.RemoteStorage(lstor, storageService, sauth, sealer.Config{
		// TODO: Not sure if I need this, or any of the other fields in this struct
		ParallelFetchLimit: 1,
	})

	// Create the piece provider
	pp := sealer.NewPieceProvider(storage, storageService, storageService)
	sa := sectoraccessor.NewSectorAccessor(dtypes.MinerAddress(maddr), storageService, pp, fullnodeApi)
	return sa, storageCloser, nil
}
