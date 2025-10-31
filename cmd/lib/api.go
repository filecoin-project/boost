package lib

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/filecoin-project/boost/api"
	cliutil "github.com/filecoin-project/boost/cli/util"
	"github.com/filecoin-project/boost/lib/sa"
	"github.com/filecoin-project/boost/markets/sectoraccessor"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/boost/piecedirectory/types"
	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/api/v0api"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/node/config"
	lotus_modules "github.com/filecoin-project/lotus/node/modules"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/node/repo"
	"github.com/filecoin-project/lotus/storage/paths"
	"github.com/filecoin-project/lotus/storage/sealer"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
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

	return fnapi, closer, nil
}

func CheckFullNodeApiVersion(ctx context.Context, fnapi v1api.FullNode) error {
	v, err := fnapi.Version(ctx)
	if err != nil {
		return fmt.Errorf("checking full node service API version: %w", err)
	}

	if !v.APIVersion.EqMajorMinor(lapi.FullAPIVersion1) {
		return fmt.Errorf("full node service API version didn't match (expected %s, remote %s)", api.FullAPIVersion1, v.APIVersion)
	}

	return nil
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
	s := strings.SplitN(apiInfo, ":", 2)
	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+s[0])
	return sealer.StorageAuth(headers), nil
}

type MultiMinerAccessor struct {
	storageApiInfos []string
	fullnodeApi     v1api.FullNode
	readers         map[address.Address]types.PieceReader
	sas             map[address.Address]sa.SectorAccessor
	closeOnce       sync.Once
	closers         []jsonrpc.ClientCloser
	cachingDuration time.Duration
}

func NewMultiMinerAccessor(storageApiInfos []string, fullnodeApi v1api.FullNode, cacheTTL time.Duration) *MultiMinerAccessor {
	return &MultiMinerAccessor{
		storageApiInfos: storageApiInfos,
		fullnodeApi:     fullnodeApi,
		cachingDuration: cacheTTL,
	}
}

func (a *MultiMinerAccessor) Start(ctx context.Context, log *logging.ZapEventLogger) error {
	a.sas = make(map[address.Address]sa.SectorAccessor, len(a.storageApiInfos))
	a.readers = make(map[address.Address]types.PieceReader, len(a.storageApiInfos))
	a.closers = make([]jsonrpc.ClientCloser, 0, len(a.storageApiInfos))
	for _, apiInfo := range a.storageApiInfos {
		sa, closer, err := CreateSectorAccessor(ctx, apiInfo, a.fullnodeApi, log)
		if err != nil {
			a.Close()
			return fmt.Errorf("creating sector accessor for endpoint '%s': %w", apiInfo, err)
		}
		a.sas[sa.maddr] = sa
		a.readers[sa.maddr] = &piecedirectory.SectorAccessorAsPieceReader{SectorAccessor: sa}
		a.closers = append(a.closers, closer)
	}
	return nil
}

func (a *MultiMinerAccessor) Close() {
	a.closeOnce.Do(func() {
		for _, c := range a.closers {
			c()
		}
	})
}

func (a *MultiMinerAccessor) GetMinerAddresses() []address.Address {
	addrs := make([]address.Address, 0, len(a.readers))
	for a := range a.readers {
		addrs = append(addrs, a)
	}
	return addrs
}

func (a *MultiMinerAccessor) GetReader(ctx context.Context, minerAddr address.Address, id abi.SectorNumber, offset abi.PaddedPieceSize, length abi.PaddedPieceSize) (types.SectionReader, error) {
	pr, ok := a.readers[minerAddr]
	if !ok {
		return nil, fmt.Errorf("get reader: no endpoint registered for miner %s, len(readers)=%d", minerAddr, len(a.readers))
	}
	return pr.GetReader(ctx, minerAddr, id, offset, length)
}

func (a *MultiMinerAccessor) UnsealSectorAt(ctx context.Context, minerAddr address.Address, sectorID abi.SectorNumber, pieceOffset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (mount.Reader, error) {
	sa, ok := a.sas[minerAddr]
	if !ok {
		return nil, fmt.Errorf("read sector: no endpoint registered for miner %s, len(readers)=%d", minerAddr, len(a.readers))
	}
	return sa.UnsealSectorAt(ctx, sectorID, pieceOffset, length)
}

func (a *MultiMinerAccessor) IsUnsealed(ctx context.Context, minerAddr address.Address, sectorID abi.SectorNumber, offset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (bool, error) {
	sa, ok := a.sas[minerAddr]
	if !ok {
		return false, fmt.Errorf("is unsealed: no endpoint registered for miner %s, len(readers)=%d", minerAddr, len(a.readers))
	}
	return sa.IsUnsealed(ctx, sectorID, offset, length)
}

type sectorAccessor struct {
	sa.SectorAccessor
	maddr address.Address
}

func CreateSectorAccessor(ctx context.Context, storageApiInfo string, fullnodeApi v1api.FullNode, log *logging.ZapEventLogger) (*sectorAccessor, jsonrpc.ClientCloser, error) {
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
	defer func() {
		_ = lr.Close()
	}()

	if err := lr.SetStorage(func(sc *storiface.StorageConfig) {
		sc.StoragePaths = []storiface.LocalPath{}
	}); err != nil {
		return nil, nil, fmt.Errorf("set storage config: %w", err)
	}

	// Create the store interface
	var urls []string
	lstor, err := paths.NewLocal(ctx, lr, storageService, urls)
	if err != nil {
		return nil, nil, fmt.Errorf("creating new local store: %w", err)
	}
	storage := lotus_modules.RemoteStorage(lstor, storageService, sauth, config.SealerConfig{
		// TODO: Not sure if I need this, or any of the other fields in this struct
		ParallelFetchLimit: 1,
	})

	// Create the piece provider
	pp := sealer.NewPieceProvider(storage, storageService, storageService)
	const maxCacheSize = 4096
	newSectorAccessor := sectoraccessor.NewCachingSectorAccessor(maxCacheSize, 10*time.Second)
	sa := newSectorAccessor(dtypes.MinerAddress(maddr), storageService, pp, fullnodeApi)
	return &sectorAccessor{SectorAccessor: sa, maddr: maddr}, storageCloser, nil
}
