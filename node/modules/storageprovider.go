package modules

import (
	"os"
	"path/filepath"
	"strings"

	mdagstore "github.com/filecoin-project/boost/node/modules/dagstore"
	"github.com/filecoin-project/go-address"
	piecefilestore "github.com/filecoin-project/go-fil-markets/filestore"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	storageimpl "github.com/filecoin-project/go-fil-markets/storagemarket/impl"
	"github.com/filecoin-project/go-fil-markets/storagemarket/impl/storedask"
	smnet "github.com/filecoin-project/go-fil-markets/storagemarket/network"
	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/lotus/markets/idxprov"
	lotus_modules "github.com/filecoin-project/lotus/node/modules"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/libp2p/go-libp2p-core/host"
	"golang.org/x/xerrors"
)

func StorageProvider(minerAddress lotus_dtypes.MinerAddress,
	storedAsk *storedask.StoredAsk,
	h host.Host, ds lotus_dtypes.MetadataDS,
	r lotus_repo.LockedRepo,
	pieceStore lotus_dtypes.ProviderPieceStore,
	indexer provider.Interface,
	dataTransfer lotus_dtypes.ProviderDataTransfer,
	spn storagemarket.StorageProviderNode,
	df lotus_dtypes.StorageDealFilter,
	dsw *mdagstore.Wrapper,
	meshCreator idxprov.MeshCreator,
) (storagemarket.StorageProvider, error) {
	net := smnet.NewFromLibp2pHost(h)

	dir := filepath.Join(r.Path(), lotus_modules.StagingAreaDirName)

	// migrate temporary files that were created directly under the repo, by
	// moving them to the new directory and symlinking them.
	oldDir := r.Path()
	if err := migrateDealStaging(oldDir, dir); err != nil {
		return nil, xerrors.Errorf("failed to make deal staging directory %w", err)
	}

	store, err := piecefilestore.NewLocalFileStore(piecefilestore.OsPath(dir))
	if err != nil {
		return nil, err
	}

	opt := storageimpl.CustomDealDecisionLogic(storageimpl.DealDeciderFunc(df))

	return storageimpl.NewProvider(
		net,
		namespace.Wrap(ds, datastore.NewKey("/deals/provider")),
		store,
		dsw,
		indexer,
		pieceStore,
		dataTransfer,
		spn,
		address.Address(minerAddress),
		storedAsk,
		meshCreator,
		opt,
	)
}

func migrateDealStaging(oldPath, newPath string) error {
	dirInfo, err := os.Stat(newPath)
	if err == nil {
		if !dirInfo.IsDir() {
			return xerrors.Errorf("%s is not a directory", newPath)
		}
		// The newPath exists already, below migration has already occurred.
		return nil
	}

	// if the directory doesn't exist, create it
	if os.IsNotExist(err) {
		if err := os.MkdirAll(newPath, 0755); err != nil {
			return xerrors.Errorf("failed to mk directory %s for deal staging: %w", newPath, err)
		}
	} else { // if we failed for other reasons, abort.
		return err
	}

	// if this is the first time we created the directory, symlink all staged deals into it. "Migration"
	// get a list of files in the miner repo
	dirEntries, err := os.ReadDir(oldPath)
	if err != nil {
		return xerrors.Errorf("failed to list directory %s for deal staging: %w", oldPath, err)
	}

	for _, entry := range dirEntries {
		// ignore directories, they are not the deals.
		if entry.IsDir() {
			continue
		}
		// the FileStore from fil-storage-market creates temporary staged deal files with the pattern "fstmp"
		// https://github.com/filecoin-project/go-fil-markets/blob/00ff81e477d846ac0cb58a0c7d1c2e9afb5ee1db/filestore/filestore.go#L69
		name := entry.Name()
		if strings.Contains(name, "fstmp") {
			// from the miner repo
			oldPath := filepath.Join(oldPath, name)
			// to its subdir "deal-staging"
			newPath := filepath.Join(newPath, name)
			// create a symbolic link in the new deal staging directory to preserve existing staged deals.
			// all future staged deals will be created here.
			if err := os.Rename(oldPath, newPath); err != nil {
				return xerrors.Errorf("failed to move %s to %s: %w", oldPath, newPath, err)
			}
			if err := os.Symlink(newPath, oldPath); err != nil {
				return xerrors.Errorf("failed to symlink %s to %s: %w", oldPath, newPath, err)
			}
			log.Infow("symlinked staged deal", "from", oldPath, "to", newPath)
		}
	}
	return nil
}
