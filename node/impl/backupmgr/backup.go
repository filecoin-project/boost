package backupmgr

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	boostdb "github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/node/repo"
	"github.com/filecoin-project/lotus/lib/backupds"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	"github.com/ipfs/go-datastore"
	"github.com/mitchellh/go-homedir"
)

const MetadataFileName = "metadata"
const ConfigDirName = "config"

var BkpFileList = []string{"config.toml",
	"storage.json",
	"token"}

var RestoreFileChk = []string{
	"metadata",
	"boost.db",
}

type BackupMgr struct {
	src    lotus_repo.LockedRepo
	ds     dtypes.MetadataDS
	dbName string
	lck    sync.Mutex
	db     *sql.DB
}

func NewBackupMgr(src lotus_repo.LockedRepo, ds datastore.Batching, name string, db *sql.DB) *BackupMgr {
	return &BackupMgr{
		src:    src,
		ds:     ds,
		dbName: name,
		db:     db,
	}
}

func (b *BackupMgr) Backup(ctx context.Context, dstDir string) error {
	_, err := os.Stat(dstDir)
	if os.IsNotExist(err) {
		return fmt.Errorf("could not open backup location: %w", err)
	}

	return b.initBackup(ctx, dstDir)
}

func (b *BackupMgr) initBackup(ctx context.Context, dstDir string) error {
	s := b.lck.TryLock()
	if !s {
		return fmt.Errorf("unable to take lock for backup, please check if there is already another backup running")
	}
	defer b.lck.Unlock()

	// Create tmp backup directory and open it as Boost repo
	bkpDir, err := os.MkdirTemp("", "boost_backup_")
	if err != nil {
		return err
	}

	defer func() {
		_ = os.RemoveAll(bkpDir)
	}()

	err = b.takeBackup(ctx, bkpDir)
	if err != nil {
		return err
	}

	// Move directory to the backup location
	backupDirName := "boost_backup_" + time.Now().Format("20060102150405")
	if err := os.Rename(bkpDir, path.Join(dstDir, backupDirName)); err != nil {
		// Try to rename in place if move fails. This would preserve the backup directory and allow user to use the backup
		if merr := os.Rename(bkpDir, path.Join(os.TempDir(), backupDirName)); merr != nil {
			return fmt.Errorf("failed to move backup directory and error renaming backup directory %s: %w", bkpDir, merr)
		}
		return fmt.Errorf("error moving backup directory %s to %s: %w\nbackup directory saved as %v", bkpDir, dstDir, err, path.Join(os.TempDir(), backupDirName))
	}

	return nil
}

func (b *BackupMgr) takeBackup(ctx context.Context, bkpDir string) error {
	dstRepo, err := lotus_repo.NewFS(bkpDir)
	if err != nil {
		return err
	}

	dst, err := dstRepo.Lock(repo.Boost)
	if err != nil {
		return err
	}

	defer func() {
		_ = dst.Close()
	}()

	// Create keystore at backup location
	if err := os.Mkdir(path.Join(dst.Path(), "keystore"), 0700); err != nil {
		return fmt.Errorf("error creating keystore directory %s: %w", path.Join(dst.Path(), "keystore"), err)
	}

	// Copy keys from Boost to the backup repo
	err = CopyKeysBetweenRepos(b.src, dst)
	if err != nil {
		return err
	}

	// Copy metadata from boost repo to backup repo
	fpath, err := homedir.Expand(path.Join(dst.Path(), MetadataFileName))
	if err != nil {
		return fmt.Errorf("expanding metadata file path: %w", err)
	}

	err = BackupMetadata(ctx, b.ds, fpath)
	if err != nil {
		return err
	}

	// Backup the SQL DBs
	if err := boostdb.SqlBackup(ctx, b.db, bkpDir, b.dbName); err != nil {
		return err
	}

	// Generate the list of files to be copied from boost repo to the backup repo
	fl, err := GenerateBkpFileList(b.src.Path(), false)
	if err != nil {
		return fmt.Errorf("unable to generate list of files: %w", err)
	}

	// Copy files
	if err := os.Mkdir(path.Join(bkpDir, ConfigDirName), 0755); err != nil {
		return fmt.Errorf("error creating config directory %s: %w", path.Join(bkpDir, ConfigDirName), err)
	}
	if err := CopyFiles(b.src.Path(), dst.Path(), fl); err != nil {
		return fmt.Errorf("error copying file: %w", err)
	}

	return nil
}

func CopyKeysBetweenRepos(srcRepo lotus_repo.LockedRepo, dstRepo lotus_repo.LockedRepo) error {
	srcKS, err := srcRepo.KeyStore()
	if err != nil {
		return err
	}

	dstKS, err := dstRepo.KeyStore()
	if err != nil {
		return err
	}

	keys, err := srcKS.List()
	if err != nil {
		return err
	}

	for _, k := range keys {
		ki, err := srcKS.Get(k)
		if err != nil {
			return err
		}
		err = dstKS.Put(k, ki)
		if err != nil {
			return err
		}
	}
	return nil
}

func BackupMetadata(ctx context.Context, srcDS datastore.Batching, fpath string) error {
	bds, err := backupds.Wrap(srcDS, backupds.NoLogdir)
	if err != nil {
		return err
	}

	out, err := os.OpenFile(fpath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("opening backup file %s: %w", fpath, err)
	}

	if err := bds.Backup(ctx, out); err != nil {
		return fmt.Errorf("backup error: %w", err)
	}

	if err := out.Close(); err != nil {
		return fmt.Errorf("closing backup file: %w", err)
	}

	return nil
}

func CopyFiles(srcDir, destDir string, flist []string) error {

	for _, fName := range flist {

		f, err := os.Lstat(path.Join(srcDir, fName))

		if os.IsNotExist(err) {
			fmt.Printf("Not copying %s as file does not exists\n", path.Join(srcDir, fName))
			return nil
		}

		if err != nil && !os.IsNotExist(err) {
			return err
		}

		// Handle if symlinks
		if f.Mode()&os.ModeSymlink == os.ModeSymlink {
			linkDest, err := os.Readlink(path.Join(srcDir, fName))
			if err != nil {
				return err
			}
			if err = os.Symlink(linkDest, path.Join(destDir, fName)); err != nil {
				return err
			}
		} else {

			input, err := os.ReadFile(path.Join(srcDir, fName))
			if err != nil {
				return err
			}

			err = os.WriteFile(path.Join(destDir, fName), input, f.Mode())
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func GenerateBkpFileList(repoPath string, offline bool) ([]string, error) {
	cfgFiles, err := os.ReadDir(path.Join(repoPath, ConfigDirName))
	if err != nil {
		return nil, fmt.Errorf("failed to read files from config directory: %w", err)
	}
	var fl []string
	for _, cfgFile := range cfgFiles {
		f := path.Join(ConfigDirName, cfgFile.Name())
		fl = append(fl, f)
	}
	fl = append(fl, BkpFileList...)
	if offline {
		fl = append(fl, boostdb.DealsDBName)
	}

	return fl, nil
}
