package backup

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"time"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/node/repo"
	"github.com/filecoin-project/lotus/lib/backupds"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	"github.com/ipfs/go-datastore"
	"github.com/mattn/go-sqlite3"
	"github.com/mitchellh/go-homedir"
)

const MetadataFileName = "metadata"

var BkpFileList = []string{"config.toml",
	"storage.json",
	"token"}

var RestoreFileChk = []string{
	"metadata",
	"boost.db",
}

type BackupDB struct {
	Db   *sqlite3.SQLiteConn
	Name string
}

type BackupMgr struct {
	src lotus_repo.LockedRepo
	ds  datastore.Batching
	db  BackupDB
	lck sync.Mutex
}

func NewBackupMgr(src lotus_repo.LockedRepo, ds datastore.Batching, db BackupDB) (*BackupMgr, error) {
	return &BackupMgr{
		src: src,
		ds:  ds,
		db:  db,
	}, nil
}

func (b *BackupMgr) Backup(ctx context.Context, dstDir string) error {
	_, err := os.Stat(dstDir)
	if os.IsNotExist(err) {
		return fmt.Errorf("could not open backup location: %w", err)
	}

	return b.takeBackup(ctx, dstDir)
}

func (b *BackupMgr) takeBackup(ctx context.Context, dstDir string) error {
	s := b.lck.TryLock()
	if !s {
		return fmt.Errorf("unable to take lock for backup, please check if there is already another backup running")
	}
	defer b.lck.Unlock()

	// Create tmp backup directory and open it as Boost repo
	bkpDir, err := ioutil.TempDir("", "boost_backup_")
	if err != nil {
		return err
	}

	defer os.RemoveAll(bkpDir)

	dstRepo, err := lotus_repo.NewFS(bkpDir)
	if err != nil {
		return err
	}

	dst, err := dstRepo.Lock(repo.Boost)
	if err != nil {
		return err
	}

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
	if err := sqlBackup(b.db.Db, bkpDir, b.db.Name); err != nil {
		return err
	}

	// Generate the list of files to be copied from boost repo to the backup repo
	cfgFiles, err := ioutil.ReadDir(path.Join(b.src.Path(), "config"))
	if err != nil {
		return fmt.Errorf("failed to read files from config directory: %w", err)
	}

	var fl []string
	fl = append(fl, BkpFileList...)
	for _, cfgFile := range cfgFiles {
		f := path.Join("config", cfgFile.Name())
		fl = append(fl, f)
	}

	// Copy files
	if err := CopyFiles(b.src.Path(), dst.Path(), fl); err != nil {
		return fmt.Errorf("error copying file: %w", err)
	}

	// Close backup repo before moving it
	if err := dst.Close(); err != nil {
		return fmt.Errorf("error closing the locked backup repo: %w", err)
	}

	// Move directory to the backup location
	if err := os.Rename(bkpDir, path.Join(dstDir, "boost_backup_"+time.Now().Format("20060102150405"))); err != nil {
		return fmt.Errorf("error moving backup directory %s to %s: %w", bkpDir, dstDir, err)
	}

	// TODO: Archive the directory and save it to the backup location

	return nil
}

func sqlBackup(srcD *sqlite3.SQLiteConn, dstDir, name string) error {
	dbPath := path.Join(dstDir, name+"?cache=shared")
	db, sqlt3, err := db.SqlDB(dbPath)
	if err != nil {
		return fmt.Errorf("failed to open source sql db for backup: %w", err)
	}

	defer db.Close()

	bkp, err := sqlt3.Backup("main", srcD, "main")
	if err != nil {
		return fmt.Errorf("failed to start db backup: %w", err)
	}
	defer bkp.Close()

	// Allow the initial page count and remaining values to be retrieved
	isDone, err := bkp.Step(0)
	if err != nil {
		return fmt.Errorf("unable to perform an initial 0-page backup step: %w", err)
	}
	if isDone {
		return fmt.Errorf("backup is unexpectedly done")
	}

	// Check that the page count and remaining values are reasonable.
	initialPageCount := bkp.PageCount()
	if initialPageCount <= 0 {
		return fmt.Errorf("unexpected initial page count value: %v", initialPageCount)
	}
	initialRemaining := bkp.Remaining()
	if initialRemaining <= 0 {
		return fmt.Errorf("unexpected initial remaining value: %v", initialRemaining)
	}
	if initialRemaining != initialPageCount {
		return fmt.Errorf("initial remaining value %v differs from the initial page count value %v", initialRemaining, initialPageCount)
	}

	// Copy all the pages
	isDone, err = bkp.Step(-1)
	if err != nil {
		return fmt.Errorf("failed to perform a backup step: %w", err)
	}
	if !isDone {
		return fmt.Errorf("backup is unexpectedly not done")
	}

	// Check that the page count and remaining values are reasonable.
	finalPageCount := bkp.PageCount()
	if finalPageCount != initialPageCount {
		return fmt.Errorf("final page count %v differs from the initial page count %v", initialPageCount, finalPageCount)
	}
	finalRemaining := bkp.Remaining()
	if finalRemaining != 0 {
		return fmt.Errorf("unexpected remaining value: %v", finalRemaining)
	}

	// Finish the backup.
	err = bkp.Finish()
	if err != nil {
		return fmt.Errorf("failed to finish backup: %w", err)
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

			input, err := ioutil.ReadFile(path.Join(srcDir, fName))
			if err != nil {
				return err
			}

			err = ioutil.WriteFile(path.Join(destDir, fName), input, f.Mode())
			if err != nil {
				return err
			}
		}
	}

	return nil
}
