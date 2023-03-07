package modules

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
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/lib/backupds"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	"github.com/ipfs/go-datastore"
	"github.com/mattn/go-sqlite3"
	"github.com/mitchellh/go-homedir"
)

const metadaFileName = "metadata"

var fm = []string{"config.toml",
	"storage.json",
	"token"}

type BackupMgrConfig struct {
	// Frequency of online backup in hours
	Frequency time.Duration
	// Location of backup files. Must be specified for online backups to work
	Location string
}

type BackupDBs struct {
	db   *sqlite3.SQLiteConn
	name string
}

type BackupMgr struct {
	ctx       context.Context
	cancel    context.CancelFunc
	closeSync sync.Once
	runWg     sync.WaitGroup
	src       lotus_repo.LockedRepo
	cfg       BackupMgrConfig
	ds        datastore.Batching
	dbs       []BackupDBs
	lck       sync.Mutex
}

func NewBackupMgr(src lotus_repo.LockedRepo, cfg BackupMgrConfig, ds datastore.Batching, dbs []BackupDBs) (*BackupMgr, error) {
	ctx, cancel := context.WithCancel(context.Background())
	return &BackupMgr{
		ctx:    ctx,
		cancel: cancel,
		src:    src,
		cfg:    cfg,
		ds:     ds,
		dbs:    dbs,
	}, nil
}

func (b *BackupMgr) Start() error {
	log.Infow("online backup manager: starting")
	_, err := os.Stat(b.cfg.Location)
	if os.IsNotExist(err) {
		return fmt.Errorf("could not open backup location: %w", err)
	}
	b.run()
	return nil
}

func (b *BackupMgr) TakeBackup() error {
	s := b.lck.TryLock()
	if !s {
		return fmt.Errorf("unable to take lock for backup")
	}
	defer b.lck.Unlock()

	// Create tmp backup directory and open it as Boost repo
	bkpDir, err := ioutil.TempDir("", "boost_backup_")
	if err != nil {
		log.Fatal(err)
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

	srcKS, err := b.src.KeyStore()
	if err != nil {
		return err
	}

	dstKS, err := dst.KeyStore()
	if err != nil {
		return err
	}

	// Copy keys from Boost to the backup repo
	err = copyKeys(srcKS, dstKS)
	if err != nil {
		return err
	}

	// Copy metadata from boost repo to backup repo
	fpath, err := homedir.Expand(path.Join(dst.Path(), metadaFileName))
	if err != nil {
		return fmt.Errorf("expanding metadata file path: %w", err)
	}

	err = backupMetadata(b.ctx, b.ds, fpath)
	if err != nil {
		return err
	}

	// Backup the SQL DBs
	var wg sync.WaitGroup
	wg.Add(len(b.dbs))
	results := make(chan error, len(b.dbs))
	for _, sqlDB := range b.dbs {
		go func(d BackupDBs) {
			defer wg.Done()
			err = sqlBackup(d.db, bkpDir, d.name)
			if err != nil {
				results <- err
				return
			}
			results <- nil
		}(sqlDB)
	}

	wg.Wait()
	close(results)
	for v := range results {
		if v != nil {
			return fmt.Errorf("sql backup failed with error: %w", v)
		}
	}

	// Generate the list of files to be copied from boost repo to the backup repo
	cfgFiles, err := ioutil.ReadDir(path.Join(b.src.Path(), "config"))
	if err != nil {
		return fmt.Errorf("failed to read files from config directory: %w", err)
	}

	for _, cfgFile := range cfgFiles {
		f := path.Join("config", cfgFile.Name())
		fm = append(fm, f)
	}

	// Copy files
	if err := copyFiles(b.src.Path(), dst.Path(), fm); err != nil {
		return fmt.Errorf("error copying file: %w", err)
	}

	// Close backup repo before moving it
	if err := dst.Close(); err != nil {
		return fmt.Errorf("error closing the locked backup repo: %w", err)
	}

	// Move directory to the backup location
	if err := os.Rename(bkpDir, path.Join(b.cfg.Location, "boost_backup_"+time.Now().Format("20060102150405"))); err != nil {
		return fmt.Errorf("error moving backup directory %s to %s: %w", bkpDir, b.cfg.Location, err)
	}

	// TODO: Archive the directory and save it to the backup location

	return nil
}

func sqlBackup(srcD *sqlite3.SQLiteConn, dstDir, name string) error {
	dbPath := path.Join(dstDir, name+"?cache=shared")
	db, sqlt3, err := db.SqlDB(dbPath)
	if err != nil {
		return fmt.Errorf("failed to open source sql db for backup for %v: %w", name, err)
	}

	defer db.Close()

	bkp, err := sqlt3.Backup("main", srcD, "main")
	if err != nil {
		return fmt.Errorf("failed to start db backup for %v: %w", name, err)
	}
	defer bkp.Close()

	// Allow the initial page count and remaining values to be retrieved
	isDone, err := bkp.Step(0)
	if err != nil {
		return fmt.Errorf("unable to perform an initial 0-page backup step for %v: %w", name, err)
	}
	if isDone {
		return fmt.Errorf("backup is unexpectedly done")
	}

	// Check that the page count and remaining values are reasonable.
	initialPageCount := bkp.PageCount()
	if initialPageCount <= 0 {
		return fmt.Errorf("unexpected initial page count value for %v: %v", name, initialPageCount)
	}
	initialRemaining := bkp.Remaining()
	if initialRemaining <= 0 {
		return fmt.Errorf("unexpected initial remaining value for %v: %v", name, initialRemaining)
	}
	if initialRemaining != initialPageCount {
		return fmt.Errorf("initial remaining value %v differs from the initial page count value %v for %v", initialRemaining, initialPageCount, name)
	}

	// Copy all the pages
	isDone, err = bkp.Step(-1)
	if err != nil {
		return fmt.Errorf("failed to perform a backup step for %v: %w", name, err)
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
		return fmt.Errorf("unexpected remaining value: %v for %v", finalRemaining, name)
	}

	// Finish the backup.
	err = bkp.Finish()
	if err != nil {
		return fmt.Errorf("failed to finish backup for %v: %w", name, err)
	}

	return nil
}

func copyKeys(srcKS types.KeyStore, dstKS types.KeyStore) error {
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

func backupMetadata(ctx context.Context, srcDS datastore.Batching, fpath string) error {
	bds, err := backupds.Wrap(srcDS, backupds.NoLogdir)
	if err != nil {
		return err
	}

	out, err := os.OpenFile(fpath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("opening backup file %s: %w", fpath, err)
	}

	defer func() {
		if err := out.Close(); err != nil {
			log.Errorw("closing backup file: %w", err)
		}
	}()

	if err := bds.Backup(ctx, out); err != nil {
		return fmt.Errorf("backup error: %w", err)
	}

	return nil
}

func copyFiles(srcDir, destDir string, flist []string) error {

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

func (b *BackupMgr) run() {
	log.Info("online backup manager: start")
	b.runWg.Add(1)
	defer func() {
		b.runWg.Done()
		log.Info("online backup manager: complete")
	}()

	// Create a ticker with an hour tick
	ticker := time.NewTicker(time.Hour * b.cfg.Frequency)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := b.TakeBackup(); err != nil {
				log.Errorf("failed to take backup at %s", time.Now())
				// TODO: Surface results to Boost UI
			}
		case <-b.ctx.Done():
			return
		}
	}
}

func (b *BackupMgr) Stop() {
	b.closeSync.Do(func() {
		log.Infow("online backup manager: shutdown")
		b.cancel()
		b.runWg.Wait()
		log.Info("online backup manager: shutdown complete")
	})
}
