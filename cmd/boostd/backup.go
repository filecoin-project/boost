package main

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"time"

	bcli "github.com/filecoin-project/boost/cli"
	"github.com/filecoin-project/boost/node/impl/backupmgr"
	"github.com/filecoin-project/boost/node/repo"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"gopkg.in/cheggaaa/pb.v1"

	"github.com/filecoin-project/lotus/lib/backupds"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
)

var backupCmd = &cli.Command{
	Name:        "backup",
	Usage:       "boostd backup <backup directory>",
	Description: "Takes a backup of Boost",
	Before:      before,
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "offline",
			Usage: "Performs an offline backup of Boost. Boost must be stopped.",
			Value: false,
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() != 1 {
			return fmt.Errorf("usage: boostd backup <backup directory>")
		}

		// Online backup
		if !cctx.Bool("offline") {
			api, closer, err := bcli.GetBoostAPI(cctx)
			if err != nil {
				return err
			}
			defer closer()

			ctx := bcli.ReqContext(cctx)

			bkpPath, err := homedir.Expand(cctx.Args().First())
			if err != nil {
				return fmt.Errorf("expanding backup directory path: %w", err)
			}

			bpath, err := filepath.Abs(bkpPath)
			if err != nil {
				return fmt.Errorf("failed get absolute path for backup directory: %w", err)
			}

			return api.OnlineBackup(ctx, bpath)
		}

		// Offline backup
		boostRepoPath := cctx.String(FlagBoostRepo)

		r, err := lotus_repo.NewFS(boostRepoPath)
		if err != nil {
			return err
		}
		ok, err := r.Exists()
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("repo at '%s' is not initialized", cctx.String(FlagBoostRepo))
		}

		lr, err := r.LockRO(repo.Boost)
		if err != nil {
			return fmt.Errorf("locking repo: %w. Please stop the boostd process to take backup", err)
		}
		defer func() {
			_ = lr.Close()
		}()

		mds, err := lr.Datastore(cctx.Context, metadataNamespace)
		if err != nil {
			return fmt.Errorf("getting metadata datastore: %w", err)
		}

		bkpPath, err := homedir.Expand(cctx.Args().First())
		if err != nil {
			return fmt.Errorf("expanding backup directory path: %w", err)
		}

		bpath, err := filepath.Abs(bkpPath)
		if err != nil {
			return fmt.Errorf("failed get absolute path for backup directory: %w", err)
		}

		fmt.Println("Creating backup directory")

		bkpDir := path.Join(bpath, "boost_backup_"+time.Now().Format("20060102150405"))

		if err := os.Mkdir(bkpDir, 0755); err != nil {
			return fmt.Errorf("error creating backup directory %s: %w", bkpDir, err)
		}

		b, err := lotus_repo.NewFS(bkpDir)
		if err != nil {
			return err
		}

		lb, err := b.Lock(repo.Boost)
		if err != nil {
			return err
		}
		defer func() {
			_ = lb.Close()
		}()

		fmt.Println("Copying keystore")

		if err := os.Mkdir(path.Join(bkpDir, "keystore"), 0700); err != nil {
			return fmt.Errorf("error creating keystore directory %s: %w", path.Join(bkpDir, "keystore"), err)
		}

		if err := backupmgr.CopyKeysBetweenRepos(lr, lb); err != nil {
			return fmt.Errorf("error copying keys: %w", err)
		}

		if err := os.Mkdir(path.Join(bkpDir, backupmgr.ConfigDirName), 0755); err != nil {
			return fmt.Errorf("error creating config directory %s: %w", path.Join(bkpDir, backupmgr.ConfigDirName), err)
		}

		fpathName := path.Join(bkpDir, backupmgr.MetadataFileName)

		fpath, err := homedir.Expand(fpathName)
		if err != nil {
			return fmt.Errorf("expanding metadata file path: %w", err)
		}

		fmt.Println("creating metadata backup")

		err = backupmgr.BackupMetadata(cctx.Context, mds, fpath)
		if err != nil {
			return fmt.Errorf("failed to take metadata backup: %w", err)
		}

		fl, err := backupmgr.GenerateBkpFileList(lr.Path(), true)
		if err != nil {
			return fmt.Errorf("failed to generate list of files to be copied: %w", err)
		}

		fmt.Println("Copying the files to backup directory")

		if err := backupmgr.CopyFiles(lr.Path(), bkpDir, fl); err != nil {
			return fmt.Errorf("error copying file: %w", err)
		}

		fmt.Println("Boost repo successfully backed up at " + bkpDir)

		return nil
	},
}

var restoreCmd = &cli.Command{
	Name:        "restore",
	Usage:       "boostd restore <backup dir>",
	Description: "Restores a boost repository from backup",
	Before:      before,
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() != 1 {
			return fmt.Errorf("restore only takes one argument (backup directory path)")
		}

		bkpPath, err := homedir.Expand(cctx.Args().First())
		if err != nil {
			return fmt.Errorf("expanding backup directory path: %w", err)
		}

		bpath, err := filepath.Abs(bkpPath)
		if err != nil {
			return fmt.Errorf("failed get absolute path for backup directory: %w", err)
		}

		fmt.Printf("Checking backup directory %s\n", bpath)

		flist := backupmgr.RestoreFileChk
		for _, fileName := range flist {
			_, err = os.Stat(path.Join(bpath, fileName))
			if os.IsNotExist(err) {
				return fmt.Errorf("did not find required repo file %s: %w", fileName, err)
			} else if err != nil {
				return fmt.Errorf("getting status of %s: %w", fileName, err)
			}
		}

		repoPath := cctx.String(FlagBoostRepo)
		fmt.Printf("Checking if repo exists at %s\n", repoPath)

		r, err := lotus_repo.NewFS(repoPath)
		if err != nil {
			return err
		}

		ok, err := r.Exists()
		if err != nil {
			return err
		}
		if ok {
			return fmt.Errorf("repo at '%s' is already initialized, cannot restore", repoPath)
		}

		fmt.Println("Creating boost repo")
		if err := r.Init(repo.Boost); err != nil {
			return err
		}

		lr, err := r.Lock(repo.Boost)
		if err != nil {
			return err
		}
		defer func() {
			_ = lr.Close()
		}()

		b, err := lotus_repo.NewFS(bpath)
		if err != nil {
			return err
		}

		lb, err := b.Lock(repo.Boost)
		if err != nil {
			return err
		}
		defer func() {
			_ = lb.Close()
		}()

		fmt.Println("Copying keystore")

		if err := backupmgr.CopyKeysBetweenRepos(lb, lr); err != nil {
			return fmt.Errorf("error copying keys: %w", err)
		}

		mds, err := lr.Datastore(cctx.Context, metadataNamespace)
		if err != nil {
			return err
		}

		fpathName := path.Join(bpath, backupmgr.MetadataFileName)

		fpath, err := homedir.Expand(fpathName)
		if err != nil {
			return fmt.Errorf("expanding metadata file path: %w", err)
		}

		st, err := os.Stat(fpath)
		if err != nil {
			return fmt.Errorf("stat backup file (%s): %w", fpath, err)
		}

		f, err := os.Open(fpath)
		if err != nil {
			return fmt.Errorf("opening backup file: %w", err)
		}
		defer func() {
			_ = f.Close()
		}()

		fmt.Println("Restoring metadata backup")

		bar := pb.New64(st.Size())
		br := bar.NewProxyReader(f)
		bar.ShowTimeLeft = true
		bar.ShowPercent = true
		bar.ShowSpeed = true
		bar.Units = pb.U_BYTES

		bar.Start()
		err = backupds.RestoreInto(br, mds)
		bar.Finish()

		if err != nil {
			return fmt.Errorf("restoring metadata: %w", err)
		}

		fmt.Println("Restoring files")

		if err := os.Mkdir(path.Join(lr.Path(), backupmgr.ConfigDirName), 0755); err != nil {
			return fmt.Errorf("error creating config directory %s: %w", path.Join(lr.Path(), backupmgr.ConfigDirName), err)
		}

		fl, err := backupmgr.GenerateBkpFileList(lb.Path(), true)
		if err != nil {
			return fmt.Errorf("failed to generate list of files to be copied: %w", err)
		}

		//Remove default config.toml created with repo to avoid conflict with symllink
		if err = os.Remove(path.Join(lr.Path(), "config.toml")); err != nil {
			return fmt.Errorf("failed to remove the default config file: %w", err)
		}

		if err := backupmgr.CopyFiles(lb.Path(), lr.Path(), fl); err != nil {
			return fmt.Errorf("error copying file: %w", err)
		}

		fmt.Println("Boost repo successfully restored at " + lr.Path())

		return nil
	},
}
