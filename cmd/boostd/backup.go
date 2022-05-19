package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"gopkg.in/cheggaaa/pb.v1"

	"github.com/filecoin-project/boost/node"
	"github.com/filecoin-project/lotus/lib/backupds"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
)

const metadaFileName = "metadata"

var fm = []string{"api",
	"boost.db",
	"boost.logs.db",
	"config.toml",
	"storage.json",
	"token"}

var backupCmd = &cli.Command{
	Name:   "backup",
	Usage: "boostd backup <backup directory>",
	Description:  "Performs offline backup of Boost",
	Before: before,
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() != 1 {
			return fmt.Errorf("usage: boostd backup <backup directory>")
		}

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

		lr, err := r.LockRO(node.Boost)
		if err != nil {
			return fmt.Errorf("locking repo: %w. Please stop the boostd process to take backup", err)
		}
		defer lr.Close()

		mds, err := lr.Datastore(cctx.Context, metadataNamespace)
		if err != nil {
			return fmt.Errorf("getting metadata datastore: %w", err)
		}

		bds, err := backupds.Wrap(mds, backupds.NoLogdir)
		if err != nil {
			return err
		}

		bpath, err := homedir.Expand(cctx.Args().First())
		if err != nil {
			return fmt.Errorf("expanding backup directory path: %w", err)
		}

		fmt.Println("Creating backup directory")

		bkpDir := path.Join(bpath, "boost_backup_"+time.Now().Format("20060102150405"))

		if err := os.Mkdir(bkpDir, 0755); err != nil {
			return fmt.Errorf("error creating backup directory %s: %w", bkpDir, err)
		}

		fpathName := path.Join(bkpDir, metadaFileName)

		fpath, err := homedir.Expand(fpathName)
		if err != nil {
			return fmt.Errorf("expanding metadata file path: %w", err)
		}

		fmt.Println("creating metadata backup")

		out, err := os.OpenFile(fpath, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("opening backup file %s: %w", fpath, err)
		}

		defer func() {
			if err := out.Close(); err != nil {
				log.Errorw("closing backup file: %w", err)
			}
		}()

		if err := bds.Backup(cctx.Context, out); err != nil {
			return fmt.Errorf("backup error: %w", err)
		}

		fmt.Println("Copying the files to backup directory")

		for _, name := range fm {
			srcName := path.Join(lr.Path(), name)

			srcPath, err := homedir.Expand(srcName)
			if err != nil {
				return fmt.Errorf("expanding source file path %s: %w", srcName, err)
			}

			destName := path.Join(bkpDir, name)

			destPath, err := homedir.Expand(destName)
			if err != nil {
				return fmt.Errorf("expanding destination file path %s: %w", destName, err)
			}

			if err := copy_files(srcPath, destPath); err != nil {
				return fmt.Errorf("error copying file %s: %w", srcName, err)
			}

		}

		fmt.Println("Boost repo successfully backed up at " + bkpDir)

		return nil
	},
}

var restoreCmd = &cli.Command{
	Name:   "restore",
	Usage:  "boostd restore <backup dir>",
	Description:  "Restores a boost repository from backup",
	Before: before,
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() != 1 {
			return fmt.Errorf("restore only takes one argument (backup directory path)")
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
		if err := r.Init(node.Boost); err != nil {
			return err
		}

		lr, err := r.Lock(node.Boost)
		if err != nil {
			return err
		}
		defer lr.Close()

		mds, err := lr.Datastore(cctx.Context, metadataNamespace)
		if err != nil {
			return err
		}

		bpath, err := homedir.Expand(cctx.Args().First())
		if err != nil {
			return fmt.Errorf("expanding backup directory path: %w", err)
		}

		fpathName := path.Join(bpath, metadaFileName)

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
		defer f.Close()

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

		rpath, err := homedir.Expand(lr.Path())
		if err != nil {
			return fmt.Errorf("expanding boost repo path: %w", err)
		}

		for _, name := range fm {
			srcName := path.Join(bpath, name)

			srcPath, err := homedir.Expand(srcName)
			if err != nil {
				return fmt.Errorf("expanding source file path %s: %w", srcName, err)
			}

			destName := path.Join(rpath, name)

			destPath, err := homedir.Expand(destName)
			if err != nil {
				return fmt.Errorf("expanding destination file path %s: %w", destName, err)
			}

			if err := copy_files(srcPath, destPath); err != nil {
				return fmt.Errorf("error copying file %s: %w", srcName, err)
			}

		}

		fmt.Println("Boost repo successfully restored at " + lr.Path())

		return nil
	},
}

func copy_files(src, dest string) error {
	f, err := os.Stat(src)

	if os.IsNotExist(err) {
		fmt.Printf("Not copying %s as file does not exists\n", src)
		return nil
	}

	if err != nil && !os.IsNotExist(err) {
		return err
	}

	input, err := ioutil.ReadFile(src)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(dest, input, f.Mode())
	if err != nil {
		return err
	}

	return nil
}
