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

var fm = []string{"boost.db",
	"boost.logs.db",
	"config.toml",
	"storage.json",
	"token"}

var backupCmd = &cli.Command{
	Name:        "backup",
	Usage:       "boostd backup <backup directory>",
	Description: "Performs offline backup of Boost",
	Before:      before,
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

		b, err := lotus_repo.NewFS(bkpDir)
		if err != nil {
			return err
		}

		lb, err := b.Lock(node.Boost)
		if err != nil {
			return err
		}
		defer lb.Close()

		fmt.Println("Copying keystore")

		if err := os.Mkdir(path.Join(bkpDir, "keystore"), 0700); err != nil {
			return fmt.Errorf("error creating keystore directory %s: %w", path.Join(bkpDir, "keystore"), err)
		}

		if err := migrateMarketsKeystore(lr, lb); err != nil {
			return fmt.Errorf("error copying keys: %w", err)
		}

		if err := os.Mkdir(path.Join(bkpDir, "config"), 0755); err != nil {
			return fmt.Errorf("error creating config directory %s: %w", path.Join(bkpDir, "config"), err)
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

		cfgFiles, err := ioutil.ReadDir(path.Join(lr.Path(), "config"))
		if err != nil {
			return fmt.Errorf("failed to read files from config directory: %w", err)
		}

		for _, cfgFile := range cfgFiles {
			f := path.Join("config", cfgFile.Name())
			fm = append(fm, f)
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

			if err := copyFiles(srcPath, destPath); err != nil {
				return fmt.Errorf("error copying file %s: %w", srcName, err)
			}

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

		bpath, err := homedir.Expand(cctx.Args().First())
		if err != nil {
			return fmt.Errorf("expanding backup directory path: %w", err)
		}

		fmt.Printf("Checking backup directory %s\n", bpath)

		flist := []string{"metadata", "boost.db", "boost.logs.db"}
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
		if err := r.Init(node.Boost); err != nil {
			return err
		}

		lr, err := r.Lock(node.Boost)
		if err != nil {
			return err
		}
		defer lr.Close()

		b, err := lotus_repo.NewFS(bpath)
		if err != nil {
			return err
		}

		lb, err := b.Lock(node.Boost)
		if err != nil {
			return err
		}
		defer lb.Close()

		fmt.Println("Copying keystore")

		if err := migrateMarketsKeystore(lb, lr); err != nil {
			return fmt.Errorf("error copying keys: %w", err)
		}

		mds, err := lr.Datastore(cctx.Context, metadataNamespace)
		if err != nil {
			return err
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

		cfgFiles, err := ioutil.ReadDir(path.Join(lb.Path(), "config"))
		if err != nil {
			return fmt.Errorf("failed to read files from config directory: %w", err)
		}

		for _, cfgFile := range cfgFiles {
			f := path.Join("config", cfgFile.Name())
			fm = append(fm, f)
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

			if err := copyFiles(srcPath, destPath); err != nil {
				return fmt.Errorf("error copying file %s: %w", srcName, err)
			}

		}

		fmt.Println("Boost repo successfully restored at " + lr.Path())

		return nil
	},
}

func copyFiles(src, dest string) error {
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
