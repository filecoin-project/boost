package main

import (
	"context"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"gopkg.in/cheggaaa/pb.v1"

	"github.com/filecoin-project/boost/node"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/lib/backupds"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	dstore "github.com/ipfs/go-datastore"
)

var fm = map[string]fs.FileMode{"api": 0644, "boost.db": 0644, "boost.logs.db": 0644, "config.toml": 0664, "storage.json": 0644, "token": 0600}

var backupCmd = &cli.Command{
	Name:  "backup",
	Usage: "Perorms offline backup of the Boost subsystem",
	Flags: []cli.Flag{
		&cli.PathFlag{
			Name:     "path",
			Usage:    "Specify a directory to create backup. Must be inside home directory.",
			Required: false,
		},
	},
	Before: before,
	Action: func(cctx *cli.Context) error {

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
			return fmt.Errorf("locking repo: %w", err)
		}
		defer lr.Close()

		mds, err := lr.Datastore(context.TODO(), "/metadata")
		if err != nil {
			return fmt.Errorf("getting metadata datastore: %w", err)
		}

		bds, err := backupds.Wrap(mds, backupds.NoLogdir)
		if err != nil {
			return err
		}

		bpath, err := homedir.Expand(cctx.Path("path"))
		if err != nil {
			return fmt.Errorf("expanding backup directory path: %w", err)
		}

		bkp_dir := path.Join(bpath, "boost_backup_"+time.Now().Format("20060102150405"))

		if err := os.Mkdir(bkp_dir, 0755); err != nil {
			return fmt.Errorf("Error creating backup directory %s: %w", bkp_dir, err)
		}

		fpath_name := path.Join(bkp_dir, "metadata")

		fpath, err := homedir.Expand(fpath_name)
		if err != nil {
			return fmt.Errorf("expanding metadata file path: %w", err)
		}

		out, err := os.OpenFile(fpath, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("opening backup file %s: %w", fpath, err)
		}

		if err := bds.Backup(cctx.Context, out); err != nil {
			if cerr := out.Close(); cerr != nil {
				log.Errorw("error closing backup file while handling backup error", "closeErr", cerr, "backupErr", err)
			}
			return fmt.Errorf("backup error: %w", err)
		}

		if err := out.Close(); err != nil {
			return fmt.Errorf("closing backup file: %w", err)
		}

		for name, perm := range fm {
			src_name := path.Join(bkp_dir, name)

			src_path, err := homedir.Expand(src_name)
			if err != nil {
				return fmt.Errorf("expanding source file path %s: %w", src_name, err)
			}

			dest_name := path.Join(bkp_dir, name)

			dest_path, err := homedir.Expand(src_name)
			if err != nil {
				return fmt.Errorf("expanding destination file path %s: %w", dest_name, err)
			}

			if err := copy_files(src_path, dest_path, perm); err != nil {
				return fmt.Errorf("Error copying file %s: %w", dest_name, err)
			}

		}

		return nil
	},
}

func copy_files(src, dest string, perm fs.FileMode) error {
	input, err := ioutil.ReadFile(src)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(dest, input, perm)
	if err != nil {
		return err
	}
	return nil
}

var restoreCmd = &cli.Command{
	Name:  "restore",
	Usage: "Restores a boost repository from backup",
	Flags: []cli.Flag{
		&cli.PathFlag{
			Name:     "restore-path",
			Usage:    "Specify the path to backup directory",
			Required: true,
		},
	},
	Before: before,
	Action: func(cctx *cli.Context) error {

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
			return fmt.Errorf("repo at '%s' is already initialized, cannot restore.", repoPath)
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

		mds, err := lr.Datastore(context.Background(), metadataNamespace)
		if err != nil {
			return err
		}

		bpath, err := homedir.Expand(cctx.Path("restore"))
		if err != nil {
			return fmt.Errorf("expanding backup directory path: %w", err)
		}

		fpath_name := path.Join(bpath, "metadata")

		fpath, err := homedir.Expand(fpath_name)
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
		defer f.Close() // nolint:errcheck

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

		fmt.Println("Resetting chainstore metadata")

		chainHead := dstore.NewKey("head")
		if err := mds.Delete(cctx.Context, chainHead); err != nil {
			return fmt.Errorf("clearing chain head: %w", err)
		}
		if err := store.FlushValidationCache(cctx.Context, mds); err != nil {
			return fmt.Errorf("clearing chain validation cache: %w", err)
		}

		fmt.Println("Restoring files")

		rpath, err := homedir.Expand(lr.Path())
		if err != nil {
			return fmt.Errorf("expanding boost repo path: %w", err)
		}

		for name, perm := range fm {
			src_name := path.Join(bpath, name)

			src_path, err := homedir.Expand(src_name)
			if err != nil {
				return fmt.Errorf("expanding source file path %s: %w", src_name, err)
			}

			dest_name := path.Join(rpath, name)

			dest_path, err := homedir.Expand(src_name)
			if err != nil {
				return fmt.Errorf("expanding destination file path %s: %w", dest_name, err)
			}

			if err := copy_files(src_path, dest_path, perm); err != nil {
				return fmt.Errorf("Error copying file %s: %w", dest_name, err)
			}

		}

		fmt.Println("Boost repo successfully restored at " + lr.Path())
		fmt.Println("You can now start boost with 'boostd -vv run'")

		return nil
	},
}
