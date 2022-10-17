package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	ldb "github.com/filecoin-project/boostd-data/ldb"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	piecestoreimpl "github.com/filecoin-project/go-fil-markets/piecestore/impl"
	"github.com/filecoin-project/lotus/lib/backupds"
	"github.com/filecoin-project/lotus/node/repo"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car/v2/index"
	carindex "github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
)

var (
	log = logging.Logger("migratedagstore")

	repopath    string
	newrepopath string
)

func init() {
	logging.SetLogLevel("*", "info")

	flag.StringVar(&repopath, "repopath", "", "path for repo to migrate (dagstore + piecestore)")
	flag.StringVar(&newrepopath, "newrepopath", "", "path for migrated repo (piecemeta store)")
}

func main() {
	flag.Parse()

	indicesPath := repopath + "/dagstore/index"

	ps, err := newPieceStore(repopath)
	if err != nil {
		panic(err)
	}

	ch := make(chan struct{}, 1)
	ps.OnReady(func(_ error) {
		ch <- struct{}{}
	})

	ctx := context.Background()
	err = ps.Start(ctx)
	if err != nil {
		panic(err)
	}

	<-ch

	pcids, err := ps.ListPieceInfoKeys()
	if err != nil {
		panic(err)
	}

	log.Infow("using newrepopath", "path", newrepopath)
	piecemeta := ldb.NewStore(newrepopath)

	log.Infow("using indicesPath", "path", indicesPath)

	indices := getAllIndices(indicesPath)

	log.Infow("all indices", "len", len(indices))

	for fcid, filepath := range indices {
		log.Infow("migrate index", "fcid", fcid)

		pieceCid, err := cid.Parse(fcid)
		if err != nil {
			panic(err)
		}

		idx, err := loadIndex(filepath)
		if err != nil {
			panic(err)
		}

		itidx, ok := idx.(carindex.IterableIndex)
		if !ok {
			panic(fmt.Errorf("index is not iterable for piece %s", pieceCid))
		}

		records, err := getRecords(itidx)
		if err != nil {
			panic(err)
		}

		err = piecemeta.AddIndex(ctx, pieceCid, records)
		if err != nil {
			panic(err)
		}
	}

	log.Infow("migrated indices", "len", len(indices))

	for _, pcid := range pcids {
		pi, err := ps.GetPieceInfo(pcid)
		if err != nil {
			panic(err)
		}

		for _, d := range pi.Deals {
			dealInfo := model.DealInfo{
				ChainDealID: d.DealID,
				SectorID:    d.SectorID,
				PieceOffset: d.Offset,
				PieceLength: d.Length,
			}

			err = piecemeta.AddDealForPiece(ctx, pcid, dealInfo)
			if err != nil {
				log.Errorw("cant add deal for piece", "pcid", pcid)
			}
		}
	}

	log.Infow("migrated piecestore", "len", len(pcids))

	time.Sleep(2 * time.Second)

	repopathSize, _ := DirSize(repopath)
	newrepopathSize, _ := DirSize(newrepopath)
	indicesSize, _ := DirSize(indicesPath)

	log.Infow("size repopath", "size", ByteCountSI(repopathSize))
	log.Infow("size indices", "size", ByteCountSI(indicesSize))
	log.Infow("size migrated repopath", "size", ByteCountSI(newrepopathSize))

	log.Infow("total indices and piececids", "indices", len(indices), "pcids", len(pcids))
}

func newPieceStore(path string) (piecestore.PieceStore, error) {
	ctx := context.Background()

	rpo, err := repo.NewFS(path)
	if err != nil {
		return nil, xerrors.Errorf("could not open repo %s: %w", path, err)
	}

	exists, err := rpo.Exists()
	if err != nil {
		return nil, xerrors.Errorf("checking repo %s exists: %w", path, err)
	}
	if !exists {
		return nil, xerrors.Errorf("repo does not exist: %s", path)
	}

	lr, err := rpo.Lock(repo.StorageMiner)
	if err != nil {
		return nil, xerrors.Errorf("locking repo %s: %w", path, err)
	}

	mds, err := lr.Datastore(ctx, "/metadata")
	if err != nil {
		return nil, err
	}

	bds, err := backupds.Wrap(mds, "")
	if err != nil {
		return nil, xerrors.Errorf("opening backupds: %w", err)
	}

	ps, err := piecestoreimpl.NewPieceStore(namespace.Wrap(bds, datastore.NewKey("/storagemarket")))

	return ps, nil
}

func getRecords(subject index.Index) ([]model.Record, error) {
	records := make([]model.Record, 0)

	switch idx := subject.(type) {
	case index.IterableIndex:
		err := idx.ForEach(func(m multihash.Multihash, offset uint64) error {

			cid := cid.NewCidV1(cid.Raw, m)

			records = append(records, model.Record{
				Cid:    cid,
				Offset: offset,
			})

			return nil
		})
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("wanted %v but got %v\n", multicodec.CarMultihashIndexSorted, idx.Codec())
	}
	return records, nil
}

func getAllIndices(path string) map[string]string {
	result := make(map[string]string)

	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal(err)
	}
	for _, f := range files {
		name := f.Name()

		if strings.Contains(name, "full.idx") {
			filepath := path + "/" + name
			name = strings.ReplaceAll(name, ".full.idx", "")

			result[name] = filepath
		}
	}

	return result
}

func loadIndex(path string) (index.Index, error) {
	defer func(now time.Time) {
		log.Debugw("loadindex", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	idxf, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer idxf.Close()

	subject, err := index.ReadFrom(idxf)
	if err != nil {
		return nil, err
	}

	return subject, nil
}

func ByteCountSI(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(b)/float64(div), "kMGTPE"[exp])
}

func DirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}
