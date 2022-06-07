package ldb

import (
	"fmt"
	"io/ioutil"
	"time"

	"github.com/filecoin-project/boost/cmd/boostd-data/model"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	levelds "github.com/ipfs/go-ds-leveldb"
	logging "github.com/ipfs/go-log/v2"
	carindex "github.com/ipld/go-car/v2/index"
	mh "github.com/multiformats/go-multihash"
	ldbopts "github.com/syndtr/goleveldb/leveldb/opt"
)

var log = logging.Logger("boostd-data-ldb")

type PieceMetaService struct {
	db datastore.Batching
}

func NewPieceMetaService(repopath string) *PieceMetaService {
	if repopath == "" {
		var err error
		repopath, err = ioutil.TempDir("", "ds-leveldb")
		if err != nil {
			panic(err)
		}
	}

	db, err := levelDs(repopath, false)
	if err != nil {
		panic(err)
	}

	log.Debugw("datastore.service", "repo path", repopath)

	return &PieceMetaService{
		db: db,
	}
}

func levelDs(path string, readonly bool) (datastore.Batching, error) {
	return levelds.NewDatastore(path, &levelds.Options{
		Compression: ldbopts.NoCompression,
		NoSync:      false,
		Strict:      ldbopts.StrictAll,
		ReadOnly:    readonly,
	})
}

func (s *PieceMetaService) AddDealForPiece(pieceCid cid.Cid, dealInfo model.DealInfo) error {
	log.Debugw("handle.add-deal-for-piece", "piece-cid", pieceCid)

	defer func(now time.Time) {
		log.Debugw("handled.add-deal-for-piece", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	return nil
}

// TODO: maybe implement over rpc subscription
func (s *PieceMetaService) GetIterableIndex(pieceCid cid.Cid) ([]carindex.Record, error) {
	log.Debugw("handle.get-iterable-index", "piece-cid", pieceCid)

	defer func(now time.Time) {
		log.Debugw("handled.get-iterable-index", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	return nil, nil
}

func (s *PieceMetaService) GetOffset(pieceCid cid.Cid, hash mh.Multihash) (uint64, error) {
	log.Debugw("handle.get-offset", "piece-cid", pieceCid)

	defer func(now time.Time) {
		log.Debugw("handled.get-offset", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	return 0, nil
}

func (s *PieceMetaService) GetPieceDeals(pieceCid cid.Cid) ([]model.DealInfo, error) {
	log.Debugw("handle.get-piece-deals", "piece-cid", pieceCid)

	defer func(now time.Time) {
		log.Debugw("handled.get-piece-deals", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	return nil, nil
}

// Get all pieces that contain a multihash (used when retrieving by payload CID)
func (s *PieceMetaService) PiecesContainingMultihash(m mh.Multihash) ([]cid.Cid, error) {
	log.Debugw("handle.pieces-containing-mh", "mh", m)

	defer func(now time.Time) {
		log.Debugw("handled.pieces-containing-mh", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	return nil, nil
}
