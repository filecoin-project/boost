package couchbase

import (
	"fmt"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/filecoin-project/boost/cmd/boostd-data/model"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	carindex "github.com/ipld/go-car/v2/index"
	mh "github.com/multiformats/go-multihash"
)

var log = logging.Logger("boostd-data-cb")

type PieceMetaService struct {
	col *gocb.Collection
}

func NewPieceMetaService() *PieceMetaService {
	bucketName := "piecestore"
	username := "Administrator"
	password := "boostdemo"

	cluster, err := gocb.Connect("couchbase://127.0.0.1", gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	bucket := cluster.Bucket(bucketName)

	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		log.Fatal(err)
	}

	return &PieceMetaService{
		col: bucket.DefaultCollection(),
	}
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
