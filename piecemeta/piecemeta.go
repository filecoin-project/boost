package piecemeta

import (
	"bufio"
	"context"
	"fmt"
	"io"

	"github.com/filecoin-project/boostd-data/client"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/boostd-data/svc"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/markets/dagstore"
	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-car/util"
	"github.com/ipld/go-car/v2"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-car/v2/index"
	carindex "github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	mh "github.com/multiformats/go-multihash"
)

type SectionReader interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}

type Sealer interface {
	// GetReader returns a reader over a piece. If there is no unsealed copy, returns ErrSealed.
	GetReader(id abi.SectorNumber, offset abi.PaddedPieceSize, length abi.PaddedPieceSize) (SectionReader, error)
}

type Store interface {
	AddDealForPiece(pieceCid cid.Cid, dealInfo model.DealInfo) error
	AddIndex(pieceCid cid.Cid, records []model.Record) error
	IsIndexed(pieceCid cid.Cid) (bool, error)
	GetIndex(pieceCid cid.Cid) (index.Index, error)
	GetOffset(pieceCid cid.Cid, hash mh.Multihash) (uint64, error)
	GetPieceDeals(pieceCid cid.Cid) ([]model.DealInfo, error)
	PiecesContaining(m mh.Multihash) ([]cid.Cid, error)

	//Delete(pieceCid cid.Cid) error
	//DeleteDealForPiece(pieceCid cid.Cid, dealUuid uuid.UUID) (bool, error)
}

type PieceMeta struct {
	store  Store
	sealer Sealer
}

func NewStore(ctx context.Context) *client.Store {
	addr, _, err := svc.Setup(ctx, "couchbase")
	if err != nil {
		panic(err)
	}

	cl, err := client.NewStore("http://" + addr)
	if err != nil {
		panic(err)
	}

	return cl
}

func NewPieceMeta(store Store, sa dagstore.SectorAccessor) *PieceMeta {
	return &PieceMeta{store: store, sealer: &sealer{sa}}
}

type sealer struct {
	dagstore.SectorAccessor
}

func (s *sealer) GetReader(id abi.SectorNumber, offset abi.PaddedPieceSize, length abi.PaddedPieceSize) (SectionReader, error) {
	ctx := context.Background()
	return s.SectorAccessor.UnsealSectorAt(ctx, id, offset.Unpadded(), length.Unpadded())
}

// Get the list of deals (and the sector the data is in) for a particular piece
func (ps *PieceMeta) GetPieceDeals(pieceCid cid.Cid) ([]model.DealInfo, error) {
	deals, err := ps.store.GetPieceDeals(pieceCid)
	if err != nil {
		return nil, fmt.Errorf("listing deals for piece %s: %w", pieceCid, err)
	}

	return deals, nil
}

func (ps *PieceMeta) GetOffset(pieceCid cid.Cid, hash mh.Multihash) (uint64, error) {
	return ps.store.GetOffset(pieceCid, hash)
}

func (ps *PieceMeta) AddDealForPiece(pieceCid cid.Cid, dealInfo model.DealInfo) error {
	// Perform indexing of piece
	if err := ps.addIndexForPiece(pieceCid, dealInfo); err != nil {
		return fmt.Errorf("adding index for piece %s: %w", pieceCid, err)
	}

	// Add deal to list of deals for this piece
	if err := ps.store.AddDealForPiece(pieceCid, dealInfo); err != nil {
		return fmt.Errorf("saving deal %s to store: %w", dealInfo.DealUuid, err)
	}

	return nil
}

func (ps *PieceMeta) addIndexForPiece(pieceCid cid.Cid, dealInfo model.DealInfo) error {
	// Check if the indexes have already been added
	isIndexed, err := ps.store.IsIndexed(pieceCid)
	if err != nil {
		return err
	}

	if isIndexed {
		return nil
	}

	// Get a reader over the piece data
	reader, err := ps.sealer.GetReader(dealInfo.SectorID, dealInfo.PieceOffset, dealInfo.PieceLength)
	if err != nil {
		return err
	}

	// Get an index from the CAR file - works for both CARv1 and CARv2
	idx, err := car.ReadOrGenerateIndex(reader, car.ZeroLengthSectionAsEOF(true), car.StoreIdentityCIDs(true))
	if err != nil {
		return fmt.Errorf("generating index for piece %s: %w", pieceCid, err)
	}

	itidx, ok := idx.(carindex.IterableIndex)
	if !ok {
		return fmt.Errorf("index is not iterable for piece %s", pieceCid)
	}

	recs, err := getRecords(itidx)
	if err != nil {
		return err
	}

	// Add mh => piece index to store: "which piece contains the multihash?"
	// Add mh => offset index to store: "what is the offset of the multihash within the piece?"
	if err := ps.store.AddIndex(pieceCid, recs); err != nil {
		return fmt.Errorf("adding CAR index for piece %s: %w", pieceCid, err)
	}

	return nil
}

func (ps *PieceMeta) DeleteDealForPiece(pieceCid cid.Cid, dealUuid uuid.UUID) error {
	// Delete deal from list of deals for this piece
	//wasLast, err := ps.dealStore.Delete(pieceCid, dealUuid)
	//if err != nil {
	//return fmt.Errorf("deleting deal %s from store: %w", dealUuid, err)
	//}

	//if !wasLast {
	//return nil
	//}

	//// Remove piece indexes
	//if err := ps.deleteIndexForPiece(pieceCid); err != nil {
	//return fmt.Errorf("deleting index for piece %s: %w", pieceCid, err)
	//}

	return nil
}

//func (ps *PieceMeta) deleteIndexForPiece(pieceCid cid.Cid) interface{} {
// TODO: Maybe mark for GC instead of deleting immediately

// Delete mh => offset index from store
//err := ps.carIndex.Delete(pieceCid)
//if err != nil {
//err = fmt.Errorf("deleting CAR index for piece %s: %w", pieceCid, err)
//}

//// Delete mh => piece index from store
//if mherr := ps.mhToPieceIndex.Delete(pieceCid); mherr != nil {
//err = multierror.Append(fmt.Errorf("deleting cid index for piece %s: %w", pieceCid, mherr))
//}
//return err
//return nil
//}

// Used internally, and also by HTTP retrieval
func (ps *PieceMeta) GetPieceReader(pieceCid cid.Cid) (SectionReader, error) {
	// Get all deals containing this piece
	deals, err := ps.GetPieceDeals(pieceCid)
	if err != nil {
		return nil, fmt.Errorf("getting piece deals: %w", err)
	}

	if len(deals) == 0 {
		return nil, fmt.Errorf("no deals found for piece cid %s: %w", pieceCid, err)
	}

	// For each deal, try to read an unsealed copy of the data from the sector
	// it is stored in
	var merr error
	for i, dl := range deals {
		reader, err := ps.sealer.GetReader(dl.SectorID, dl.PieceOffset, dl.PieceLength)
		if err != nil {
			// TODO: log error
			if i < 3 {
				merr = multierror.Append(merr, err)
			}
			continue
		}

		return reader, nil
	}

	return nil, merr
}

// Get all pieces that contain a multihash (used when retrieving by payload CID)
func (ps *PieceMeta) PiecesContainingMultihash(m mh.Multihash) ([]cid.Cid, error) {
	return ps.store.PiecesContaining(m)
}

func (ps *PieceMeta) GetIterableIndex(pieceCid cid.Cid) (carindex.IterableIndex, error) {
	idx, err := ps.store.GetIndex(pieceCid)
	if err != nil {
		return nil, err
	}

	switch concrete := idx.(type) {
	case carindex.IterableIndex:
		return concrete, nil
	default:
		panic("expected MultihashIndexSorted idx")
	}
}

// Get a block (used by Bitswap retrieval)
func (ps *PieceMeta) GetBlock(c cid.Cid) ([]byte, error) {
	// TODO: use caching to make this efficient for repeated Gets against the same piece

	// Get the pieces that contain the cid
	pieces, err := ps.PiecesContainingMultihash(c.Hash())
	if err != nil {
		return nil, fmt.Errorf("getting pieces containing cid %s: %w", c, err)
	}
	if len(pieces) == 0 {
		return nil, fmt.Errorf("no pieces with cid %s found", c)
	}

	// Get a reader over one of the pieces and extract the block data
	var merr error
	for i, pieceCid := range pieces {
		data, err := func() ([]byte, error) {
			// Get a reader over the piece data
			reader, err := ps.GetPieceReader(pieceCid)
			if err != nil {
				return nil, fmt.Errorf("getting piece reader: %w", err)
			}

			// Get the offset of the block within the piece (CAR file)
			offset, err := ps.GetOffset(pieceCid, c.Hash())
			if err != nil {
				return nil, fmt.Errorf("getting offset for cid %s in piece %s: %w", c, pieceCid, err)
			}

			// Seek to the block offset
			_, err = reader.Seek(int64(offset), io.SeekStart)
			if err != nil {
				return nil, fmt.Errorf("seeking to offset %d in piece reader: %w", int64(offset), err)
			}

			// Read the block data
			_, data, err := util.ReadNode(bufio.NewReader(reader))
			if err != nil {
				return nil, fmt.Errorf("reading data for block %s from reader for piece %s: %w", c, pieceCid, err)
			}
			return data, nil
		}()
		if err != nil {
			if i < 3 {
				merr = multierror.Append(merr, err)
			}
			continue
		}
		return data, nil
	}

	return nil, merr
}

// Get a blockstore over a piece (used by Graphsync retrieval)
func (ps *PieceMeta) GetBlockstore(pieceCid cid.Cid) (bstore.Blockstore, error) {
	// Get a reader over the piece
	reader, err := ps.GetPieceReader(pieceCid)
	if err != nil {
		return nil, fmt.Errorf("getting piece reader for piece %s: %w", pieceCid, err)
	}

	// Get an index for the piece
	idx, err := ps.GetIterableIndex(pieceCid)
	if err != nil {
		return nil, fmt.Errorf("getting index for piece %s: %w", pieceCid, err)
	}

	// process index and store entries
	// Create a blockstore from the index and the piece reader
	bs, err := blockstore.NewReadOnly(reader, idx, carv2.ZeroLengthSectionAsEOF(true))
	if err != nil {
		return nil, fmt.Errorf("creating blockstore for piece %s: %w", pieceCid, err)
	}

	return bs, nil
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
