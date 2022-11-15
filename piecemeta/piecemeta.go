package piecemeta

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/filecoin-project/boost/tracing"
	"github.com/filecoin-project/boostd-data/client"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/markets/dagstore"
	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	format "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car/util"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-car/v2/index"
	carindex "github.com/ipld/go-car/v2/index"
	mh "github.com/multiformats/go-multihash"
)

var log = logging.Logger("piecemeta")

//go:generate go run github.com/golang/mock/mockgen -destination=mocks/piecemeta.go -package=mock_piecemeta . SectionReader,PieceReader,Store

type SectionReader interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
}

type PieceReader interface {
	// GetReader returns a reader over a piece. If there is no unsealed copy, returns ErrSealed.
	GetReader(ctx context.Context, id abi.SectorNumber, offset abi.PaddedPieceSize, length abi.PaddedPieceSize) (SectionReader, error)
}

type Store interface {
	AddDealForPiece(ctx context.Context, pieceCid cid.Cid, dealInfo model.DealInfo) error
	AddIndex(ctx context.Context, pieceCid cid.Cid, records []model.Record) error
	IsIndexed(ctx context.Context, pieceCid cid.Cid) (bool, error)
	GetIndex(ctx context.Context, pieceCid cid.Cid) (index.Index, error)
	GetOffsetSize(ctx context.Context, pieceCid cid.Cid, hash mh.Multihash) (*model.OffsetSize, error)
	GetPieceMetadata(ctx context.Context, pieceCid cid.Cid) (model.Metadata, error)
	GetPieceDeals(ctx context.Context, pieceCid cid.Cid) ([]model.DealInfo, error)
	SetCarSize(ctx context.Context, pieceCid cid.Cid, size uint64) error
	PiecesContainingMultihash(ctx context.Context, m mh.Multihash) ([]cid.Cid, error)
	MarkIndexErrored(ctx context.Context, pieceCid cid.Cid, err error) error
	RemoveDealForPiece(context.Context, cid.Cid, uuid.UUID) error
	RemovePieceMetadata(context.Context, cid.Cid) error
	RemoveIndexes(context.Context, cid.Cid) error

	//Delete(ctx context.Context, pieceCid cid.Cid) error
	//DeleteDealForPiece(ctx context.Context, pieceCid cid.Cid, dealUuid uuid.UUID) (bool, error)
}

type PieceMeta struct {
	store       Store
	pieceReader PieceReader

	addIdxThrottle chan struct{}
	addIdxOpByCid  sync.Map
}

func NewStore() *client.Store {
	return client.NewStore()
}

func NewPieceMeta(store Store, pr PieceReader, addIndexThrottleSize int) *PieceMeta {
	return &PieceMeta{
		store:          store,
		pieceReader:    pr,
		addIdxThrottle: make(chan struct{}, addIndexThrottleSize),
	}
}

type SectorAccessorAsPieceReader struct {
	dagstore.SectorAccessor
}

func (s *SectorAccessorAsPieceReader) GetReader(ctx context.Context, id abi.SectorNumber, offset abi.PaddedPieceSize, length abi.PaddedPieceSize) (SectionReader, error) {
	ctx, span := tracing.Tracer.Start(ctx, "sealer.get_reader")
	defer span.End()

	return s.SectorAccessor.UnsealSectorAt(ctx, id, offset.Unpadded(), length.Unpadded())
}

// Get all metadata about a particular piece
func (ps *PieceMeta) GetPieceMetadata(ctx context.Context, pieceCid cid.Cid) (model.Metadata, error) {
	ctx, span := tracing.Tracer.Start(ctx, "pm.get_piece_metadata")
	defer span.End()

	return ps.store.GetPieceMetadata(ctx, pieceCid)
}

// Get the list of deals (and the sector the data is in) for a particular piece
func (ps *PieceMeta) GetPieceDeals(ctx context.Context, pieceCid cid.Cid) ([]model.DealInfo, error) {
	ctx, span := tracing.Tracer.Start(ctx, "pm.get_piece_deals")
	defer span.End()

	deals, err := ps.store.GetPieceDeals(ctx, pieceCid)
	if err != nil {
		return nil, fmt.Errorf("listing deals for piece %s: %w", pieceCid, err)
	}

	return deals, nil
}

func (ps *PieceMeta) GetOffsetSize(ctx context.Context, pieceCid cid.Cid, hash mh.Multihash) (*model.OffsetSize, error) {
	ctx, span := tracing.Tracer.Start(ctx, "pm.get_offset")
	defer span.End()

	return ps.store.GetOffsetSize(ctx, pieceCid, hash)
}

func (ps *PieceMeta) GetCarSize(ctx context.Context, pieceCid cid.Cid) (uint64, error) {
	// Get the deals for the piece
	dls, err := ps.GetPieceDeals(ctx, pieceCid)
	if err != nil {
		return 0, fmt.Errorf("getting piece deals for piece %s: %w", pieceCid, err)
	}

	if len(dls) == 0 {
		return 0, fmt.Errorf("no deals for piece %s in index: piece not found", pieceCid)
	}

	// The size of the CAR should be the same for any deal, so just return the
	// first non-zero CAR size
	for _, dl := range dls {
		if dl.CarLength > 0 {
			return dl.CarLength, nil
		}
	}

	// There are no deals with a non-zero CAR size.
	// The CAR size is zero if it's been imported from the dagstore (the
	// dagstore doesn't store CAR size information). So instead work out the
	// size of the CAR by getting the offset of the last section in the CAR
	// file, then reading the section information.

	// Get the offset of the last section in the CAR file from the index.
	var lastSectionOffset uint64
	idx, err := ps.GetIterableIndex(ctx, pieceCid)
	if err != nil {
		return 0, fmt.Errorf("getting index for piece %s: %w", pieceCid, err)
	}
	err = idx.ForEach(func(_ mh.Multihash, offset uint64) error {
		if offset > lastSectionOffset {
			lastSectionOffset = offset
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("iterating index for piece %s: %w", pieceCid, err)
	}

	// Get a reader over the piece
	pieceReader, err := ps.GetPieceReader(ctx, pieceCid)
	if err != nil {
		return 0, fmt.Errorf("getting piece reader for piece %s: %w", pieceCid, err)
	}

	// Seek to the last section
	_, err = pieceReader.Seek(int64(lastSectionOffset), io.SeekStart)
	if err != nil {
		return 0, fmt.Errorf("seeking to offset %d in piece data: %w", lastSectionOffset, err)
	}

	// A section consists of
	// <size of cid+block><cid><block>

	// Get <size of cid+block>
	cr := &countReader{r: bufio.NewReader(pieceReader)}
	dataLength, err := binary.ReadUvarint(cr)
	if err != nil {
		return 0, fmt.Errorf("reading CAR section length: %w", err)
	}

	// The number of bytes in the uvarint that records <size of cid+block>
	dataLengthUvarSize := cr.count

	// Get the size of the (unpadded) CAR file
	unpaddedCarSize := lastSectionOffset + dataLengthUvarSize + dataLength

	// Write the CAR size back to the store so that it's cached for next time
	err = ps.store.SetCarSize(ctx, pieceCid, unpaddedCarSize)
	if err != nil {
		log.Errorw("writing CAR size to piece directory store", "pieceCid", pieceCid, "err", err)
	}

	return unpaddedCarSize, nil
}

func (ps *PieceMeta) AddDealForPiece(ctx context.Context, pieceCid cid.Cid, dealInfo model.DealInfo) error {
	ctx, span := tracing.Tracer.Start(ctx, "pm.add_deal_for_piece")
	defer span.End()

	// Check if the indexes have already been added
	isIndexed, err := ps.store.IsIndexed(ctx, pieceCid)
	if err != nil {
		return err
	}

	if !isIndexed {
		// Perform indexing of piece
		if err := ps.addIndexForPieceThrottled(ctx, pieceCid, dealInfo); err != nil {
			return fmt.Errorf("adding index for piece %s: %w", pieceCid, err)
		}
	}

	// Add deal to list of deals for this piece
	if err := ps.store.AddDealForPiece(ctx, pieceCid, dealInfo); err != nil {
		return fmt.Errorf("saving deal %s to store: %w", dealInfo.DealUuid, err)
	}

	return nil
}

type addIndexOperation struct {
	done chan struct{}
	err  error
}

func (ps *PieceMeta) addIndexForPieceThrottled(ctx context.Context, pieceCid cid.Cid, dealInfo model.DealInfo) error {
	// Check if there is already an add index operation in progress for the
	// given piece cid. If not, create a new one.
	opi, loaded := ps.addIdxOpByCid.LoadOrStore(pieceCid, &addIndexOperation{
		done: make(chan struct{}),
	})
	op := opi.(*addIndexOperation)
	if loaded {
		// There is an add index operation in progress, so wait for it to
		// complete
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-op.done:
			return op.err
		}
	} else {
		// A new operation was added to the map, so clean it up when it's done
		defer ps.addIdxOpByCid.Delete(pieceCid)
	}

	// Wait for the throttle to yield an open spot
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ps.addIdxThrottle <- struct{}{}:
	}
	defer func() { <-ps.addIdxThrottle }()

	// Perform the add index operation
	op.err = ps.addIndexForPiece(ctx, pieceCid, dealInfo)
	close(op.done)

	// Return the result
	return op.err
}

func (ps *PieceMeta) addIndexForPiece(ctx context.Context, pieceCid cid.Cid, dealInfo model.DealInfo) error {
	// Get a reader over the piece data
	reader, err := ps.pieceReader.GetReader(ctx, dealInfo.SectorID, dealInfo.PieceOffset, dealInfo.PieceLength)
	if err != nil {
		return fmt.Errorf("getting reader over piece %s: %w", pieceCid, err)
	}

	// Iterate over all the blocks in the piece to extract the index records
	recs := make([]model.Record, 0)
	opts := []carv2.Option{carv2.ZeroLengthSectionAsEOF(true)}
	blockReader, err := carv2.NewBlockReader(reader, opts...)
	if err != nil {
		return fmt.Errorf("getting block reader over piece %s: %w", pieceCid, err)
	}

	blockMetadata, err := blockReader.SkipNext()
	for err == nil {
		recs = append(recs, model.Record{
			Cid: blockMetadata.Cid,
			OffsetSize: model.OffsetSize{
				Offset: blockMetadata.Offset,
				Size:   blockMetadata.Size,
			},
		})

		blockMetadata, err = blockReader.SkipNext()
	}
	if !errors.Is(err, io.EOF) {
		return fmt.Errorf("generating index for piece %s: %w", pieceCid, err)
	}

	// Add mh => piece index to store: "which piece contains the multihash?"
	// Add mh => offset index to store: "what is the offset of the multihash within the piece?"
	if err := ps.store.AddIndex(ctx, pieceCid, recs); err != nil {
		return fmt.Errorf("adding CAR index for piece %s: %w", pieceCid, err)
	}

	return nil
}

func (ps *PieceMeta) buildIndexForPiece(ctx context.Context, pieceCid cid.Cid) error {
	dls, err := ps.GetPieceDeals(ctx, pieceCid)
	if err != nil {
		return fmt.Errorf("getting piece deals: %w", err)
	}

	if len(dls) == 0 {
		return fmt.Errorf("getting piece deals: no deals found for piece")
	}

	err = ps.addIndexForPieceThrottled(ctx, pieceCid, dls[0])
	if err != nil {
		return fmt.Errorf("adding index for piece deal %d: %w", dls[0].ChainDealID, err)
	}

	return nil
}

func (ps *PieceMeta) DeleteDealForPiece(ctx context.Context, pieceCid cid.Cid, dealUuid uuid.UUID) error {
	ctx, span := tracing.Tracer.Start(ctx, "pm.delete_deal_for_piece")
	defer span.End()

	//Delete deal from list of deals for this piece
	//It removes metadata and indexes if []deal is empty
	err := ps.store.RemoveDealForPiece(ctx, pieceCid, dealUuid)
	if err != nil {
		return fmt.Errorf("deleting deal from piece metadata: %w", err)
	}
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
func (ps *PieceMeta) GetPieceReader(ctx context.Context, pieceCid cid.Cid) (SectionReader, error) {
	ctx, span := tracing.Tracer.Start(ctx, "pm.get_piece_reader")
	defer span.End()

	// Get all deals containing this piece
	deals, err := ps.GetPieceDeals(ctx, pieceCid)
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
		reader, err := ps.pieceReader.GetReader(ctx, dl.SectorID, dl.PieceOffset, dl.PieceLength)
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
func (ps *PieceMeta) PiecesContainingMultihash(ctx context.Context, m mh.Multihash) ([]cid.Cid, error) {
	ctx, span := tracing.Tracer.Start(ctx, "pm.pieces_containing_multihash")
	defer span.End()

	return ps.store.PiecesContainingMultihash(ctx, m)
}

func (ps *PieceMeta) GetIterableIndex(ctx context.Context, pieceCid cid.Cid) (carindex.IterableIndex, error) {
	ctx, span := tracing.Tracer.Start(ctx, "pm.get_iterable_index")
	defer span.End()

	idx, err := ps.store.GetIndex(ctx, pieceCid)
	if err != nil {
		return nil, err
	}

	switch concrete := idx.(type) {
	case carindex.IterableIndex:
		return concrete, nil
	default:
		return nil, fmt.Errorf("expected index to be MultihashIndexSorted but got %T", idx)
	}
}

// Get a block (used by Bitswap retrieval)
func (ps *PieceMeta) BlockstoreGet(ctx context.Context, c cid.Cid) ([]byte, error) {
	// TODO: use caching to make this efficient for repeated Gets against the same piece
	ctx, span := tracing.Tracer.Start(ctx, "pm.get_block")
	defer span.End()

	// Get the pieces that contain the cid
	pieces, err := ps.PiecesContainingMultihash(ctx, c.Hash())
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
			reader, err := ps.GetPieceReader(ctx, pieceCid)
			if err != nil {
				return nil, fmt.Errorf("getting piece reader: %w", err)
			}
			defer reader.Close()

			// Get the offset of the block within the piece (CAR file)
			offsetSize, err := ps.GetOffsetSize(ctx, pieceCid, c.Hash())
			if err != nil {
				return nil, fmt.Errorf("getting offset/size for cid %s in piece %s: %w", c, pieceCid, err)
			}

			// Seek to the block offset
			_, err = reader.Seek(int64(offsetSize.Offset), io.SeekStart)
			if err != nil {
				return nil, fmt.Errorf("seeking to offset %d in piece reader: %w", int64(offsetSize.Offset), err)
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

func (ps *PieceMeta) BlockstoreGetSize(ctx context.Context, c cid.Cid) (int, error) {
	ctx, span := tracing.Tracer.Start(ctx, "pm.get_block_size")
	defer span.End()

	// Get the pieces that contain the cid
	pieces, err := ps.PiecesContainingMultihash(ctx, c.Hash())
	if err != nil {
		return 0, fmt.Errorf("getting pieces containing cid %s: %w", c, err)
	}
	if len(pieces) == 0 {
		// We must return ipld ErrNotFound here because that's the only type
		// that bitswap interprets as a not found error. All other error types
		// are treated as general errors.
		return 0, format.ErrNotFound{Cid: c}
	}

	// Get the size of the block from the first piece (should be the same for
	// any piece)
	offsetSize, err := ps.GetOffsetSize(ctx, pieces[0], c.Hash())
	if err != nil {
		return 0, fmt.Errorf("getting size of cid %s in piece %s: %w", c, pieces[0], err)
	}

	// Indexes imported from the DAG store do not have block size information
	// (they only have offset information). If the block has no size
	// information, rebuild the index from the piece data
	if offsetSize.Size == 0 {
		err = ps.buildIndexForPiece(ctx, pieces[0])
		if err != nil {
			return 0, fmt.Errorf("re-building index for piece %s: %w", pieces[0], err)
		}

		offsetSize, err = ps.GetOffsetSize(ctx, pieces[0], c.Hash())
		if err != nil {
			return 0, fmt.Errorf("getting size of cid %s in piece %s: %w", c, pieces[0], err)
		}
		if offsetSize.Size == 0 {
			zeroSizeErr := fmt.Errorf("bad index: size of block %s is zero", c)
			err = ps.store.MarkIndexErrored(ctx, pieces[0], zeroSizeErr)
			if err != nil {
				return 0, fmt.Errorf("setting index for piece %s to error state (%s): %w", pieces[0], zeroSizeErr, err)
			}
			return 0, fmt.Errorf("re-building index for piece %s: %w", pieces[0], zeroSizeErr)
		}
	}

	return int(offsetSize.Size), nil
}

func (ps *PieceMeta) BlockstoreHas(ctx context.Context, c cid.Cid) (bool, error) {
	ctx, span := tracing.Tracer.Start(ctx, "pm.has_block")
	defer span.End()

	// Get the pieces that contain the cid
	pieces, err := ps.PiecesContainingMultihash(ctx, c.Hash())
	if err != nil {
		return false, fmt.Errorf("getting pieces containing cid %s: %w", c, err)
	}
	return len(pieces) > 0, nil
}

// Get a blockstore over a piece (used by Graphsync retrieval)
func (ps *PieceMeta) GetBlockstore(ctx context.Context, pieceCid cid.Cid) (bstore.Blockstore, error) {
	ctx, span := tracing.Tracer.Start(ctx, "pm.get_blockstore")
	defer span.End()

	// Get a reader over the piece
	reader, err := ps.GetPieceReader(ctx, pieceCid)
	if err != nil {
		return nil, fmt.Errorf("getting piece reader for piece %s: %w", pieceCid, err)
	}

	// Get an index for the piece
	idx, err := ps.GetIterableIndex(ctx, pieceCid)
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

// countReader just counts the number of bytes read
type countReader struct {
	r     *bufio.Reader
	count uint64
}

func (c *countReader) ReadByte() (byte, error) {
	b, err := c.r.ReadByte()
	if err == nil {
		c.count++
	}
	return b, err
}
