package piecedirectory

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	carutil "github.com/filecoin-project/boost/car"
	"github.com/filecoin-project/boost/piecedirectory/types"
	bdclient "github.com/filecoin-project/boostd-data/client"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/boostd-data/shared/tracing"
	bdtypes "github.com/filecoin-project/boostd-data/svc/types"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-data-segment/datasegment"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/lib/readerutil"
	"github.com/filecoin-project/lotus/markets/dagstore"
	"github.com/hashicorp/go-multierror"
	bstore "github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car"
	"github.com/ipld/go-car/util"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	carindex "github.com/ipld/go-car/v2/index"
	"github.com/jellydator/ttlcache/v2"
	"github.com/multiformats/go-multihash"
	mh "github.com/multiformats/go-multihash"
	"go.opentelemetry.io/otel/attribute"
)

var log = logging.Logger("piecedirectory")

var MaxCachedReaders = 128

type PieceDirectory struct {
	store       *bdclient.Store
	pieceReader types.PieceReader

	pieceReaderCacheMu sync.Mutex
	pieceReaderCache   *ttlcache.Cache

	ctx context.Context

	addIdxThrottleSize int
	addIdxThrottle     chan struct{}
	addIdxOpByCid      sync.Map
}

func NewPieceDirectory(store *bdclient.Store, pr types.PieceReader, addIndexThrottleSize int) *PieceDirectory {
	prCache := ttlcache.NewCache()
	_ = prCache.SetTTL(30 * time.Second)
	prCache.SetCacheSizeLimit(MaxCachedReaders)

	pd := &PieceDirectory{
		store:              store,
		pieceReader:        pr,
		pieceReaderCache:   prCache,
		addIdxThrottleSize: addIndexThrottleSize,
		addIdxThrottle:     make(chan struct{}, addIndexThrottleSize),
	}

	expireCallback := func(key string, reason ttlcache.EvictionReason, value interface{}) {
		log.Debugw("expire callback", "piececid", key, "reason", reason)

		r := value.(*cachedSectionReader)

		pd.pieceReaderCacheMu.Lock()
		defer pd.pieceReaderCacheMu.Unlock()

		r.expired = true

		if r.refs <= 0 {
			r.cancel()
			return
		}

		log.Debugw("expire callback with refs > 0", "refs", r.refs, "piececid", key, "reason", reason)
	}

	prCache.SetExpirationReasonCallback(expireCallback)

	return pd
}

func (ps *PieceDirectory) Start(ctx context.Context) {
	ps.ctx = ctx
}

func (ps *PieceDirectory) FlaggedPiecesList(ctx context.Context, filter *bdtypes.FlaggedPiecesListFilter, cursor *time.Time, offset int, limit int) ([]model.FlaggedPiece, error) {
	return ps.store.FlaggedPiecesList(ctx, filter, cursor, offset, limit)
}

func (ps *PieceDirectory) FlaggedPiecesCount(ctx context.Context, filter *bdtypes.FlaggedPiecesListFilter) (int, error) {
	return ps.store.FlaggedPiecesCount(ctx, filter)
}

func (ps *PieceDirectory) PiecesCount(ctx context.Context, maddr address.Address) (int, error) {
	return ps.store.PiecesCount(ctx, maddr)
}

func (ps *PieceDirectory) ScanProgress(ctx context.Context, maddr address.Address) (*bdtypes.ScanProgress, error) {
	return ps.store.ScanProgress(ctx, maddr)
}

// Get all metadata about a particular piece
func (ps *PieceDirectory) GetPieceMetadata(ctx context.Context, pieceCid cid.Cid) (types.PieceDirMetadata, error) {
	ctx, span := tracing.Tracer.Start(ctx, "pm.get_piece_metadata")
	defer span.End()

	// Get the piece metadata from the DB
	log.Debugw("piece metadata: get", "pieceCid", pieceCid)
	md, err := ps.store.GetPieceMetadata(ctx, pieceCid)
	if err != nil {
		return types.PieceDirMetadata{}, err
	}

	// Check if this process is currently indexing the piece
	log.Debugw("piece metadata: get indexing status", "pieceCid", pieceCid)
	_, indexing := ps.addIdxOpByCid.Load(pieceCid)

	// Return the db piece metadata along with the indexing flag
	log.Debugw("piece metadata: get complete", "pieceCid", pieceCid)
	return types.PieceDirMetadata{
		Metadata: md,
		Indexing: indexing,
	}, nil
}

// Get the list of deals (and the sector the data is in) for a particular piece
func (ps *PieceDirectory) GetPieceDeals(ctx context.Context, pieceCid cid.Cid) ([]model.DealInfo, error) {
	ctx, span := tracing.Tracer.Start(ctx, "pm.get_piece_deals")
	defer span.End()

	deals, err := ps.store.GetPieceDeals(ctx, pieceCid)
	if err != nil {
		return nil, fmt.Errorf("listing deals for piece %s: %w", pieceCid, err)
	}

	return deals, nil
}

func (ps *PieceDirectory) GetOffsetSize(ctx context.Context, pieceCid cid.Cid, hash mh.Multihash) (*model.OffsetSize, error) {
	ctx, span := tracing.Tracer.Start(ctx, "pm.get_offset")
	defer span.End()

	return ps.store.GetOffsetSize(ctx, pieceCid, hash)
}

func (ps *PieceDirectory) AddDealForPiece(ctx context.Context, pieceCid cid.Cid, dealInfo model.DealInfo) error {
	log.Debugw("add deal for piece", "piececid", pieceCid, "uuid", dealInfo.DealUuid)

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
	} else {
		log.Infow("add deal for piece", "index", "not re-indexing, piece already indexed")
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

func (ps *PieceDirectory) addIndexForPieceThrottled(ctx context.Context, pieceCid cid.Cid, dealInfo model.DealInfo) error {
	// Check if there is already an add index operation in progress for the
	// given piece cid. If not, create a new one.
	opi, loaded := ps.addIdxOpByCid.LoadOrStore(pieceCid, &addIndexOperation{
		done: make(chan struct{}),
	})
	op := opi.(*addIndexOperation)
	if loaded {
		log.Debugw("add index: operation in progress, waiting for completion", "pieceCid", pieceCid)
		defer func() {
			log.Debugw("add index: in progress operation completed", "pieceCid", pieceCid)
		}()

		// There is an add index operation in progress, so wait for it to
		// complete
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-op.done:
			return op.err
		}
	}

	// A new operation was added to the map, so clean it up when it's done
	defer ps.addIdxOpByCid.Delete(pieceCid)
	defer close(op.done)

	// Wait for the throttle to yield an open spot
	log.Debugw("add index: wait for open throttle position",
		"pieceCid", pieceCid, "queued", len(ps.addIdxThrottle), "queue-limit", ps.addIdxThrottleSize)
	select {
	case <-ctx.Done():
		op.err = ctx.Err()
		return ctx.Err()
	case ps.addIdxThrottle <- struct{}{}:
	}
	defer func() { <-ps.addIdxThrottle }()

	// Perform the add index operation.
	// Note: Once we start the add index operation we don't want to cancel it
	// if one of the waiting threads cancels its context. So instead we use the
	// PieceDirectory's context.
	op.err = ps.addIndexForPiece(ps.ctx, pieceCid, dealInfo)

	// Return the result
	log.Debugw("add index: completed", "pieceCid", pieceCid)
	return op.err
}

func (ps *PieceDirectory) addIndexForPiece(ctx context.Context, pieceCid cid.Cid, dealInfo model.DealInfo) error {
	// Get a reader over the piece data
	log.Debugw("add index: get index", "pieceCid", pieceCid)
	reader, err := ps.pieceReader.GetReader(ctx, dealInfo.MinerAddr, dealInfo.SectorID, dealInfo.PieceOffset, dealInfo.PieceLength)
	if err != nil {
		return fmt.Errorf("getting reader over piece %s: %w", pieceCid, err)
	}
	defer reader.Close() //nolint:errcheck

	// Try to parse data as containing a data segment index
	log.Debugw("add index: read index", "pieceCid", pieceCid)
	recs, err := parsePieceWithDataSegmentIndex(pieceCid, int64(dealInfo.PieceLength.Unpadded()), reader)
	if err != nil {
		log.Infow("add index: data segment check failed. falling back to car", "pieceCid", pieceCid, "err", err)
		// Iterate over all the blocks in the piece to extract the index records
		if _, err := reader.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("seek to start for piece %s: %w", pieceCid, err)
		}
		recs, err = parseRecordsFromCar(reader)
		if err != nil {
			return fmt.Errorf("parse car for piece %s: %w", pieceCid, err)
		}
	}

	// Add mh => piece index to store: "which piece contains the multihash?"
	// Add mh => offset index to store: "what is the offset of the multihash within the piece?"
	log.Debugw("add index: store index in local index directory", "pieceCid", pieceCid, "records", len(recs))
	if err := ps.store.AddIndex(ctx, pieceCid, recs, true); err != nil {
		return fmt.Errorf("adding CAR index for piece %s: %w", pieceCid, err)
	}

	return nil
}

func parseRecordsFromCar(reader io.Reader) ([]model.Record, error) {
	// Iterate over all the blocks in the piece to extract the index records
	recs := make([]model.Record, 0)
	opts := []carv2.Option{carv2.ZeroLengthSectionAsEOF(true)}
	blockReader, err := carv2.NewBlockReader(reader, opts...)
	if err != nil {
		return nil, fmt.Errorf("getting block reader over piece: %w", err)
	}

	blockMetadata, err := blockReader.SkipNext()
	for err == nil {
		recs = append(recs, model.Record{
			Cid: blockMetadata.Cid,
			OffsetSize: model.OffsetSize{
				Offset: blockMetadata.SourceOffset,
				Size:   blockMetadata.Size,
			},
		})

		blockMetadata, err = blockReader.SkipNext()
	}
	if !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("generating index for piece: %w", err)
	}
	return recs, nil
}

func parsePieceWithDataSegmentIndex(pieceCid cid.Cid, unpaddedSize int64, r types.SectionReader) ([]model.Record, error) {
	ps := abi.UnpaddedPieceSize(unpaddedSize).Padded()
	dsis := datasegment.DataSegmentIndexStartOffset(ps)
	if _, err := r.Seek(int64(dsis), io.SeekStart); err != nil {
		return nil, fmt.Errorf("could not seek to data segment index: %w", err)
	}
	dataSegments, err := datasegment.ParseDataSegmentIndex(r)
	if err != nil {
		return nil, fmt.Errorf("could not parse data segment index: %w", err)
	}
	segments, err := dataSegments.ValidEntries()
	if err != nil {
		return nil, fmt.Errorf("could not calculate valid entries: %w", err)
	}
	if len(segments) == 0 {
		return nil, fmt.Errorf("no data segments found")
	}

	recs := make([]model.Record, 0)
	for _, s := range segments {
		segOffset := s.UnpaddedOffest()
		segSize := s.UnpaddedLength()

		lr := io.NewSectionReader(r, int64(segOffset), int64(segSize))
		subRecs, err := parseRecordsFromCar(lr)
		if err != nil {
			// revisit when non-car files supported: one corrupt segment shouldn't translate into an error in other segments.
			return nil, fmt.Errorf("could not parse data segment #%d at offset %d: %w", len(recs), segOffset, err)
		}
		for i := range subRecs {
			subRecs[i].Offset += segOffset
		}
		recs = append(recs, subRecs...)
	}

	return recs, nil
}

// BuildIndexForPiece builds indexes for a given piece CID. The piece must contain a valid deal
// corresponding to an unsealed sector for this method to work. It will try to build index
// using all available deals and will exit as soon as it succeeds for one of the deals
func (ps *PieceDirectory) BuildIndexForPiece(ctx context.Context, pieceCid cid.Cid) error {
	ctx, span := tracing.Tracer.Start(ctx, "pm.build_index_for_piece")
	defer span.End()

	log.Debugw("build index: get piece deals", "pieceCid", pieceCid)

	dls, err := ps.GetPieceDeals(ctx, pieceCid)
	if err != nil {
		return fmt.Errorf("getting piece deals: %w", err)
	}

	if len(dls) == 0 {
		log.Debugw("build index: no deals found for piece", "pieceCid", pieceCid)
		return fmt.Errorf("getting piece deals: no deals found for piece")
	}

	var merr error

	// Iterate over all available deals in case first deal does not have an unsealed sector
	for _, dl := range dls {
		err = ps.addIndexForPieceThrottled(ctx, pieceCid, dl)
		if err == nil {
			return nil
		}
		if dl.IsDirectDeal {
			merr = multierror.Append(merr, fmt.Errorf("adding index for allocation ID %d: %w", dl.ChainDealID, err))
		} else {
			merr = multierror.Append(merr, fmt.Errorf("adding index for piece deal %d: %w", dl.ChainDealID, err))
		}
	}

	return merr
}

func (ps *PieceDirectory) RemoveDealForPiece(ctx context.Context, pieceCid cid.Cid, dealUuid string) error {
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

//func (ps *piecedirectory) deleteIndexForPiece(pieceCid cid.Cid) interface{} {
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
func (ps *PieceDirectory) GetPieceReader(ctx context.Context, pieceCid cid.Cid) (types.SectionReader, error) {
	ctx, span := tracing.Tracer.Start(ctx, "pm.get_piece_reader")
	defer span.End()
	span.SetAttributes(attribute.String("piececid", pieceCid.String()))

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
		reader, err := ps.pieceReader.GetReader(ctx, dl.MinerAddr, dl.SectorID, dl.PieceOffset, dl.PieceLength)
		if err != nil {
			if i < 3 {
				merr = multierror.Append(merr, err)
			}
			continue
		}

		return reader, nil
	}

	return nil, merr
}

type cachedSectionReader struct {
	types.SectionReader
	ps       *PieceDirectory
	pieceCid cid.Cid
	// Signals when the underlying piece reader is ready
	ready chan struct{}
	// err is non-nil if there's an error getting the underlying piece reader
	err error
	// cancel for underlying GetPieceReader call
	cancel  func()
	refs    int
	expired bool
}

func (r *cachedSectionReader) Close() error {
	r.ps.pieceReaderCacheMu.Lock()
	defer r.ps.pieceReaderCacheMu.Unlock()

	r.refs--

	if r.refs == 0 && r.expired {
		log.Debugw("canceling underlying section reader context as cache entry doesn't exist", "piececid", r.pieceCid)

		r.cancel()
	}

	return nil
}

// Get a piece reader that is shared between callers. These readers are most
// performant for random acccess (eg bitswap reads).
// If there is no error, the caller must call Close() on the section reader.
func (ps *PieceDirectory) GetSharedPieceReader(ctx context.Context, pieceCid cid.Cid) (types.SectionReader, error) {
	ctx, span := tracing.Tracer.Start(ctx, "pm.get_shared_piece_reader")
	defer span.End()
	span.SetAttributes(attribute.String("piececid", pieceCid.String()))

	var r *cachedSectionReader

	// Check if there is already a piece reader in the cache
	ps.pieceReaderCacheMu.Lock()
	rr, err := ps.pieceReaderCache.Get(pieceCid.String())
	if err != nil {
		// There is not yet a cached piece reader, create a new one and add it
		// to the cache
		r = &cachedSectionReader{
			ps:       ps,
			pieceCid: pieceCid,
			ready:    make(chan struct{}),
			refs:     1,
		}
		_ = ps.pieceReaderCache.Set(pieceCid.String(), r)
		ps.pieceReaderCacheMu.Unlock()

		// We just added a cached reader, so get its underlying piece reader
		readerCtx, readerCtxCancel := context.WithCancel(context.Background())
		sr, err := ps.GetPieceReader(readerCtx, pieceCid)

		r.SectionReader = sr
		r.err = err
		r.cancel = readerCtxCancel

		// Inform any waiting threads that the cached reader is ready
		close(r.ready)
	} else {

		r = rr.(*cachedSectionReader)
		r.refs++

		ps.pieceReaderCacheMu.Unlock()

		// We already had a cached reader, wait for it to be ready
		select {
		case <-ctx.Done():
			// The context timed out. Deference the cached piece reader and
			// return an error.
			_ = r.Close()
			return nil, ctx.Err()
		case <-r.ready:
		}
	}

	// If there was an error getting the underlying piece reader, make sure
	// that the cached reader gets cleaned up
	if r.err != nil {
		_ = r.Close()
		return nil, r.err
	}

	return r, nil
}

// Get all pieces that contain a multihash (used when retrieving by payload CID)
func (ps *PieceDirectory) PiecesContainingMultihash(ctx context.Context, m mh.Multihash) ([]cid.Cid, error) {
	ctx, span := tracing.Tracer.Start(ctx, "pm.pieces_containing_multihash")
	defer span.End()

	return ps.store.PiecesContainingMultihash(ctx, m)
}

func (ps *PieceDirectory) GetIterableIndex(ctx context.Context, pieceCid cid.Cid) (carindex.IterableIndex, error) {
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
func (ps *PieceDirectory) BlockstoreGet(ctx context.Context, c cid.Cid) ([]byte, error) {
	ctx, span := tracing.Tracer.Start(ctx, "pm.get_block")
	defer span.End()

	// Get the pieces that contain the cid
	pieces, err := ps.PiecesContainingMultihash(ctx, c.Hash())

	// Check if it's an identity cid, if it is, return its digest
	if err != nil {
		digest, ok, iderr := isIdentity(c)
		if iderr == nil && ok {
			return digest, nil
		}
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
			reader, err := ps.GetSharedPieceReader(ctx, pieceCid)
			if err != nil {
				return nil, fmt.Errorf("getting piece reader: %w", err)
			}
			defer reader.Close()

			// Get the offset of the block within the piece (CAR file)
			offsetSize, err := ps.GetOffsetSize(ctx, pieceCid, c.Hash())
			if err != nil {
				return nil, fmt.Errorf("getting offset/size for cid %s in piece %s: %w", c, pieceCid, err)
			}

			// Seek to the section offset
			readerAt := readerutil.NewReadSeekerFromReaderAt(reader, int64(offsetSize.Offset))
			// Read the block data
			readCid, data, err := util.ReadNode(bufio.NewReader(readerAt))
			if err != nil {
				return nil, fmt.Errorf("reading data for block %s from reader for piece %s: %w", c, pieceCid, err)
			}
			if !bytes.Equal(readCid.Hash(), c.Hash()) {
				return nil, fmt.Errorf("read block %s from reader for piece %s, but expected block %s", readCid, pieceCid, c)
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

func (ps *PieceDirectory) BlockstoreGetSize(ctx context.Context, c cid.Cid) (int, error) {
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

	var merr error

	// Iterate over all pieces in case the sector containing the first piece with the Block
	// is not unsealed
	for _, p := range pieces {
		// Get the size of the block from the piece (should be the same for
		// all pieces)
		offsetSize, err := ps.GetOffsetSize(ctx, p, c.Hash())
		if err != nil {
			merr = multierror.Append(merr, fmt.Errorf("getting size of cid %s in piece %s: %w", c, p, err))
			continue
		}

		if offsetSize.Size > 0 {
			return int(offsetSize.Size), nil
		}

		// Indexes imported from the DAG store do not have block size information
		// (they only have offset information). Check if the block size is zero
		// because the index is incomplete.
		isComplete, err := ps.store.IsCompleteIndex(ctx, p)
		if err != nil {
			merr = multierror.Append(merr, fmt.Errorf("getting index complete status for piece %s: %w", p, err))
			continue
		}

		if isComplete {
			// The deal index is complete, so it must be a zero-sized block.
			// A zero-sized block is unusual, but possible.
			return int(offsetSize.Size), nil
		}

		// The index is incomplete, so re-build the index on the fly
		err = ps.BuildIndexForPiece(ctx, p)
		if err != nil {
			merr = multierror.Append(merr, fmt.Errorf("re-building index for piece %s: %w", p, err))
			continue
		}

		// Now get the size again
		offsetSize, err = ps.GetOffsetSize(ctx, p, c.Hash())
		if err != nil {
			merr = multierror.Append(merr, fmt.Errorf("getting size of cid %s in piece %s: %w", c, p, err))
			continue
		}

		return int(offsetSize.Size), nil
	}

	return 0, merr
}

func (ps *PieceDirectory) BlockstoreHas(ctx context.Context, c cid.Cid) (bool, error) {
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
func (ps *PieceDirectory) GetBlockstore(ctx context.Context, pieceCid cid.Cid) (bstore.Blockstore, error) {
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
	carVersion, err := carv2.ReadVersion(reader)
	if err != nil {
		return nil, fmt.Errorf("getting car version for piece %s: %w", pieceCid, err)
	}

	// handle absolute index offsets for carv2.
	var bsR io.ReaderAt
	if carVersion == 2 {
		// this code handles the current 'absolute' index offsets stored by boost.
		// initially, the data looks like [carv2-header carv1-header block block ...]
		// we transform the reader here to look like:
		// [carv1-header [gap of carv2-header-size] block block ...]
		// the carv1 header at the beginning makes the offset used by the subsequent `blockstore.NewReadOnly` work properly.

		// read the carv2 header to get the payload layout
		carReader, err := carv2.NewReader(reader)
		if err != nil {
			return nil, fmt.Errorf("getting car reader for piece %s: %w", pieceCid, err)
		}
		dataOffset := int64(carReader.Header.DataOffset)
		dataSize := int64(carReader.Header.DataSize)

		// read the payload (CARv1) header
		sectionReader := io.NewSectionReader(reader, dataOffset, dataSize)
		carHeader, err := car.ReadHeader(bufio.NewReader(sectionReader))
		if err != nil {
			return nil, fmt.Errorf("reading car header for piece %s: %w", pieceCid, err)
		}

		// write the header back out to a buffer
		headerBuf := bytes.NewBuffer(nil)
		if err := car.WriteHeader(carHeader, headerBuf); err != nil {
			return nil, fmt.Errorf("copying car header for piece %s: %w", pieceCid, err)
		}
		headerLen := int64(headerBuf.Len())

		// create a reader that will address the payload after the header
		sectionReader = io.NewSectionReader(reader, dataOffset+headerLen, dataSize-headerLen)

		bsR = carutil.NewMultiReaderAt(
			bytes.NewReader(headerBuf.Bytes()),        // payload (CARv1) header
			bytes.NewReader(make([]byte, dataOffset)), // padding to account for the CARv2 wrapper
			sectionReader, // payload (CARv1) data
		)
	} else {
		bsR = reader
		if _, err := reader.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("seeking back to start of piece %s: %w", pieceCid, err)
		}
	}

	// Create a blockstore from the index and the piece reader
	bs, err := blockstore.NewReadOnly(bsR, idx, carv2.ZeroLengthSectionAsEOF(true))
	if err != nil {
		return nil, fmt.Errorf("creating blockstore for piece %s: %w", pieceCid, err)
	}

	return bs, nil
}

type SectorAccessorAsPieceReader struct {
	dagstore.SectorAccessor
}

func (s *SectorAccessorAsPieceReader) GetReader(ctx context.Context, minerAddr address.Address, id abi.SectorNumber, offset abi.PaddedPieceSize, length abi.PaddedPieceSize) (types.SectionReader, error) {
	ctx, span := tracing.Tracer.Start(ctx, "sealer.get_reader")
	defer span.End()

	isUnsealed, err := s.SectorAccessor.IsUnsealed(ctx, id, offset.Unpadded(), length.Unpadded())
	if err != nil {
		return nil, fmt.Errorf("checking unsealed state of sector %d: %w", id, err)
	}

	if !isUnsealed {
		return nil, fmt.Errorf("getting reader over sector %d: %w", id, types.ErrSealed)
	}

	r, err := s.SectorAccessor.UnsealSectorAt(ctx, id, offset.Unpadded(), length.Unpadded())
	if err != nil {
		return nil, fmt.Errorf("getting reader over sector %d: %w", id, err)
	}

	return r, nil
}

func isIdentity(c cid.Cid) (digest []byte, ok bool, err error) {
	dmh, err := multihash.Decode(c.Hash())
	if err != nil {
		return nil, false, err
	}
	ok = dmh.Code == multihash.IDENTITY
	digest = dmh.Digest
	return digest, ok, nil
}
