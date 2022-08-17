package indexbs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/filecoin-project/dagstore"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/filecoin-project/go-state-types/abi"
	blocks "github.com/ipfs/go-block-format"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car/v2"
	"github.com/multiformats/go-multihash"

	"github.com/filecoin-project/dagstore/mount"
	"github.com/hashicorp/go-multierror"
	lru "github.com/hashicorp/golang-lru"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	carbs "github.com/ipld/go-car/v2/blockstore"
)

var logbs = logging.Logger("dagstore-all-readblockstore")

var ErrBlockNotFound = errors.New("block not found")
var ErrNotFound = errors.New("not found")

var _ blockstore.Blockstore = (*IndexBackedBlockstore)(nil)

// ErrNoPieceSelected means that the piece selection function rejected all of the given pieces.
var ErrNoPieceSelected = errors.New("no piece selected")

// PieceSelectorF helps select a piece to fetch a cid from if the given cid is present in multiple pieces.
// It should return `ErrNoPieceSelected` if none of the given piece is selected.
type PieceSelectorF func(c cid.Cid, pieceCids []cid.Cid) (cid.Cid, error)

type accessorWithBlockstore struct {
	mount mount.Reader
	bs    dagstore.ReadBlockstore
}

type IndexBackedBlockstoreAPI interface {
	PiecesContainingMultihash(mh multihash.Multihash) ([]cid.Cid, error)
	GetMaxPieceOffset(pieceCid cid.Cid) (uint64, error)
	GetPieceInfo(pieceCID cid.Cid) (*piecestore.PieceInfo, error)
	IsUnsealed(ctx context.Context, sectorID abi.SectorNumber, offset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (bool, error)
	UnsealSectorAt(ctx context.Context, sectorID abi.SectorNumber, pieceOffset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (mount.Reader, error)
}

// IndexBackedBlockstore is a read only blockstore over all cids across all pieces on a provider.
type IndexBackedBlockstore struct {
	api          IndexBackedBlockstoreAPI
	pieceSelectF PieceSelectorF

	bsStripedLocks  [256]sync.Mutex
	blockstoreCache *lru.Cache // caches the blockstore for a given piece for piece read affinity i.e. further reads will likely be from the same piece. Maps (piece CID -> blockstore).
}

func NewIndexBackedBlockstore(api IndexBackedBlockstoreAPI, pieceSelector PieceSelectorF, maxCacheSize int) (blockstore.Blockstore, error) {
	// instantiate the blockstore cache
	bslru, err := lru.NewWithEvict(maxCacheSize, func(_ interface{}, val interface{}) {
		// ensure we close the blockstore for a piece when it's evicted from the cache so dagstore can gc it.
		abs := val.(*accessorWithBlockstore)
		abs.mount.Close()
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create lru cache for read only blockstores")
	}

	return &IndexBackedBlockstore{
		api:             api,
		pieceSelectF:    pieceSelector,
		blockstoreCache: bslru,
	}, nil
}

func (ro *IndexBackedBlockstore) Get(ctx context.Context, c cid.Cid) (b blocks.Block, finalErr error) {
	logbs.Debugw("Get called", "cid", c)
	defer func() {
		if finalErr != nil {
			logbs.Debugw("Get: got error", "cid", c, "error", finalErr)
		}
	}()

	mhash := c.Hash()

	// fetch all the pieceCIDs containing the multihash
	pieceCIDs, err := ro.api.PiecesContainingMultihash(mhash)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch pieces containing the block: %w", err)
	}
	if len(pieceCIDs) == 0 {
		return nil, ErrBlockNotFound
	}

	// do we have a cached blockstore for a piece containing the required block ? If yes, serve the block from that blockstore
	for _, pieceCID := range pieceCIDs {
		lk := &ro.bsStripedLocks[pieceCIDToStriped(pieceCID)]
		lk.Lock()

		blk, err := ro.readFromBSCacheUnlocked(ctx, c, pieceCID)
		if err == nil && blk != nil {
			logbs.Debugw("Get: returning from block store cache", "cid", c)

			lk.Unlock()
			return blk, nil
		}

		lk.Unlock()
	}

	// ---- we don't have a cached blockstore for a piece that can serve the block -> let's build one.

	// select a valid piece that can serve the retrieval
	pieceCID, err := ro.pieceSelectF(c, pieceCIDs)
	if err != nil && err == ErrNoPieceSelected {
		return nil, ErrBlockNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to run piece selection function: %w", err)
	}

	lk := &ro.bsStripedLocks[pieceCIDToStriped(pieceCID)]
	lk.Lock()
	defer lk.Unlock()

	// see if we have blockstore in the cache we can serve the retrieval from as the previous code in this critical section
	// could have added a blockstore to the cache for the given piece CID.
	blk, err := ro.readFromBSCacheUnlocked(ctx, c, pieceCID)
	if err == nil && blk != nil {
		return blk, nil
	}

	// load blockstore for the selected piece and try to serve the cid from that blockstore.
	reader, err := ro.getPieceContent(ctx, pieceCID)

	bs, err := ro.getBlockstore(reader)

	blk, err = bs.Get(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}

	// update the block cache and the blockstore cache
	ro.blockstoreCache.Add(pieceCID, &accessorWithBlockstore{reader, bs})

	logbs.Debugw("Get: returning after creating new blockstore", "cid", c)
	return blk, nil
}

func (ro *IndexBackedBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	logbs.Debugw("Has called", "cid", c)

	// if there is a piece that can serve the retrieval for the given cid, we have the requested cid
	// and has should return true.
	mhash := c.Hash()

	pieceCIDs, err := ro.api.PiecesContainingMultihash(mhash)
	if err != nil {
		logbs.Debugw("Has error", "cid", c, "err", err)
		return false, nil
	}
	if len(pieceCIDs) == 0 {
		logbs.Debugw("Has: returning false no error", "cid", c)
		return false, nil
	}

	_, err = ro.pieceSelectF(c, pieceCIDs)
	if err != nil && err == ErrNoPieceSelected {
		logbs.Debugw("Has error", "cid", c, "err", err)
		return false, nil
	}
	if err != nil {
		logbs.Debugw("Has error", "cid", c, "err", err)
		return false, fmt.Errorf("failed to run piece selection function: %w", err)
	}

	logbs.Debugw("Has: returning true", "cid", c)
	return true, nil
}

func (ro *IndexBackedBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	logbs.Debugw("GetSize called", "cid", c)

	blk, err := ro.Get(ctx, c)
	if err != nil {
		logbs.Debugw("GetSize error", "cid", c, "err", err)
		return 0, fmt.Errorf("failed to get block: %w", err)
	}

	logbs.Debugw("GetSize success", "cid", c)
	return len(blk.RawData()), nil
}

func (ro *IndexBackedBlockstore) readFromBSCacheUnlocked(ctx context.Context, c cid.Cid, pieceCid cid.Cid) (blocks.Block, error) {
	// We've already ensured that the given piece has the cid/multihash we are looking for.
	val, ok := ro.blockstoreCache.Get(pieceCid)
	if !ok {
		return nil, ErrBlockNotFound
	}

	rbs := val.(*accessorWithBlockstore).bs
	blk, err := rbs.Get(ctx, c)
	if err != nil {
		// we know that the cid we want to lookup belongs to a piece with given pieceCID and
		// so if we fail to get the corresponding block from the blockstore for that piece, something has gone wrong
		// and we should remove the blockstore for that pieceCID from our cache.
		ro.blockstoreCache.Remove(pieceCid)
		return nil, err
	}

	return blk, nil
}

func pieceCIDToStriped(pieceCid cid.Cid) byte {
	return pieceCid.String()[len(pieceCid.String())-1]
}

// --- UNSUPPORTED BLOCKSTORE METHODS -------
func (ro *IndexBackedBlockstore) DeleteBlock(context.Context, cid.Cid) error {
	return errors.New("unsupported operation DeleteBlock")
}
func (ro *IndexBackedBlockstore) HashOnRead(_ bool) {}
func (ro *IndexBackedBlockstore) Put(context.Context, blocks.Block) error {
	return errors.New("unsupported operation Put")
}
func (ro *IndexBackedBlockstore) PutMany(context.Context, []blocks.Block) error {
	return errors.New("unsupported operation PutMany")
}
func (ro *IndexBackedBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, errors.New("unsupported operation AllKeysChan")
}

func (ro *IndexBackedBlockstore) getPieceContent(ctx context.Context, pieceCid cid.Cid) (mount.Reader, error) {
	// Get the deals for the piece
	pieceInfo, err := ro.api.GetPieceInfo(pieceCid)
	if err != nil {
		return nil, fmt.Errorf("getting sector info for piece %s: %w", pieceCid, err)
	}

	// Get the first unsealed deal
	di, err := ro.unsealedDeal(ctx, *pieceInfo)
	if err != nil {
		return nil, fmt.Errorf("getting unsealed CAR file: %w", err)
	}

	// Get the raw piece data from the sector
	pieceReader, err := ro.api.UnsealSectorAt(ctx, di.SectorID, di.Offset.Unpadded(), di.Length.Unpadded())
	if err != nil {
		return nil, fmt.Errorf("getting raw data from sector %d: %w", di.SectorID, err)
	}

	return pieceReader, nil
}

func (ro *IndexBackedBlockstore) getBlockstore(pieceReader mount.Reader) (dagstore.ReadBlockstore, error) {
	idx, err := car.ReadOrGenerateIndex(pieceReader, car.ZeroLengthSectionAsEOF(true), car.StoreIdentityCIDs(true))
	if err != nil {
		return nil, err
	}
	return carbs.NewReadOnly(pieceReader, idx, car.ZeroLengthSectionAsEOF(true))
}

func (ro *IndexBackedBlockstore) unsealedDeal(ctx context.Context, pieceInfo piecestore.PieceInfo) (*piecestore.DealInfo, error) {
	// There should always been deals in the PieceInfo, but check just in case
	if len(pieceInfo.Deals) == 0 {
		return nil, fmt.Errorf("there are no deals containing piece %s: %w", pieceInfo.PieceCID, ErrNotFound)
	}

	// The same piece can be in many deals. Find the first unsealed deal.
	sealedCount := 0
	var allErr error
	for _, di := range pieceInfo.Deals {
		isUnsealed, err := ro.api.IsUnsealed(ctx, di.SectorID, di.Offset.Unpadded(), di.Length.Unpadded())
		if err != nil {
			allErr = multierror.Append(allErr, err)
			continue
		}
		if isUnsealed {
			return &di, nil
		}
		sealedCount++
	}

	// Try to return an error message with as much useful information as possible
	dealSectors := make([]string, 0, len(pieceInfo.Deals))
	for _, di := range pieceInfo.Deals {
		dealSectors = append(dealSectors, fmt.Sprintf("Deal %d: Sector %d", di.DealID, di.SectorID))
	}

	if allErr == nil {
		dealSectorsErr := fmt.Errorf("%s: %w", strings.Join(dealSectors, ", "), ErrNotFound)
		return nil, fmt.Errorf("checked unsealed status of %d deals containing piece %s: none are unsealed: %w",
			len(pieceInfo.Deals), pieceInfo.PieceCID, dealSectorsErr)
	}

	if len(pieceInfo.Deals) == 1 {
		return nil, fmt.Errorf("checking unsealed status of deal %d (sector %d) containing piece %s: %w",
			pieceInfo.Deals[0].DealID, pieceInfo.Deals[0].SectorID, pieceInfo.PieceCID, allErr)
	}

	if sealedCount == 0 {
		return nil, fmt.Errorf("checking unsealed status of %d deals containing piece %s: %s: %w",
			len(pieceInfo.Deals), pieceInfo.PieceCID, dealSectors, allErr)
	}

	return nil, fmt.Errorf("checking unsealed status of %d deals containing piece %s - %d are sealed, %d had errors: %s: %w",
		len(pieceInfo.Deals), pieceInfo.PieceCID, sealedCount, len(pieceInfo.Deals)-sealedCount, dealSectors, allErr)
}
