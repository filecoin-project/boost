package car

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-car"
	"github.com/ipld/go-car/util"
)

// CarOffsetWriter turns a blockstore and a root CID into a CAR file stream,
// starting from an offset. The traversal is depth-first. Other selectors are
// not yet supported.
type CarOffsetWriter struct {
	payloadCid cid.Cid
	nodeGetter format.NodeGetter
	blockInfos *BlockInfoCache
	header     car.CarHeader
}

func NewCarOffsetWriter(payloadCid cid.Cid, bstore blockstore.Blockstore, blockInfos *BlockInfoCache) *CarOffsetWriter {
	ng := merkledag.NewDAGService(blockservice.New(bstore, offline.Exchange(bstore)))
	return &CarOffsetWriter{
		payloadCid: payloadCid,
		nodeGetter: ng,
		blockInfos: blockInfos,
		header:     carHeader(payloadCid),
	}
}

func carHeader(payloadCid cid.Cid) car.CarHeader {
	return car.CarHeader{
		Roots:   []cid.Cid{payloadCid},
		Version: 1,
	}
}

// Write writes the CAR file to the writer, starting from writeOffset
func (s *CarOffsetWriter) Write(ctx context.Context, w io.Writer, writeOffset uint64) error {
	headerSize, err := s.writeHeader(w, writeOffset)
	if err != nil {
		return err
	}

	return s.writeBlocks(ctx, w, headerSize, writeOffset)
}

// writeHeader writes the header to the writer, starting from writeOffset
func (s *CarOffsetWriter) writeHeader(w io.Writer, writeOffset uint64) (uint64, error) {
	headerSize, err := car.HeaderSize(&s.header)
	if err != nil {
		return 0, fmt.Errorf("failed to size car header: %w", err)
	}

	// Check if the offset from which to start writing is after the header
	if writeOffset >= headerSize {
		return headerSize, nil
	}

	// Write out the header, starting at the offset
	_, err = skipWrite(w, writeOffset, func(sw io.Writer) (int, error) {
		return 0, car.WriteHeader(&s.header, sw)
	})
	if err != nil {
		return 0, fmt.Errorf("failed to write car header: %w", err)
	}

	return headerSize, nil
}

// writeBlocks does a depth first search of the blocks in the blockstore,
// starting at the root, and writes the blocks to the writer, starting from
// writeOffset
func (s *CarOffsetWriter) writeBlocks(ctx context.Context, w io.Writer, headerSize uint64, writeOffset uint64) error {
	// The first block's offset is the size of the header
	offset := headerSize

	// This function gets called for each CID during the merkle DAG walk
	nextCid := func(ctx context.Context, c cid.Cid) ([]*format.Link, error) {
		// There will be an item in the cache if writeBlocks has already been
		// called before, and the DAG traversal reached this CID
		cached, ok := s.blockInfos.Get(c)
		if ok {
			// Check if the offset from which to start writing is after this
			// block
			nextBlockOffset := cached.offset + cached.size
			if writeOffset >= nextBlockOffset {
				// The offset from which to start writing is after this block
				// so don't write anything, just skip over this block
				offset = nextBlockOffset
				return cached.links, nil
			}
		}

		// Get the block from the blockstore
		nd, err := s.nodeGetter.Get(ctx, c)
		if err != nil {
			return nil, fmt.Errorf("getting block %s: %w", c, err)
		}

		// take a copy of the links array before nd.RawData() triggers a sort
		links := make([]*format.Link, len(nd.Links()))
		copy(links, nd.Links())
		byts := nd.RawData()

		// Get the size of the block and metadata
		ldsize := util.LdSize(nd.Cid().Bytes(), byts)

		// Check if the offset from which to start writing is in or before this
		// block
		nextBlockOffset := offset + ldsize
		if writeOffset < nextBlockOffset {
			// Check if the offset from which to start writing is in this block
			var blockWriteOffset uint64
			if writeOffset >= offset {
				blockWriteOffset = writeOffset - offset
			}

			// Write the block data to the writer, starting at the block offset
			_, err = skipWrite(w, blockWriteOffset, func(sw io.Writer) (int, error) {
				return 0, util.LdWrite(sw, nd.Cid().Bytes(), byts)
			})
			if err != nil {
				return nil, fmt.Errorf("writing CAR block %s: %w", c, err)
			}
		}

		// Add the block to the cache
		s.blockInfos.Put(nd.Cid(), &BlockInfo{
			offset: offset,
			size:   ldsize,
			links:  links,
		})

		offset = nextBlockOffset

		// Return any links from this block to other DAG blocks
		return links, nil
	}

	seen := cid.NewSet()
	return merkledag.Walk(ctx, nextCid, s.payloadCid, seen.Visit)
}

// Write data to the writer, skipping the first skip bytes
func skipWrite(w io.Writer, skip uint64, write func(sw io.Writer) (int, error)) (int, error) {
	// If there's nothing to skip, just do a normal write
	if skip == 0 {
		return write(w)
	}

	// Write to a buffer
	var buff bytes.Buffer
	if count, err := write(&buff); err != nil {
		return count, err
	}

	// Write the buffer to the writer, skipping the first skip bytes
	bz := buff.Bytes()
	if skip >= uint64(len(bz)) {
		return 0, nil
	}
	return w.Write(bz[skip:])
}
