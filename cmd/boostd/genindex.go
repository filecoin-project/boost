package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	_ "net/http/pprof"
	"time"

	"github.com/filecoin-project/boost/cmd/lib"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/boostd-data/model"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/ipfs/go-cid"
	carv1 "github.com/ipld/go-car"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/multiformats/go-varint"
	"github.com/urfave/cli/v2"
)

var genindexCmd = &cli.Command{
	Name:  "genindex",
	Usage: "Generate index for a given piececid and store it in the piece directory",
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:  "add-index-throttle",
			Usage: "the maximum number of add index operations that can run in parallel",
			Value: 4,
		},
		&cli.StringFlag{
			Name:     "api-fullnode",
			Usage:    "the endpoint for the full node API",
			Required: false,
		},
		&cli.StringFlag{
			Name:     "api-storage",
			Usage:    "the endpoint for the storage node API",
			Required: false,
		},
		&cli.StringFlag{
			Name:     "api-piece-directory",
			Usage:    "the endpoint for the piece directory API",
			Required: false,
		},
		&cli.StringFlag{
			Name:     "piece-cid",
			Usage:    "piece-cid to index",
			Required: true,
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)

		// parse piececid
		piececid, err := cid.Decode(cctx.String("piece-cid"))
		if err != nil {
			return err
		}
		fmt.Println("piece-cid to index: ", piececid)

		// connect to the piece directory service
		pdClient := piecedirectory.NewStore()
		defer pdClient.Close(ctx)
		err = pdClient.Dial(ctx, cctx.String("api-piece-directory"))
		if err != nil {
			return fmt.Errorf("error while connecting to piece directory service: %w", err)
		}

		// connect to the full node
		fnApiInfo := cctx.String("api-fullnode")
		fullnodeApi, ncloser, err := lib.GetFullNodeApi(ctx, fnApiInfo, log)
		if err != nil {
			return fmt.Errorf("getting full node API: %w", err)
		}
		defer ncloser()

		// connect to the storage API and create a sector accessor
		storageApiInfo := cctx.String("api-storage")
		sa, storageCloser, err := lib.CreateSectorAccessor(ctx, storageApiInfo, fullnodeApi, log)
		if err != nil {
			return err
		}
		defer storageCloser()

		pr := &piecedirectory.SectorAccessorAsPieceReader{SectorAccessor: sa}

		pd := piecedirectory.NewPieceDirectory(pdClient, pr, cctx.Int("add-index-throttle"))

		var r *carv2.Reader
		var headerDataSize uint64

		fmt.Println("fetching piece-cid from the storage api")

		sr, err := pd.GetPieceReader(ctx, piececid)
		if err != nil {
			if errors.Is(err, piecedirectory.ErrNoUnsealedPieceAvailable) {
				return fmt.Errorf("piece reader could not find unsealed piece for cid %s: %w", piececid, err)
			}
			return fmt.Errorf("error while getting piece reader: %w", err)
		}

		r, err = carv2.NewReader(sr)
		if err != nil {
			return err
		}

		// gen index
		recs, err := GetRecords(r, headerDataSize)
		if err != nil {
			return err
		}

		addStart := time.Now()

		fmt.Printf("about to add %d records for piece-cid %s to the piece directory\n", len(recs), piececid)
		err = pd.AddIndex(ctx, piececid, recs)
		if err != nil {
			return err
		}

		//TODO: maybe set car size?
		//SetCarSize(ctx context.Context, pieceCid cid.Cid, size uint64) error

		fmt.Println("adding index took", time.Since(addStart).String())

		fmt.Printf("successfully added index (%d records) for piece-cid %s to the piece directory\n", len(recs), piececid)

		return nil
	},
}

func GetRecords(r *carv2.Reader, headerDataSize uint64) ([]model.Record, error) {
	v1r, err := r.DataReader()
	if err != nil {
		return nil, fmt.Errorf("data reader: %w", err)
	}

	if r.Version == 1 {
		r.Header.DataSize = headerDataSize
	}
	v2Header := carv2.NewHeader(r.Header.DataSize)
	v2Header.IndexOffset = 0

	// collect records as we go through the v1r
	br := bufio.NewReader(v1r)
	_, err = carv1.ReadHeader(br)
	if err != nil {
		return nil, fmt.Errorf("error reading car header: %w", err)
	}

	var records []model.Record
	var sectionOffset int64
	if sectionOffset, err = v1r.Seek(0, io.SeekCurrent); err != nil {
		return nil, err
	}
	sectionOffset -= int64(br.Buffered())

	for {
		// Read the section's length.
		sectionLen, err := varint.ReadUvarint(br)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("reading uvarint: %w", err)
		}
		if sectionLen == 0 {
			break
		}

		// Read the CID.
		cidLen, c, err := cid.CidFromReader(br)
		if err != nil {
			return nil, fmt.Errorf("cidfromreader err: %w", err)
		}

		// Seek to the next section by skipping the block.
		// The section length includes the CID, so subtract it.
		remainingSectionLen := int64(sectionLen) - int64(cidLen)
		if _, err := io.CopyN(io.Discard, br, remainingSectionLen); err != nil {
			return nil, err
		}

		records = append(records, model.Record{
			Cid: c,
			OffsetSize: model.OffsetSize{
				Offset: uint64(sectionOffset),
				Size:   uint64(remainingSectionLen), // TODO: must confirm this field is correct
			},
		})

		sectionOffset += int64(sectionLen) + int64(varint.UvarintSize(sectionLen))
	}

	return records, nil
}
