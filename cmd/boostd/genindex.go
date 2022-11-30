package main

import (
	"bufio"
	"fmt"
	"io"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/filecoin-project/boost/cmd/lib"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/boostd-data/model"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/ipfs/go-cid"
	carv1 "github.com/ipld/go-car"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/multiformats/go-varint"
	"github.com/urfave/cli/v2"
)

var genindexCmd = &cli.Command{
	Name:  "genindex",
	Usage: "Generate index for a given piececid and store in the piece directory",
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:  "add-index-throttle",
			Usage: "the maximum number of add index operations that can run in parallel",
			Value: 4,
		},
		&cli.StringFlag{
			Name:     "api-fullnode",
			Usage:    "the endpoint for the full node API",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "api-storage",
			Usage:    "the endpoint for the storage node API",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "api-piece-directory",
			Usage:    "the endpoint for the piece directory API",
			Required: true,
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)

		if !cctx.Args().Present() {
			return lcli.ShowHelp(cctx, fmt.Errorf("must specify piece cid"))
		}

		piececid, err := cid.Decode(cctx.Args().First())
		if err != nil {
			return err
		}

		fmt.Println("piececid to fetch from lotus: ", piececid)

		filename := piececid.String()

		fmt.Println("filename to read: ", filename)

		// Connect to the full node API
		fnApiInfo := cctx.String("api-fullnode")
		fullnodeApi, ncloser, err := lib.GetFullNodeApi(ctx, fnApiInfo, log)
		if err != nil {
			return fmt.Errorf("getting full node API: %w", err)
		}
		defer ncloser()

		// Connect to the storage API and create a sector accessor
		storageApiInfo := cctx.String("api-storage")
		sa, storageCloser, err := lib.CreateSectorAccessor(ctx, storageApiInfo, fullnodeApi, log)
		if err != nil {
			return err
		}
		defer storageCloser()

		pr := &piecedirectory.SectorAccessorAsPieceReader{SectorAccessor: sa}

		// Connect to the piece directory service
		pdClient := piecedirectory.NewStore()
		defer pdClient.Close(ctx)
		err = pdClient.Dial(ctx, cctx.String("api-piece-directory"))
		if err != nil {
			return fmt.Errorf("connecting to piece directory service: %w", err)
		}

		piecedirectory := piecedirectory.NewPieceDirectory(pdClient, pr, cctx.Int("add-index-throttle"))

		_ = piecedirectory

		// Load the index file and store in Piece Directory similar to how we migrate indices
		{
			//TODO: idealy store is something generic for leveldb or couchbase
			//store := couchbase.NewStore(settings)

			// gen index
			err := IndexCar(filename)
			if err != nil {
				panic(err)
			}

			//idx, err := car.ReadOrGenerateIndex(reader, car.ZeroLengthSectionAsEOF(true), car.StoreIdentityCIDs(true))
			//if err != nil {
			//return err
			//}

			//itidx, ok := idx.(index.IterableIndex)
			//if !ok {
			//return fmt.Errorf("index is not iterable for piece %s", piececid)
			//}

			//// Convert from IterableIndex to an array of records
			//records, err := getRecords(itidx)
			//if err != nil {
			//return fmt.Errorf("getting records for index: %w", err)
			//}

			//// Add the index to the store
			//addStart := time.Now()
			//err = store.AddIndex(ctx, pieceCid, records)
			//if err != nil {
			//return fmt.Errorf("adding index to store: %w", err)
			//}
			//log.Debugw("AddIndex", "took", time.Since(addStart).String())
		}

		return nil
	},
}

func loadIndex(path string) (index.Index, error) {
	defer func(now time.Time) {
		log.Debugw("loadindex", "took", time.Since(now))
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

func getRecords(subject index.Index) ([]model.Record, error) {
	records := make([]model.Record, 0)

	switch idx := subject.(type) {
	case index.IterableIndex:
		err := idx.ForEach(func(m multihash.Multihash, offset uint64) error {

			cid := cid.NewCidV1(cid.Raw, m)

			records = append(records, model.Record{
				Cid: cid,
				OffsetSize: model.OffsetSize{
					Offset: offset,
					Size:   0,
				},
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

// IndexCar is a command to add an index to a car
func IndexCar(filename string) error {
	r, err := carv2.OpenReader(filename)
	if err != nil {
		return fmt.Errorf("open reader: %w", err)
	}
	defer r.Close()

	//if c.Int("version") == 1 {
	//if c.IsSet("codec") && c.String("codec") != "none" {
	//return fmt.Errorf("'none' is the only supported codec for a v1 car")
	//}
	//outStream := os.Stdout
	//if c.Args().Len() >= 2 {
	//outStream, err = os.Create(c.Args().Get(1))
	//if err != nil {
	//return err
	//}
	//}
	//defer outStream.Close()

	//dr, err := r.DataReader()
	//if err != nil {
	//return err
	//}
	//_, err = io.Copy(outStream, dr)
	//return err
	//}

	//if c.Int("version") != 2 {
	//return fmt.Errorf("invalid CAR version %d", c.Int("version"))
	//}

	codec := "car-multihash-index-sorted"
	var idx index.Index
	var mc multicodec.Code
	if err := mc.Set(codec); err != nil {
		return err
	}
	idx, err = index.New(mc)
	if err != nil {
		return err
	}

	//outStream := os.Stdout
	//if c.Args().Len() >= 2 {
	//outStream, err = os.Create(c.Args().Get(1))
	//if err != nil {
	//return err
	//}
	//}
	//defer outStream.Close()

	v1r, err := r.DataReader()
	if err != nil {
		return fmt.Errorf("data reader: %w", err)
	}

	if r.Version == 1 {
		fi, err := os.Stat(filename)
		if err != nil {
			return fmt.Errorf("os.stat err: %w", err)
		}
		r.Header.DataSize = uint64(fi.Size())
	}
	v2Header := carv2.NewHeader(r.Header.DataSize)
	//if c.String("codec") == "none" {
	v2Header.IndexOffset = 0
	//if _, err := outStream.Write(carv2.Pragma); err != nil {
	//return err
	//}
	//if _, err := v2Header.WriteTo(outStream); err != nil {
	//return err
	//}
	//if _, err := io.Copy(outStream, v1r); err != nil {
	//return err
	//}
	//return nil
	//}

	//if _, err := outStream.Write(carv2.Pragma); err != nil {
	//return err
	//}
	//if _, err := v2Header.WriteTo(outStream); err != nil {
	//return err
	//}

	// collect records as we go through the v1r
	br := bufio.NewReader(v1r)
	hdr, err := carv1.ReadHeader(br)
	if err != nil {
		return fmt.Errorf("error reading car header: %w", err)
	}
	_ = hdr
	//if err := carv1.WriteHeader(hdr, outStream); err != nil {
	//return err
	//}

	records := make([]index.Record, 0)
	var sectionOffset int64
	if sectionOffset, err = v1r.Seek(0, io.SeekCurrent); err != nil {
		return err
	}
	sectionOffset -= int64(br.Buffered())

	for {
		// Read the section's length.
		sectionLen, err := varint.ReadUvarint(br)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("reading uvarint: %w", err)
		}
		//if _, err := outStream.Write(varint.ToUvarint(sectionLen)); err != nil {
		//return err
		//}

		// Null padding; by default it's an error.
		// TODO: integrate corresponding ReadOption
		if sectionLen == 0 {
			// TODO: pad writer to expected length.
			break
		}

		// Read the CID.
		cidLen, c, err := cid.CidFromReader(br)
		if err != nil {
			return fmt.Errorf("cidfromreader err: %w", err)
		}
		records = append(records, index.Record{Cid: c, Offset: uint64(sectionOffset)})
		//if _, err := c.WriteBytes(outStream); err != nil {
		//return err
		//}

		// Seek to the next section by skipping the block.
		// The section length includes the CID, so subtract it.
		remainingSectionLen := int64(sectionLen) - int64(cidLen)
		if _, err := io.CopyN(io.Discard, br, remainingSectionLen); err != nil {
			return err
		}

		sectionOffset += int64(sectionLen) + int64(varint.UvarintSize(sectionLen))

		fmt.Println("record: ", c, sectionOffset)
	}

	if err := idx.Load(records); err != nil {
		return fmt.Errorf("loading records: %w", err)
	}

	//_, err = index.WriteTo(idx, outStream)
	//return err

	return nil
}
