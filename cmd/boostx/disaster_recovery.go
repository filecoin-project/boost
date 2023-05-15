package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/filecoin-project/boost/cmd/lib"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-commp-utils/writer"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin/v9/miner"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/markets/dagstore"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-cidutil/cidenc"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/mitchellh/go-homedir"
	"github.com/multiformats/go-multibase"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var (
	dr          *DisasterRecovery
	sa          dagstore.SectorAccessor
	fullnodeApi v1api.FullNode
	pd          *piecedirectory.PieceDirectory
	maddr       address.Address

	ignoreCommp bool
	ignoreLID   bool
)

var disasterRecoveryCmd = &cli.Command{
	Name:  "disaster-recovery",
	Usage: "Disaster Recovery commands",
	Subcommands: []*cli.Command{
		restorePieceStoreCmd,
	},
}

var restorePieceStoreCmd = &cli.Command{
	Name:   "restore-piece-store",
	Usage:  "Restore Piece store",
	Before: before,
	Flags: []cli.Flag{
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
			Name:  "api-lid",
			Usage: "the endpoint for the LID API",
			//Required: true,
		},
		&cli.StringFlag{
			Name:  "disaster-recovery-dir",
			Usage: "location to store progress of disaster recovery",
			Value: "~/.boost-disaster-recovery",
		},
		&cli.IntFlag{
			Name:  "sector-id",
			Usage: "sector-id",
		},
		&cli.IntFlag{
			Name:  "add-index-throttle",
			Usage: "",
			Value: 4,
		},
		&cli.BoolFlag{
			Name:  "ignore-commp",
			Usage: "whether we should ignore sanity check of local data vs chain data",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "ignore-lid",
			Usage: "whether we should ignore lid",
			Value: false,
		},
	},
	Action: func(cctx *cli.Context) error {
		c := make(chan os.Signal)
		errc := make(chan error)

		signal.Notify(c, os.Interrupt, syscall.SIGTERM)

		go func() {
			errc <- action(cctx)
		}()

		var err error
		select {
		case <-c:
		case err = <-errc:
		}

		err2 := dr.WriteReport()
		if err2 != nil {
			return err2
		}

		if err != nil {
			return err
		}

		return nil
	},
}

func action(cctx *cli.Context) error {
	ctx := lcli.ReqContext(cctx)

	var err error
	dr, err = NewDisasterRecovery(cctx.String("disaster-recovery-dir"))
	if err != nil {
		return err
	}

	var sectorid abi.SectorNumber
	if cctx.IsSet("sector-id") {
		sectorid = abi.SectorNumber(cctx.Uint64("sector-id"))
		fmt.Println("sector id: ", sectorid)
	}

	ignoreCommp = cctx.Bool("ignore-commp")
	ignoreLID = cctx.Bool("ignore-lid")

	// Connect to the full node API
	fnApiInfo := cctx.String("api-fullnode")
	var ncloser jsonrpc.ClientCloser
	fullnodeApi, ncloser, err = lib.GetFullNodeApi(ctx, fnApiInfo, log)
	if err != nil {
		return fmt.Errorf("getting full node API: %w", err)
	}
	defer ncloser()

	// Connect to the storage API and create a sector accessor
	storageApiInfo := cctx.String("api-storage")
	var storageCloser jsonrpc.ClientCloser
	sa, storageCloser, err = lib.CreateSectorAccessor(ctx, storageApiInfo, fullnodeApi, log)
	if err != nil {
		return err
	}
	defer storageCloser()

	// Connect to the local index directory service
	if ignoreLID {
		pd = nil
	} else {
		pdClient := piecedirectory.NewStore()
		defer pdClient.Close(ctx)
		err = pdClient.Dial(ctx, cctx.String("api-lid"))
		if err != nil {
			return fmt.Errorf("connecting to local index directory service: %w", err)
		}
		pr := &piecedirectory.SectorAccessorAsPieceReader{SectorAccessor: sa}
		pd = piecedirectory.NewPieceDirectory(pdClient, pr, cctx.Int("add-index-throttle"))
	}

	maddr, err = getActorAddress(ctx, cctx)
	if err != nil {
		return err
	}

	sectors, err := fullnodeApi.StateMinerSectors(ctx, maddr, nil, types.EmptyTSK)
	if err != nil {
		return err
	}

	dr.TotalSectors = len(sectors)

	var sectorsWithDeals []*miner.SectorOnChainInfo

	for _, info := range sectors {
		if cctx.IsSet("sector-id") && info.SectorNumber != sectorid {
			continue
		}

		// ignore sector 0
		if info.SectorNumber == abi.SectorNumber(0) {
			continue
		}

		if len(info.DealIDs) < 1 {
			fmt.Println("no deals in sector", info.SectorNumber)

			dr.SectorsWithoutDeals = append(dr.SectorsWithoutDeals, uint64(info.SectorNumber))
			continue
		}

		dr.SectorsWithDeals = append(dr.SectorsWithDeals, uint64(info.SectorNumber))
		sectorsWithDeals = append(sectorsWithDeals, info)
	}

	for _, info := range sectorsWithDeals {
		dr.Sectors[uint64(info.SectorNumber)] = &SectorStatus{}

		if dr.IsDone(info.SectorNumber) {
			fmt.Println("sector already processed", info.SectorNumber)
			dr.Sectors[uint64(info.SectorNumber)].AlreadyProcessed = true
			continue
		}

		ok, isUnsealed, err := processSector(ctx, info)
		if err != nil {
			return err
		}
		if !isUnsealed {
			fmt.Println("sector is not unsealed", info.SectorNumber)
			continue
		}
		if !ok {
			return errors.New("weird -- not ok, but sector is unsealed and no error?!")
		}
	}

	return nil
}

type DisasterRecovery struct {
	Dir     string // main disaster recovery dir - keeps progress on recovery
	DoneDir string

	TotalSectors int
	PieceErrors  int

	SectorsWithDeals    []uint64
	SectorsWithoutDeals []uint64

	Sectors map[uint64]*SectorStatus
}

type SectorStatus struct {
	AlreadyProcessed bool

	Deals map[uint64]*PieceStatus

	ProcessingTook time.Duration
}

type PieceStatus struct {
	PieceCID      cid.Cid
	PieceSize     abi.PaddedPieceSize
	PieceOffset   abi.UnpaddedPieceSize
	IsUnsealed    bool
	GotDataReader bool
	Error         string

	ProcessingTook time.Duration
}

func NewDisasterRecovery(dir string) (*DisasterRecovery, error) {
	drDir, err := homedir.Expand(dir)
	if err != nil {
		return nil, fmt.Errorf("expanding disaster recovery dir path: %w", err)
	}
	if drDir == "" {
		return nil, errors.New("disaster-recovery-dir is a required flag")
	}

	d, err := os.Stat(drDir)
	if err == nil {
		if d.IsDir() {
			fmt.Println("WARNING: disaster recovery dir exists, so tool will continue from where it left off previously!!!")
		}
	}

	err = os.MkdirAll(drDir, 0755)
	if err != nil {
		return nil, err
	}

	doneDir := path.Join(drDir, "done")
	err = os.MkdirAll(doneDir, 0755)
	if err != nil {
		return nil, err
	}

	return &DisasterRecovery{
		Dir:     drDir,
		DoneDir: doneDir,
		Sectors: make(map[uint64]*SectorStatus),
	}, nil
}

func (dr *DisasterRecovery) IsDone(s abi.SectorNumber) bool {
	f := fmt.Sprintf("%s/%d", dr.DoneDir, s)

	_, err := os.Stat(f)

	return !os.IsNotExist(err)
}

func (dr *DisasterRecovery) MarkSectorInProgress(s abi.SectorNumber) error {
	f := fmt.Sprintf("%s/sector-%d-in-progress", dr.Dir, s)

	_, err := os.Stat(f)
	if os.IsNotExist(err) {
		file, err := os.Create(f)
		if err != nil {
			return err
		}
		defer file.Close()
	} else {
		//return fmt.Errorf("sector %d already marked as in progress", s)
	}

	return nil
}

func (dr *DisasterRecovery) WriteReport() error {
	f, err := os.Create(fmt.Sprintf("%s/report-%d", dr.Dir, time.Now().UnixNano()))
	if err != nil {
		return err
	}

	_, err = f.WriteString(spew.Sdump(dr))
	if err != nil {
		return err
	}
	err = f.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (dr *DisasterRecovery) CompleteSector(s abi.SectorNumber) error {
	oldLocation := fmt.Sprintf("%s/sector-%d-in-progress", dr.Dir, s)
	newLocation := fmt.Sprintf("%s/%d", dr.DoneDir, s)

	return os.Rename(oldLocation, newLocation)
}

func safeUnsealSector(ctx context.Context, sectorid abi.SectorNumber, offset abi.UnpaddedPieceSize, piecesize abi.PaddedPieceSize) (io.ReadCloser, bool, error) {
	var reader io.ReadCloser
	var isUnsealed bool
	var err error

	done := make(chan struct{})

	go func() {
		isUnsealed, err = sa.IsUnsealed(ctx, sectorid, offset, piecesize.Unpadded())
		if err != nil {
			return
		}

		if !isUnsealed {
			return
		}

		reader, err = sa.UnsealSector(ctx, sectorid, offset, piecesize.Unpadded())
		if err != nil {
			return
		}

		done <- struct{}{}
	}()

	select {
	case <-done:
		return reader, isUnsealed, err
	case <-time.After(3 * time.Second):
		return nil, false, errors.New("timeout on unseal sector after 3 seconds")
	}
}

func processPiece(ctx context.Context, sectorid abi.SectorNumber, chainDealID abi.DealID, piececid cid.Cid, piecesize abi.PaddedPieceSize, offset abi.UnpaddedPieceSize, l string) error {
	fmt.Println("sector: ", sectorid, "piece cid: ", piececid, "; piece size: ", piecesize, "; offset: ", offset, "label: ", l)

	cdi := uint64(chainDealID)
	sid := uint64(sectorid)

	dr.Sectors[sid].Deals[cdi] = &PieceStatus{
		PieceCID:    piececid,
		PieceSize:   piecesize,
		PieceOffset: offset,
		IsUnsealed:  false,
	}

	defer func(start time.Time) {
		took := time.Since(start)
		dr.Sectors[sid].Deals[cdi].ProcessingTook = took
		fmt.Println("processed piece cid: ", piececid, "sector: ", sectorid, "took: ", took)
	}(time.Now())

	reader, isUnsealed, err := safeUnsealSector(ctx, sectorid, offset, piecesize)
	if err != nil {
		return err
	}
	if !isUnsealed {
		return fmt.Errorf("sector %d is not unsealed", sid)
	}

	dr.Sectors[sid].Deals[cdi].IsUnsealed = true

	readerAt := reader.(Reader)

	opts := []carv2.Option{carv2.ZeroLengthSectionAsEOF(true)}
	rr, err := carv2.NewReader(readerAt, opts...)
	if err != nil {
		return err
	}

	drr, err := rr.DataReader()
	if err != nil {
		return err
	}

	dr.Sectors[sid].Deals[cdi].GotDataReader = true

	if !ignoreLID { // populate LID
		di := model.DealInfo{
			DealUuid:    uuid.NewString(),
			IsLegacy:    false,
			ChainDealID: chainDealID,
			MinerAddr:   maddr,
			SectorID:    sectorid,
			PieceOffset: offset.Padded(), // TODO: confirm that this is correct...?
			PieceLength: piecesize,
		}

		err = pd.AddDealForPiece(ctx, piececid, di)
		if err != nil {
			return err
		}
	}

	if !ignoreCommp { // commp over data reader
		w := &writer.Writer{}
		_, err = io.CopyBuffer(w, drr, make([]byte, writer.CommPBuf))
		if err != nil {
			return fmt.Errorf("copy into commp writer: %w", err)
		}

		commp, err := w.Sum()
		if err != nil {
			return fmt.Errorf("computing commP failed: %w", err)
		}

		encoder := cidenc.Encoder{Base: multibase.MustNewEncoder(multibase.Base32)}

		fmt.Println("CommP CID: ", encoder.Encode(commp.PieceCID))
		fmt.Println("Piece size: ", types.NewInt(uint64(commp.PieceSize.Unpadded().Padded())))

		if !commp.PieceCID.Equals(piececid) {
			return fmt.Errorf("calculated commp doesnt match on-chain data, expected %s, got %s", piececid, commp.PieceCID)
		}
	}

	return nil
}

func processSector(ctx context.Context, info *miner.SectorOnChainInfo) (bool, bool, error) { // ok, isUnsealed, error
	fmt.Println("sector number: ", info.SectorNumber, "; deals: ", info.DealIDs)

	sectorid := info.SectorNumber
	sid := uint64(sectorid)

	defer func(start time.Time) {
		took := time.Since(start)
		dr.Sectors[sid].ProcessingTook = took
		fmt.Println("processed sector number: ", info.SectorNumber, "; took: ", took)
	}(time.Now())

	err := dr.MarkSectorInProgress(sectorid)
	if err != nil {
		return false, false, err
	}

	dr.Sectors[sid].Deals = make(map[uint64]*PieceStatus)

	nextoffset := uint64(0)
	for _, did := range info.DealIDs {
		marketDeal, err := fullnodeApi.StateMarketStorageDeal(ctx, did, types.EmptyTSK)
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				fmt.Println("ERROR: deal mentioned in sector but not found in state; ", err)
				continue
			}
			return false, false, err
		}

		l := "(not a string)"
		if marketDeal.Proposal.Label.IsString() {
			l, err = marketDeal.Proposal.Label.ToString()
			if err != nil {
				return false, false, err
			}
		}

		err = processPiece(ctx, sectorid, did, marketDeal.Proposal.PieceCID, marketDeal.Proposal.PieceSize, abi.UnpaddedPieceSize(nextoffset), l)
		if err != nil {
			dr.Sectors[sid].Deals[uint64(did)].Error = err.Error()
			dr.PieceErrors++
			fmt.Println("piece error:", err)
			continue
		}

		nextoffset += uint64(marketDeal.Proposal.PieceSize.Unpadded())
	}

	err = dr.CompleteSector(sectorid)
	if err != nil {
		return false, false, err
	}

	return true, true, nil
}

func getActorAddress(ctx context.Context, cctx *cli.Context) (maddr address.Address, err error) {
	if cctx.IsSet("actor") {
		maddr, err = address.NewFromString(cctx.String("actor"))
		if err != nil {
			return maddr, err
		}
		return
	}

	minerApi, closer, err := lcli.GetStorageMinerAPI(cctx)
	if err != nil {
		return address.Undef, err
	}
	defer closer()

	maddr, err = minerApi.ActorAddress(ctx)
	if err != nil {
		return maddr, xerrors.Errorf("getting actor address: %w", err)
	}

	return maddr, nil
}

type Reader interface {
	io.Closer
	io.Reader
	io.ReaderAt
	io.Seeker
}
