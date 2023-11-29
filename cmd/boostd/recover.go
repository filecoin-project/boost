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
	"github.com/filecoin-project/boost-gfm/piecestore"
	"github.com/filecoin-project/boost/cmd/lib"
	"github.com/filecoin-project/boost/db"
	bdclient "github.com/filecoin-project/boost/extern/boostd-data/client"
	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-commp-utils/writer"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin/v9/miner"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/markets/dagstore"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-cidutil/cidenc"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/mitchellh/go-homedir"
	"github.com/multiformats/go-multibase"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
)

var (
	dr          *DisasterRecovery
	sa          dagstore.SectorAccessor
	fullnodeApi v1api.FullNode
	minerApi    api.StorageMiner
	pd          *piecedirectory.PieceDirectory
	maddr       address.Address
	ps          piecestore.PieceStore

	ignoreCommp bool
	ignoreLID   bool

	logger *zap.SugaredLogger
)

var recoverCmd = &cli.Command{
	Name:  "recover",
	Usage: "LID recover commands",
	Subcommands: []*cli.Command{
		lidCmd,
	},
}

var lidCmd = &cli.Command{
	Name:   "lid",
	Usage:  "lid",
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
			Name:  "recover-dir",
			Usage: "location to store progress of recovery",
			Value: "~/.boost-recover",
		},
		&cli.StringFlag{
			Name:  "repo",
			Usage: "location to boost repo",
			Value: "~/.boost",
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
		&cli.IntFlag{
			Name:  "add-index-concurrency",
			Usage: "the maximum number of parallel tasks that a single add index operation can be split into",
			Value: config.DefaultAddIndexConcurrency,
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
		c := make(chan os.Signal, 1)
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
	dr, err = NewDisasterRecovery(ctx, cctx.String("recover-dir"), cctx.String("repo"))
	if err != nil {
		return err
	}

	var sectorid abi.SectorNumber
	if cctx.IsSet("sector-id") {
		sectorid = abi.SectorNumber(cctx.Uint64("sector-id"))
		logger.Infow("running recovery tool on a single sector", "sector", sectorid)
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

	err = lib.CheckFullNodeApiVersion(ctx, fullnodeApi)
	if err != nil {
		return err
	}

	// Connect to the storage API and create a sector accessor
	storageApiInfo := cctx.String("api-storage")

	var mcloser jsonrpc.ClientCloser
	minerApi, mcloser, err = lib.GetMinerApi(ctx, storageApiInfo, log)
	if err != nil {
		return err
	}
	defer mcloser()

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
		cl := bdclient.NewStore()
		defer cl.Close(ctx)
		err = cl.Dial(ctx, cctx.String("api-lid"))
		if err != nil {
			return fmt.Errorf("connecting to local index directory service: %w", err)
		}
		pr := &piecedirectory.SectorAccessorAsPieceReader{SectorAccessor: sa}
		pd = piecedirectory.NewPieceDirectory(cl, pr, cctx.Int("add-index-throttle"), piecedirectory.WithAddIndexConcurrency(cctx.Int("add-index-concurrency")))
		pd.Start(ctx)
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
			logger.Infow("no deals in sector", "sector", info.SectorNumber)

			dr.SectorsWithoutDeals = append(dr.SectorsWithoutDeals, uint64(info.SectorNumber))
			continue
		}

		dr.SectorsWithDeals = append(dr.SectorsWithDeals, uint64(info.SectorNumber))
		sectorsWithDeals = append(sectorsWithDeals, info)
	}

	for _, info := range sectorsWithDeals {
		dr.Sectors[uint64(info.SectorNumber)] = &SectorStatus{}

		if dr.IsDone(info.SectorNumber) {
			logger.Infow("sector already processed", "sector", info.SectorNumber)
			dr.Sectors[uint64(info.SectorNumber)].AlreadyProcessed = true
			continue
		}

		ok, isUnsealed, err := processSector(ctx, info)
		if err != nil {
			return err
		}
		if !isUnsealed {
			logger.Errorw("sector is not unsealed", "sector", info.SectorNumber)
			continue
		}
		if !ok {
			logger.Errorw("unexpected state - not ok, but sector is unsealed and we got no errors", "sector", info.SectorNumber)
			return errors.New("unexpected state - not ok, but sector is unsealed and no error")
		}
	}

	return nil
}

type DisasterRecovery struct {
	Dir     string // main recovery dir - keeps progress on recovery
	DoneDir string

	TotalSectors int
	PieceErrors  int

	SectorsWithDeals    []uint64
	SectorsWithoutDeals []uint64

	Sectors map[uint64]*SectorStatus

	HaveBoostDealsAndPieceStore bool                   // flag whether we managed to load boost sqlite db and piece store
	PieceStoreLoadError         string                 // error in case we failed to load piece store
	BoostDeals                  map[abi.DealID]string  // deals from boost sqlite db
	PropCidByChainDealID        map[abi.DealID]cid.Cid // proposal cid by chain deal id (legacy deals)
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

func NewDisasterRecovery(ctx context.Context, dir, repodir string) (*DisasterRecovery, error) {
	drDir, err := homedir.Expand(dir)
	if err != nil {
		return nil, fmt.Errorf("expanding recovery dir path: %w", err)
	}
	if drDir == "" {
		return nil, errors.New("recover-dir is a required flag")
	}

	repodir, err = homedir.Expand(repodir)
	if err != nil {
		return nil, fmt.Errorf("expanding repo dir path: %w", err)
	}
	if repodir == "" {
		return nil, errors.New("repo is a required flag")
	}

	var recoverRanPreviously bool
	d, err := os.Stat(drDir)
	if err == nil {
		if d.IsDir() {
			recoverRanPreviously = true
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

	drr := &DisasterRecovery{
		Dir:     drDir,
		DoneDir: doneDir,
		Sectors: make(map[uint64]*SectorStatus),
	}

	// logger for the recovery that outputs to a file and stdout
	logger, err = createLogger(fmt.Sprintf("%s/output-%d.log", drr.Dir, time.Now().UnixNano()))
	if err != nil {
		return nil, err
	}

	if recoverRanPreviously {
		logger.Warn("recovery directory exists, so will continue from where recovery left off previously")
	}

	err = drr.loadPieceStoreAndBoostDB(ctx, repodir)
	if err != nil {
		drr.HaveBoostDealsAndPieceStore = false
		drr.PieceStoreLoadError = err.Error()
	} else {
		drr.HaveBoostDealsAndPieceStore = true
	}

	return drr, nil
}

func (dr *DisasterRecovery) loadPieceStoreAndBoostDB(ctx context.Context, repoDir string) error {
	ds, err := lib.OpenDataStore(repoDir)
	if err != nil {
		return fmt.Errorf("creating piece store from repo %s: %w", repoDir, err)
	}

	// create a mapping of on-chain deal ID to deal proposal cid (legacy deals)
	dr.PropCidByChainDealID, err = lib.GetPropCidByChainDealID(ctx, ds)
	if err != nil {
		return fmt.Errorf("building chain deal id -> proposal cid map: %w", err)
	}

	ps, err = lib.OpenPieceStore(ctx, ds)
	if err != nil {
		return fmt.Errorf("opening piece store: %w", err)
	}

	dbPath := path.Join(repoDir, "boost.db?cache=shared")
	sqldb, err := db.SqlDB(dbPath)
	if err != nil {
		return fmt.Errorf("opening boost sqlite db: %w", err)
	}

	qry := "SELECT ID, ChainDealID FROM Deals"
	rows, err := sqldb.QueryContext(ctx, qry)
	if err != nil {
		return fmt.Errorf("executing select on Deals: %w", err)
	}

	dr.BoostDeals = make(map[abi.DealID]string)
	for rows.Next() {
		var uuid string
		var chainDealId abi.DealID

		err := rows.Scan(&uuid, &chainDealId)
		if err != nil {
			return fmt.Errorf("executing row scan: %w", err)
		}

		dr.BoostDeals[chainDealId] = uuid
	}

	return nil
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

// safeUnsealSector tries to return a reader to an unsealed sector or times out
func safeUnsealSector(ctx context.Context, sectorid abi.SectorNumber, offset abi.UnpaddedPieceSize, piecesize abi.PaddedPieceSize) (io.ReadCloser, bool, error) {
	mid, _ := address.IDFromAddress(maddr)

	sid := abi.SectorID{
		Miner:  abi.ActorID(mid),
		Number: sectorid,
	}

	u, err := minerApi.StorageFindSector(ctx, sid, storiface.FTUnsealed, 0, false)
	if err != nil {
		logger.Errorw("storage find sector", "err", err)
	}

	var reader io.ReadCloser
	var isUnsealed bool

	done := make(chan struct{})
	doneIsUnsealed := make(chan struct{})

	go func() {
		isUnsealed, err = sa.IsUnsealed(ctx, sectorid, offset, piecesize.Unpadded())
		if err != nil {
			logger.Errorw("sa.IsUnseaed return error", "sector", sectorid, "err", err)
			return
		}

		if !isUnsealed && len(u) > 0 {
			logger.Errorw(fmt.Sprintf("isUnsealed(%d, %d, %d) returned false, but `storage find` returns an unsealed copy; most probably unsealed copy is full of 0x00 and is corrupted (confirm with xxd)", sectorid, offset, piecesize.Unpadded()), "sector", sectorid)
		}

		doneIsUnsealed <- struct{}{}
	}()

	select {
	case <-doneIsUnsealed:
	case <-time.After(3000 * time.Millisecond):
		return nil, false, errors.New("timeout on isUnsealed sector after 3 seconds")
	}

	if !isUnsealed {
		return nil, false, nil
	}

	logger.Debugw("sa.IsUnsealed return true", "sector", sectorid)

	go func() {
		reader, err = sa.UnsealSector(ctx, sectorid, offset, piecesize.Unpadded())
		if err != nil {
			logger.Errorw("sa.UnsealSector return error", "sector", sectorid, "err", err)
			return
		}

		done <- struct{}{}
	}()

	select {
	case <-done:
		return reader, isUnsealed, err
	case <-time.After(3000 * time.Millisecond):
		return nil, false, errors.New("timeout on unseal sector after 3 seconds")
	}
}

func processPiece(ctx context.Context, sectorid abi.SectorNumber, chainDealID abi.DealID, piececid cid.Cid, piecesize abi.PaddedPieceSize, offset abi.UnpaddedPieceSize, l string) error {
	logger.Debugw("processing piece", "sector", sectorid, "piececid", piececid, "piecesize", piecesize, "offset", offset, "label", l)

	cdi := uint64(chainDealID)
	sid := uint64(sectorid)

	dr.Sectors[sid].Deals[cdi] = &PieceStatus{
		PieceCID:    piececid,
		PieceSize:   piecesize,
		PieceOffset: offset,
		IsUnsealed:  false,
	}

	if dr.HaveBoostDealsAndPieceStore { // sanity check on piece store / piece info vs chain data and infered piece size / offset data
		pi, err := ps.GetPieceInfo(piececid)
		if err != nil {
			logger.Errorw("cant get piece info from piece store", "piececid", piececid, "sector", sid, "err", err)
		} else {
			var found bool
			for _, di := range pi.Deals {
				if di.DealID == chainDealID {
					found = true

					if di.SectorID != sectorid {
						logger.Errorw("sector mismatch", "sector", sid, "piececid", piececid, "chain-deal-id", chainDealID, "got", di.SectorID)
					}
					if di.Offset != offset.Padded() {
						logger.Errorw("offset mismatch", "sector", sid, "piececid", piececid, "chain-deal-id", chainDealID, "calculated", offset.Padded(), "got from ps", di.Offset)
					}
					if di.Length != piecesize {
						logger.Errorw("length/piece size mismatch", "sector", sid, "piececid", piececid, "chain-deal-id", chainDealID, "expected", piecesize, "got from ps", di.Length)
					}
				}
			}
			if !found {
				logger.Errorw("chain deal not found in piece info", "sector", sid, "piececid", piececid, "chain-deal-id", chainDealID, "pi", spew.Sdump(pi))
			}
		}
	}

	defer func(start time.Time) {
		took := time.Since(start)
		dr.Sectors[sid].Deals[cdi].ProcessingTook = took
		logger.Debugw("processed piece", "took", took, "sector", sectorid, "piececid", piececid, "piecesize", piecesize, "offset", offset, "label", l)
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
		var shouldGenerateNewDeal bool

		var di model.DealInfo

		if dr.HaveBoostDealsAndPieceStore { // successfully loaded boost sqlite db and piece store => try to infer dealinfo
			// find the deal corresponding to the deal info's DealID
			proposalCid, okLegacy := dr.PropCidByChainDealID[chainDealID]
			uuid, okBoost := dr.BoostDeals[chainDealID]

			if !okLegacy && !okBoost {
				logger.Errorw("cant find boost deal or legacy deal",
					"piececid", piececid, "chain-deal-id", chainDealID, "err", err)

				shouldGenerateNewDeal = true
			} else {
				isLegacy := false
				if uuid == "" {
					uuid = proposalCid.String()
					isLegacy = true
				}

				di = model.DealInfo{
					DealUuid:    uuid,
					IsLegacy:    isLegacy,
					ChainDealID: chainDealID,
					MinerAddr:   maddr,
					SectorID:    sectorid,
					PieceOffset: offset.Padded(),
					PieceLength: piecesize,
				}
			}
		}

		if !dr.HaveBoostDealsAndPieceStore || shouldGenerateNewDeal { // missing boost sqlite db and piece store, so generate new dealinfo
			// in the future we could also regenerate boost db sqlite??

			di = model.DealInfo{
				DealUuid:    uuid.NewString(),
				IsLegacy:    false,
				ChainDealID: chainDealID,
				MinerAddr:   maddr,
				SectorID:    sectorid,
				PieceOffset: offset.Padded(),
				PieceLength: piecesize,
			}
		}

		timeAddIndex := time.Now()

		err = pd.AddDealForPiece(ctx, piececid, di)
		if err != nil {
			logger.Errorw("cant add deal info for piece", "piececid", piececid, "chain-deal-id", chainDealID, "err", err)

			return err
		}

		logger.Infow("added index", "took", time.Since(timeAddIndex), "sector", di.SectorID, "piececid", piececid, "chain-deal-id", di.ChainDealID, "uuid", di.DealUuid)
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
		_ = encoder

		//fmt.Println("CommP CID: ", encoder.Encode(commp.PieceCID))
		//fmt.Println("Piece size: ", types.NewInt(uint64(commp.PieceSize.Unpadded().Padded())))

		if !commp.PieceCID.Equals(piececid) {
			return fmt.Errorf("calculated commp doesnt match on-chain data, expected %s, got %s", piececid, commp.PieceCID)
		}
	}

	return nil
}

func processSector(ctx context.Context, info *miner.SectorOnChainInfo) (bool, bool, error) { // ok, isUnsealed, error
	logger.Debugw("processing sector", "sector", info.SectorNumber, "deals", info.DealIDs)

	sectorid := info.SectorNumber
	sid := uint64(sectorid)

	var gotErr bool

	defer func(start time.Time) {
		took := time.Since(start)
		dr.Sectors[sid].ProcessingTook = took
		if gotErr {
			logger.Debugw("processed sector with errors", "sector", sectorid, "took", took, "deals", info.DealIDs)
		} else {
			logger.Debugw("successfully processed sector", "sector", sectorid, "took", took, "deals", info.DealIDs)
		}
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
				logger.Warnw("deal present in sector, but not in market actor state, so probably expired", "sector", sectorid, "deal", did, "err", err)
				break
			} else {
				return false, false, err
			}
		}

		l := "(not a string)"
		if marketDeal.Proposal.Label.IsString() {
			l, err = marketDeal.Proposal.Label.ToString()
			if err != nil {
				return false, false, err
			}
		}

		if nextoffset%uint64(marketDeal.Proposal.PieceSize.Unpadded()) != 0 {
			currentoffset := nextoffset
			nextoffset = 0
			for nextoffset < currentoffset {
				nextoffset += uint64(marketDeal.Proposal.PieceSize.Unpadded())
			}
		}

		err = processPiece(ctx, sectorid, did, marketDeal.Proposal.PieceCID, marketDeal.Proposal.PieceSize, abi.UnpaddedPieceSize(nextoffset), l)
		nextoffset += uint64(marketDeal.Proposal.PieceSize.Unpadded())
		if err != nil {
			dr.Sectors[sid].Deals[uint64(did)].Error = err.Error()
			dr.PieceErrors++
			gotErr = true
			logger.Errorw("got piece error", "sector", sectorid, "deal", did, "err", err)
			continue
		}
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

func createLogger(logPath string) (*zap.SugaredLogger, error) {
	logCfg := zap.NewDevelopmentConfig()
	logCfg.OutputPaths = []string{"stdout", logPath}
	logCfg.ErrorOutputPaths = []string{"stdout", logPath}
	logCfg.DisableStacktrace = true
	zl, err := logCfg.Build()
	if err != nil {
		return nil, err
	}
	defer zl.Sync() //nolint:errcheck
	return zl.Sugar(), err
}
