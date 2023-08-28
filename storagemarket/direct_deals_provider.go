package storagemarket

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/storagemarket/logs"
	"github.com/filecoin-project/boost/storagemarket/types"
	smtypes "github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-padreader"
	"github.com/filecoin-project/go-state-types/abi"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"

	"github.com/google/uuid"
)

//var log = logging.Logger("direct-deals-providers")

type DirectDealsProvider struct {
	fullnodeApi v1api.FullNode
	pieceAdder  types.PieceAdder

	//db            *sql.DB
	directDealsDB *db.DirectDataDB
	//logsSqlDB     *sql.DB
	//logsDB        *db.LogsDB

	dealLogger *logs.DealLogger
}

func NewDirectDealsProvider(fullnodeApi v1api.FullNode, pieceAdder types.PieceAdder, directDealsDB *db.DirectDataDB, dealLogger *logs.DealLogger) *DirectDealsProvider {
	return &DirectDealsProvider{
		fullnodeApi: fullnodeApi,
		pieceAdder:  pieceAdder,

		//db: db,
		directDealsDB: directDealsDB,
		//logsSqlDB: logsSqlDB,
		//logsDB: logsDB,

		dealLogger: dealLogger,
	}
}

func (ddp *DirectDealsProvider) Start() error {
	ctx := context.Background()

	deals, err := ddp.directDealsDB.ListAll(ctx)
	if err != nil {
		return fmt.Errorf("failed to list all direct deals: %w", err)
	}

	for _, entry := range deals {
		log.Infow("direct deal entry", "checkpoint", entry.Checkpoint)

		go func() {
			err := ddp.Process(ctx, entry.ID)
			if err != nil {
				log.Errorw("error while processing direct deal", "uuid", entry.ID, "err", err)
			}
		}()
	}

	return nil
}

func (ddp *DirectDealsProvider) Accept(ctx context.Context, entry *types.DirectDataEntry) (*api.ProviderDealRejectionInfo, error) {
	// Validate the deal proposal and Check for deal acceptance (allocation id, start epoch, etc.)

	//chainHead, err := ddp.fullnodeApi.ChainHead(ctx)
	//if err != nil {
	//log.Warnw("failed to get chain head", "err", err)
	//return nil, err
	//}

	//log.Infow("chain head", "epoch", chainHead)

	return &api.ProviderDealRejectionInfo{
		Accepted: true,
		Reason:   "",
	}, nil
}

func (ddp *DirectDealsProvider) Import(ctx context.Context, piececid cid.Cid, filepath string, deleteAfterImport bool, allocationId uint64, clientAddr address.Address, removeUnsealedCopy bool, skipIpniAnnounce bool) (*api.ProviderDealRejectionInfo, error) {
	log.Infow("received direct data import", "piececid", piececid, "filepath", filepath, "clientAddr", clientAddr, "allocationId", allocationId)

	entry := &types.DirectDataEntry{
		ID:        uuid.New(),
		CreatedAt: time.Now(),
		//CreatedAt time.Time
		PieceCID: piececid,
		//PieceSize abi.PaddedPieceSize
		Client: clientAddr,
		//Provider  address.Address
		CleanupData:      deleteAfterImport,
		InboundFilePath:  filepath,
		AllocationID:     allocationId,
		KeepUnsealedCopy: !removeUnsealedCopy,
		AnnounceToIPNI:   !skipIpniAnnounce,
		//SectorID abi.SectorNumber
		//Offset   abi.PaddedPieceSize
		//Length   abi.PaddedPieceSize
		//Checkpoint dealcheckpoints.Checkpoint
		//CheckpointAt time.Time
		//StartEpoch abi.ChainEpoch
		//EndEpoch   abi.ChainEpoch
		//Err string
		//Retry DealRetryType
	}

	ddp.dealLogger.Infow(entry.ID, "executing direct deal import", "client", clientAddr, "piececid", piececid)

	log.Infow("check for deal acceptance", "uuid", entry.ID, "piececid", piececid, "filepath", filepath)

	res, err := ddp.Accept(ctx, entry)
	if err != nil {
		return nil, err
	}

	log.Infow("deal accepted. insert direct deal entry to local db", "uuid", entry.ID, "piececid", piececid, "filepath", filepath)

	if res.Accepted {
		err := ddp.directDealsDB.Insert(ctx, entry)
		if err != nil {
			return nil, fmt.Errorf("failed to insert direct deal entry to local db: %w", err)
		}
	}

	go func() {
		err := ddp.Process(ctx, entry.ID)
		if err != nil {
			log.Errorw("error while processing direct deal", "uuid", entry.ID, "err", err)
		}
	}()

	return res, nil
}

func (ddp *DirectDealsProvider) Process(ctx context.Context, dealUuid uuid.UUID) error {
	entry, err := ddp.directDealsDB.ByID(ctx, dealUuid)
	if err != nil {
		return err
	}

	log.Infow("processing direct deal", "piececid", entry.PieceCID, "filepath", entry.InboundFilePath, "clientAddr", entry.Client, "allocationId", entry.AllocationID)

	// os stat
	fstat, err := os.Stat(entry.InboundFilePath)
	if err != nil {
		return err
	}

	// commp and piece size
	var commpCalc smtypes.CommpCalculator
	throttle := make(chan struct{})
	doRemoteCommP := false
	// TODO: fix pieceSize to be based on os.Stat()
	pieceSize := abi.UnpaddedPieceSize(fstat.Size())
	generatedPieceCid, dmErr := generatePieceCommitment(ctx, commpCalc, throttle, entry.InboundFilePath, pieceSize.Padded(), doRemoteCommP)
	if dmErr != nil {
		return fmt.Errorf("couldnt generate commp: %w", dmErr)
	}

	log.Infow("import details", "filepath", entry.InboundFilePath, "supplied-piececid", entry.PieceCID, "calculated-piececid", generatedPieceCid, "os stat size", fstat.Size(), "piece size for os stat", entry.PieceSize)

	// Open a reader against the CAR file with the deal data
	v2r, err := carv2.OpenReader(entry.InboundFilePath)
	if err != nil {
		return err
	}

	defer func() {
		_ = v2r.Close()
	}()

	var size uint64
	switch v2r.Version {
	case 1:
		st, err := os.Stat(entry.InboundFilePath)
		if err != nil {
			return err
		}
		size = uint64(st.Size())
	case 2:
		size = v2r.Header.DataSize
	}

	// Inflate the deal size so that it exactly fills a piece
	r, err := v2r.DataReader()
	if err != nil {
		return err
	}

	log.Infow("got v2r.DataReader over inbound file")

	paddedReader, err := padreader.NewInflator(r, size, pieceSize)
	if err != nil {
		return err
	}

	log.Infow("got paddedReader")

	//TODO: fix PieceDealInfo to include information about AllocationID and Client

	// Add the piece to a sector
	sdInfo := lapi.PieceDealInfo{
		//DealID:       entry.ChainDealID,
		//DealProposal: &entry.ClientDealProposal.Proposal,
		DealSchedule: lapi.DealSchedule{
			StartEpoch: entry.StartEpoch,
			EndEpoch:   entry.EndEpoch,
		},
		KeepUnsealed: entry.KeepUnsealedCopy,
	}

	// Attempt to add the piece to a sector (repeatedly if necessary)
	sectorNum, offset, err := ddp.pieceAdder.AddPiece(ctx, pieceSize, paddedReader, sdInfo)
	if err != nil {
		return fmt.Errorf("AddPiece failed: %w", err)
	}

	_ = sectorNum
	_ = offset
	//TODO: retry mechanism for AddPiece
	//curTime := build.Clock.Now()

	//for build.Clock.Since(curTime) < addPieceRetryTimeout {
	//if !errors.Is(err, sealing.ErrTooManySectorsSealing) {
	//if err != nil {
	////p.dealLogger.Warnw(deal.DealUuid, "failed to addPiece for deal, will-retry", "err", err.Error())
	//}
	//break
	//}
	//select {
	//case <-build.Clock.After(addPieceRetryWait):
	//sectorNum, offset, err = p.pieceAdder.AddPiece(ctx, pieceSize, pieceData, sdInfo)
	//case <-ctx.Done():
	//return nil, fmt.Errorf("error while waiting to retry AddPiece: %w", ctx.Err())
	//}
	//}

	//p.dealLogger.Infow(deal.DealUuid, "added new deal to sector", "sector", sectorNum.String())

	//deal.SectorID = packingInfo.SectorNumber
	//deal.Offset = packingInfo.Offset
	//deal.Length = packingInfo.Size
	//p.dealLogger.Infow(deal.DealUuid, "deal successfully handed to the sealing subsystem",
	//"sectorNum", deal.SectorID.String(), "offset", deal.Offset, "length", deal.Length)

	//if derr := p.updateCheckpoint(pub, deal, dealcheckpoints.AddedPiece); derr != nil {
	//return derr
	//}

	return nil
}
