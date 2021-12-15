package db

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/google/uuid"
)

func LoadFixtures(ctx context.Context, db *sql.DB) ([]types.ProviderDealState, error) {
	err := CreateTables(ctx, db)
	if err != nil {
		return nil, err
	}

	dealsDB := NewDealsDB(db)

	deals, err := GenerateDeals()
	if err != nil {
		return nil, err
	}

	for _, deal := range deals {
		err = dealsDB.Insert(ctx, &deal)
		if err != nil {
			return nil, err
		}
	}

	logs := generateDealLogs(deals)
	for _, l := range logs {
		err = dealsDB.InsertLog(ctx, &l)
		if err != nil {
			return nil, err
		}
	}

	return deals, err
}

func GenerateDeals() ([]types.ProviderDealState, error) {
	clientAddrs := []uint64{01312, 42134, 01322, 43242, 01312}
	provAddr, err := address.NewActorAddress([]byte("f1523"))
	if err != nil {
		return nil, err
	}
	publishCid := testutil.GenerateCid()

	deals := []types.ProviderDealState{}
	for _, clientNum := range clientAddrs {
		startEpoch := abi.ChainEpoch(rand.Intn(100000))
		endEpoch := abi.ChainEpoch(rand.Intn(5000)) + startEpoch
		clientAddr, err := address.NewIDAddress(clientNum)
		if err != nil {
			return nil, err
		}
		deal := types.ProviderDealState{
			DealUuid:  uuid.New(),
			CreatedAt: time.Now(),
			ClientDealProposal: market.ClientDealProposal{
				Proposal: market.DealProposal{
					PieceCID:             testutil.GenerateCid(),
					PieceSize:            34359738368,
					VerifiedDeal:         false,
					Client:               clientAddr,
					Provider:             provAddr,
					Label:                testutil.GenerateCid().String(),
					StartEpoch:           startEpoch,
					EndEpoch:             endEpoch,
					StoragePricePerEpoch: abi.NewTokenAmount(rand.Int63()),
					ProviderCollateral:   abi.NewTokenAmount(rand.Int63()),
					ClientCollateral:     abi.NewTokenAmount(rand.Int63()),
				},
				ClientSignature: crypto.Signature{
					Type: crypto.SigTypeSecp256k1,
					Data: []byte("sig"),
				},
			},
			ClientPeerID:    testutil.GeneratePeer(),
			DealDataRoot:    testutil.GenerateCid(),
			InboundFilePath: fmt.Sprintf("/data/staging/inbound/file-%d.car", rand.Intn(10000)),
			Transfer: types.Transfer{
				Type:   "http",
				Params: []byte(fmt.Sprintf("{url:'http://files.org/file%d.car'}", rand.Intn(1000))),
				Size:   uint64(rand.Intn(10000)),
			},
			ChainDealID: abi.DealID(rand.Intn(10000)),
			PublishCID:  &publishCid,
			SectorID:    abi.SectorNumber(rand.Intn(10000)),
			Offset:      abi.PaddedPieceSize(rand.Intn(1000000)),
			Length:      abi.PaddedPieceSize(rand.Intn(1000000)),
			Checkpoint:  dealcheckpoints.Accepted,
		}

		deals = append(deals, deal)
	}

	return deals, err
}

func generateDealLogs(deals []types.ProviderDealState) []DealLog {
	var logs []DealLog
	for i, deal := range deals {
		switch i {
		case 0:
			logs = append(logs, []DealLog{{
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Minute * 2),
				Text:      "Propose Deal",
			}, {
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Minute * 2).Add(234 * time.Millisecond),
				Text:      "Accepted",
			}, {
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Minute * 2).Add(853 * time.Millisecond),
				Text:      "Start Data Transfer",
			}}...)

		case 1:
			logs = append(logs, []DealLog{{
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Minute * 4),
				Text:      "Propose Deal",
			}, {
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Minute * 4).Add(743 * time.Millisecond),
				Text:      "Accepted",
			}, {
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Minute * 4).Add(853 * time.Millisecond),
				Text:      "Start Data Transfer",
			}}...)

		case 2:
			logs = append(logs, []DealLog{{
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Minute * 20),
				Text:      "Propose Deal",
			}, {
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Minute * 20).Add(432 * time.Millisecond),
				Text:      "Accepted",
			}, {
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Minute * 20).Add(634 * time.Millisecond),
				Text:      "Start Data Transfer",
			}, {
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Minute * 20).Add(81 * time.Second),
				Text:      "Data Transfer Complete",
			}, {
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Minute * 20).Add(81 * time.Second).Add(325 * time.Millisecond),
				Text:      "Publishing",
			}}...)

		case 3:
			logs = append(logs, []DealLog{{
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Hour * 2).Add(262 * time.Millisecond),
				Text:      "Propose Deal",
			}, {
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Hour * 2).Add(523 * time.Millisecond),
				Text:      "Accepted",
			}, {
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Hour * 2).Add(745 * time.Millisecond),
				Text:      "Start Data Transfer",
			}, {
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Hour * 2).Add(242 * time.Second).Add(523 * time.Millisecond),
				Text:      "Data Transfer Complete",
			}, {
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Hour * 2).Add(242 * time.Second).Add(754 * time.Millisecond),
				Text:      "Publishing",
			}, {
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Hour * 2).Add(544 * time.Second).Add(423 * time.Millisecond),
				Text:      "Deal Published",
			}, {
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Hour * 1).Add(734 * time.Second).Add(345 * time.Millisecond),
				Text:      "Deal Pre-committed",
			}}...)

		case 4:
			logs = append(logs, []DealLog{{
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Hour * 4).Add(432 * time.Millisecond),
				Text:      "Propose Deal",
			}, {
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Hour * 4).Add(543 * time.Millisecond),
				Text:      "Accepted",
			}, {
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Hour * 4).Add(643 * time.Millisecond),
				Text:      "Start Data Transfer",
			}, {
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Hour * 4).Add(22 * time.Second).Add(523 * time.Millisecond),
				Text:      "Error - Connection Lost",
			}}...)

		case 5:
			logs = append(logs, []DealLog{{
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Hour * 5).Add(843 * time.Millisecond),
				Text:      "Propose Deal",
			}, {
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Hour * 5).Add(942 * time.Millisecond),
				Text:      "Accepted",
			}, {
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Hour * 5).Add(993 * time.Millisecond),
				Text:      "Start Data Transfer",
			}, {
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Hour * 5).Add(432 * time.Second).Add(823 * time.Millisecond),
				Text:      "Data Transfer Complete",
			}, {
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Hour * 5).Add(432 * time.Second).Add(953 * time.Millisecond),
				Text:      "Publishing",
			}, {
				DealUUID:  deal.DealUuid,
				CreatedAt: deal.CreatedAt.Add(-time.Hour * 5).Add(433 * time.Second).Add(192 * time.Millisecond),
				Text:      "Error - Not enough funds",
			}}...)
		}
	}
	return logs
}
