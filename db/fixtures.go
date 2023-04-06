package db

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"
)

var clientAddrs = []uint64{01312, 42134, 01322, 43242, 04212}

func GenerateDeals() ([]types.ProviderDealState, error) {
	return GenerateNDeals(len(clientAddrs))
}

func GenerateNDeals(count int) ([]types.ProviderDealState, error) {
	provAddr, err := address.NewIDAddress(1523)
	if err != nil {
		return nil, err
	}

	deals := []types.ProviderDealState{}
	rounds := (count / len(clientAddrs)) + 1
	for i := 0; i < rounds; i++ {
		for _, clientNum := range clientAddrs {
			if len(deals) == count {
				return deals, nil
			}

			dealErr := ""
			if len(deals) == 0 {
				dealErr = "data-transfer failed"
			}

			startEpoch := abi.ChainEpoch(rand.Intn(100000))
			endEpoch := abi.ChainEpoch(rand.Intn(5000)) + startEpoch
			publishCid := testutil.GenerateCid()
			clientAddr, err := address.NewIDAddress(clientNum)
			if err != nil {
				return nil, err
			}
			l, err := market.NewLabelFromString(testutil.GenerateCid().String())
			if err != nil {
				return nil, err
			}
			deal := types.ProviderDealState{
				DealUuid:    uuid.New(),
				CreatedAt:   time.Now(),
				IsOffline:   true,
				CleanupData: false,
				ClientDealProposal: market.ClientDealProposal{
					Proposal: market.DealProposal{
						PieceCID:             testutil.GenerateCid(),
						PieceSize:            34359738368,
						VerifiedDeal:         false,
						Client:               clientAddr,
						Provider:             provAddr,
						Label:                l,
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
				ClientPeerID:    generatePeerID(),
				DealDataRoot:    testutil.GenerateCid(),
				InboundFilePath: fmt.Sprintf("/data/staging/inbound/file-%d.car", rand.Intn(10000)),
				Transfer: types.Transfer{
					Type:   "http",
					Params: []byte(fmt.Sprintf(`{"url":"http://files.org/file%d.car"}`, rand.Intn(1000))),
					Size:   uint64(rand.Intn(10000)),
				},
				ChainDealID:    abi.DealID(rand.Intn(10000)),
				PublishCID:     &publishCid,
				SectorID:       abi.SectorNumber(rand.Intn(10000)),
				Offset:         abi.PaddedPieceSize(rand.Intn(1000000)),
				Length:         abi.PaddedPieceSize(rand.Intn(1000000)),
				Checkpoint:     dealcheckpoints.Accepted,
				Retry:          types.DealRetryAuto,
				Err:            dealErr,
				FastRetrieval:  true,
				AnnounceToIPNI: true,
			}

			deals = append(deals, deal)
		}
	}
	return deals, err
}

var pidSeed = 1

func generatePeerID() peer.ID {
	var alg uint64 = mh.SHA2_256
	hash, _ := mh.Sum([]byte(fmt.Sprintf("%d", pidSeed)), alg, -1)
	pidSeed++
	return peer.ID(hash)
}
