package storagemarket

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	mbig "math/big"

	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types/ethtypes"
	"github.com/filecoin-project/lotus/gateway"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"golang.org/x/crypto/sha3"
)

var (
	TopicHash = paddedEthHash(ethTopicHash("DealProposalCreate(bytes32)")) // deals published on chain
)

type ContractDealMonitor struct {
	api   api.FullNode
	prov  *Provider
	subCh *gateway.EthSubHandler
}

func NewContractDealMonitor(p *Provider, a api.FullNode, subCh *gateway.EthSubHandler) (*ContractDealMonitor, error) {
	cdm := &ContractDealMonitor{
		api:   a,
		prov:  p,
		subCh: subCh,
	}

	return cdm, nil
}

func (c *ContractDealMonitor) Start(ctx context.Context) error {
	var topicSpec ethtypes.EthTopicSpec
	topicSpec = append(topicSpec, ethtypes.EthHashList{TopicHash})

	subParam, err := json.Marshal(ethtypes.EthSubscribeParams{
		EventType: "logs",
		Params:    &ethtypes.EthSubscriptionParams{Topics: topicSpec},
	})
	if err != nil {
		return err
	}

	subID, err := c.api.EthSubscribe(ctx, subParam)
	if err != nil {
		return err
	}

	responseCh := make(chan ethtypes.EthSubscriptionResponse, 1)

	err = c.subCh.AddSub(ctx, subID, func(ctx context.Context, resp *ethtypes.EthSubscriptionResponse) error {
		responseCh <- *resp
		return nil
	})
	if err != nil {
		return err
	}

	//contractAbi, err := ethabi.JSON(strings.NewReader(DealClientABI))
	//if err != nil {
	//return err
	//}
	go func() {
		// add cancel

		for resp := range responseCh {
			//fmt.Println("event sub response triggered")
			//spew.Dump(resp)

			//fmt.Println("== after resp ==")
			result := resp.Result.([]interface{})[0]
			event := result.(map[string]interface{})
			topicContractAddress := event["address"].(string)
			topicDealProposalID := event["topics"].([]interface{})[1].(string)

			_from := "0xD66E69D6FeD4202DdB21266C5ECBe3B2Acd738a4" // address belonging to storage provider (configurable)

			// later we could filter / whitelist / blocklist by the originating contract
			_to := topicContractAddress

			// GetDealProposal is a free data retrieval call binding the contract method 0xf4b2e4d8.
			_params := "0xf4b2e4d8" + topicDealProposalID[2:] // cut 0x prefix

			//fmt.Println("from: ", _from)
			fromEthAddr, err := ethtypes.ParseEthAddress(_from)
			if err != nil {
				panic(err)
			}
			//fmt.Println("to: ", _to)
			toEthAddr, err := ethtypes.ParseEthAddress(_to)
			if err != nil {
				panic(err)
			}
			//fmt.Println("params: ", _to)
			params, err := ethtypes.DecodeHexString(_params)
			if err != nil {
				panic(err)
			}

			res, err := c.api.EthCall(ctx, ethtypes.EthCall{
				From: &fromEthAddr,
				To:   &toEthAddr,
				Data: params,
			}, "latest")
			if err != nil {
				panic(err)
			}

			begin, length, err := lengthPrefixPointsTo(res)
			if err != nil {
				panic(err)
			}

			var dpc types.DealProposalCbor
			err = dpc.UnmarshalCBOR(bytes.NewReader(res[begin : begin+length]))
			if err != nil {
				panic(err)
			}

			var pv1 types.ParamsVersion1
			err = pv1.UnmarshalCBOR(bytes.NewReader(dpc.Params))
			if err != nil {
				panic(err)
			}

			rootCidStr, err := dpc.Label.ToString()
			if err != nil {
				panic(err)
			}

			rootCid, err := cid.Parse(rootCidStr)
			if err != nil {
				panic(err)
			}

			providerAddr, _ := address.NewFromString("t01000")

			prop := market.DealProposal{
				PieceCID:     dpc.PieceCID,
				PieceSize:    dpc.PieceSize,
				VerifiedDeal: dpc.VerifiedDeal,
				Client:       dpc.Client,
				Provider:     providerAddr,

				Label: dpc.Label,

				StartEpoch:           dpc.StartEpoch,
				EndEpoch:             dpc.EndEpoch,
				StoragePricePerEpoch: dpc.StoragePricePerEpoch,

				ProviderCollateral: dpc.ProviderCollateral,
				ClientCollateral:   dpc.ClientCollateral,
			}

			proposal := types.DealParams{
				DealUUID:  uuid.New(),
				IsOffline: false,
				ClientDealProposal: market.ClientDealProposal{
					Proposal: prop,
					// signature is garbage, but it still needs to serialize, so shouldnt be empty!!
					ClientSignature: crypto.Signature{
						Type: crypto.SigTypeBLS,
						Data: []byte{0xde, 0xad},
					},
				},
				DealDataRoot: rootCid,
				Transfer: types.Transfer{
					Type:   "http",
					Params: []byte(fmt.Sprintf(`{"URL":"%s"}`, pv1.LocationRef)),
					Size:   pv1.CarSize,
				},
				//TODO: maybe add to pv1?? RemoveUnsealedCopy: paramsAndVersion.RemoveUnsealedCopy,
				RemoveUnsealedCopy: false,
				SkipIPNIAnnounce:   pv1.SkipIpniAnnounce,
			}

			log.Infow("received contract deal proposal", "id", proposal.DealUUID, "client-peer", dpc.Client)

			resdeal, err := c.prov.ExecuteLibp2pDeal(context.Background(), &proposal, "")
			if err != nil {
				log.Warnw("contract deal proposal failed", "id", proposal.DealUUID, "err", err, "reason", resdeal.Reason)
			}

			_ = resdeal
		}
	}()

	return nil
}

func (c *ContractDealMonitor) Stop() error {
	return nil
}

func paddedEthHash(orig []byte) ethtypes.EthHash {
	if len(orig) > 32 {
		panic("exceeds EthHash length")
	}
	var ret ethtypes.EthHash
	needed := 32 - len(orig)
	copy(ret[needed:], orig)
	return ret
}

func ethTopicHash(sig string) []byte {
	hasher := sha3.NewLegacyKeccak256()
	hasher.Write([]byte(sig))
	return hasher.Sum(nil)
}

// lengthPrefixPointsTo interprets a 32 byte slice as an offset and then determines which indices to look to decode the type.
func lengthPrefixPointsTo(output []byte) (start int, length int, err error) {
	index := 0
	bigOffsetEnd := mbig.NewInt(0).SetBytes(output[index : index+32])
	bigOffsetEnd.Add(bigOffsetEnd, mbig.NewInt(32))
	outputLength := mbig.NewInt(int64(len(output)))

	if bigOffsetEnd.Cmp(outputLength) > 0 {
		return 0, 0, fmt.Errorf("cannot marshal in to go slice: offset %v would go over slice boundary (len=%v)", bigOffsetEnd, outputLength)
	}

	if bigOffsetEnd.BitLen() > 63 {
		return 0, 0, fmt.Errorf("offset larger than int64: %v", bigOffsetEnd)
	}

	offsetEnd := int(bigOffsetEnd.Uint64())
	lengthBig := mbig.NewInt(0).SetBytes(output[offsetEnd-32 : offsetEnd])

	totalSize := mbig.NewInt(0)
	totalSize.Add(totalSize, bigOffsetEnd)
	totalSize.Add(totalSize, lengthBig)
	if totalSize.BitLen() > 63 {
		return 0, 0, fmt.Errorf("length larger than int64: %v", totalSize)
	}

	if totalSize.Cmp(outputLength) > 0 {
		return 0, 0, fmt.Errorf("cannot marshal in to go type: length insufficient %v require %v", outputLength, totalSize)
	}
	start = int(bigOffsetEnd.Uint64())
	length = int(lengthBig.Uint64())
	return
}
