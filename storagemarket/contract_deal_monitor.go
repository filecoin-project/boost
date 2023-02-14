package storagemarket

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/davecgh/go-spew/spew"
	"github.com/filecoin-project/boost/eth/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types/ethtypes"
	"github.com/filecoin-project/lotus/gateway"
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

	contractAbi, err := abi.JSON(strings.NewReader(DealClientABI))
	if err != nil {
		return err
	}

	go func() {
		// add cancel

		for resp := range responseCh {
			fmt.Println("event sub response triggered")
			spew.Dump(resp)

			fmt.Println("== after resp ==")
			result := resp.Result.([]interface{})[0]
			event := result.(map[string]interface{})
			topicContractAddress := event["address"].(string)
			topicDealProposalID := event["topics"].([]interface{})[1].(string)

			_from := "0xD66E69D6FeD4202DdB21266C5ECBe3B2Acd738a4" // address belonging to storage provider (configurable)

			// later we could filter / whitelist / blocklist by the originating contract
			_to := topicContractAddress

			// GetDealProposal is a free data retrieval call binding the contract method 0xf4b2e4d8.

			_params := "0xf4b2e4d8" + topicDealProposalID[2:] // cut 0x prefix

			fmt.Println("from: ", _from)
			fromEthAddr, err := ethtypes.ParseEthAddress(_from)
			if err != nil {
				panic(err)
			}
			fmt.Println("to: ", _to)
			toEthAddr, err := ethtypes.ParseEthAddress(_to)
			if err != nil {
				panic(err)
			}
			fmt.Println("params: ", _to)
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

			it, err := contractAbi.Unpack("getDealProposal", res)
			if err != nil {
				panic(err)
			}

			dealProposalResult := *abi.ConvertType(it[0], new(DealClientDealProposal)).(*DealClientDealProposal)

			paramsVersion1, _ := abi.NewType("tuple", "paramsVersion1", []abi.ArgumentMarshaling{
				{Name: "location_ref", Type: "string"},
				{Name: "skip_ipni_announce", Type: "bool"},
			})

			it2, err := abi.Arguments{
				{Type: paramsVersion1, Name: "paramsVersion1"},
			}.Unpack(dealProposalResult.Params)
			if err != nil {
				panic(err)
			}

			paramsAndVersion := *abi.ConvertType(it2[0], new(paramsRecord)).(*paramsRecord)

			_ = paramsAndVersion
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
