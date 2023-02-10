package storagemarket

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/davecgh/go-spew/spew"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types/ethtypes"
	"github.com/filecoin-project/lotus/gateway"
	"golang.org/x/crypto/sha3"
)

var (
	TopicHash = paddedEthHash(ethTopicHash("DealProposalCreate(bytes32)")) // deals published on chain
)

type ContractDealMonitor struct {
	api  api.FullNode
	prov *Provider
}

func NewContractDealMonitor(p *Provider, a api.FullNode) (*ContractDealMonitor, error) {
	//api, closer, err := lcli.GetFullNodeAPIV1(cctx, cliutil.FullNodeWithEthSubscribtionHandler(subCh))
	//if err != nil {
	//return err
	//}
	//defer closer()

	cdm := &ContractDealMonitor{
		api:  a,
		prov: p,
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

	subID, err := c.api.EthSubscribe(ctx, subParam)
	if err != nil {
		return err
	}

	responseCh := make(chan ethtypes.EthSubscriptionResponse, 1)

	subCh := gateway.NewEthSubHandler()

	err = subCh.AddSub(ctx, subID, func(ctx context.Context, resp *ethtypes.EthSubscriptionResponse) error {
		responseCh <- *resp
		return nil
	})

	go func() {
		// add cancel

		for resp := range responseCh {
			fmt.Println("event sub response triggered")
			spew.Dump(resp)

			//TODO: extract id

			//TODO: eth_call GetDealProposal(id)

			//TODO: parse response

			// c.prov.ExecuteContractDeal(resp)
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
