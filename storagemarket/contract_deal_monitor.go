package storagemarket

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	mbig "math/big"

	"github.com/filecoin-project/boost/node/config"
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
	"golang.org/x/exp/slices"
)

var (
	TopicHash = paddedEthHash(ethTopicHash("DealProposalCreate(bytes32)")) // deals published on chain
)

type ContractDealMonitor struct {
	api   api.FullNode
	prov  *Provider
	subCh *gateway.EthSubHandler
	cfg   *config.ContractDealsConfig
	maddr address.Address
}

func NewContractDealMonitor(p *Provider, a api.FullNode, subCh *gateway.EthSubHandler, cfg *config.ContractDealsConfig, maddr address.Address) *ContractDealMonitor {
	return &ContractDealMonitor{
		api:   a,
		prov:  p,
		subCh: subCh,
		cfg:   cfg,
		maddr: maddr,
	}
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

	log.Infow("Contract deals miner address", "maddr", c.maddr)

	go func() {
		for {
			select {
			case <-ctx.Done():
				err := ctx.Err()
				if err == context.Canceled {
					log.Infow("contract deal monitor context canceled, exiting...")
				} else {
					log.Warnw("contract deal monitor context closed, exiting...", "err", err)
				}
				return
			case resp := <-responseCh:
				err := func() error {
					result := resp.Result.([]interface{})[0]
					event := result.(map[string]interface{})
					topicContractAddress := event["address"].(string)
					topicDealProposalID := event["topics"].([]interface{})[1].(string)

					_from := c.cfg.From

					// allowlist check
					if len(c.cfg.AllowlistContracts) != 0 && !slices.Contains(c.cfg.AllowlistContracts, topicContractAddress) {
						return fmt.Errorf("allowlist does not contain this contract address: %s", topicContractAddress)
					}

					_to := topicContractAddress

					// GetDealProposal is a free data retrieval call binding the contract method 0xf4b2e4d8.
					_params := "0xf4b2e4d8" + topicDealProposalID[2:] // cut 0x prefix

					fromEthAddr, err := ethtypes.ParseEthAddress(_from)
					if err != nil {
						return fmt.Errorf("parsing `from` eth address failed: %w", err)
					}

					toEthAddr, err := ethtypes.ParseEthAddress(_to)
					if err != nil {
						return fmt.Errorf("parsing `to` eth address failed: %w", err)
					}

					params, err := ethtypes.DecodeHexString(_params)
					if err != nil {
						return fmt.Errorf("decoding params failed: %w", err)
					}

					res, err := c.api.EthCall(ctx, ethtypes.EthCall{
						From: &fromEthAddr,
						To:   &toEthAddr,
						Data: params,
					}, "latest")
					if err != nil {
						return fmt.Errorf("eth call erred: %w", err)
					}

					begin, length, err := lengthPrefixPointsTo(res)
					if err != nil {
						return fmt.Errorf("length prefix points erred: %w", err)
					}

					var dpc types.DealProposalCbor
					err = dpc.UnmarshalCBOR(bytes.NewReader(res[begin : begin+length]))
					if err != nil {
						return fmt.Errorf("cbor unmarshal failed: %w", err)
					}

					var pv1 types.ParamsVersion1
					err = pv1.UnmarshalCBOR(bytes.NewReader(dpc.Params))
					if err != nil {
						return fmt.Errorf("params cbor unmarshal failed: %w", err)
					}

					rootCidStr, err := dpc.Label.ToString()
					if err != nil {
						return fmt.Errorf("getting cid from label failed: %w", err)
					}

					rootCid, err := cid.Parse(rootCidStr)
					if err != nil {
						return fmt.Errorf("parsing cid failed: %w", err)
					}

					//TODO: get this from config or lotus-miner info?
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

					reason, err := c.prov.ExecuteLibp2pDeal(context.Background(), &proposal, "")
					if err != nil {
						log.Warnw("contract deal proposal failed", "id", proposal.DealUUID, "err", err, "reason", reason.Reason)
					}

					if reason.Accepted {
						log.Infow("contract deal proposal accepted", "id", proposal.DealUUID)
					} else {
						log.Warnw("contract deal proposal rejected", "id", proposal.DealUUID, "err", err, "reason", reason.Reason)
					}

					return nil
				}()
				if err != nil {
					log.Errorw("handling DealProposalCreate event erred: %w", err)
				}
			}
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

func lengthPrefixPointsTo(output []byte) (int, int, error) {
	index := 0
	boffset := mbig.NewInt(0).SetBytes(output[index : index+32])
	boffset.Add(boffset, mbig.NewInt(32))
	boutputLen := mbig.NewInt(int64(len(output)))

	if boffset.Cmp(boutputLen) > 0 {
		return 0, 0, fmt.Errorf("offset %v is over boundary; len: %v", boffset, boutputLen)
	}

	if boffset.BitLen() > 63 {
		return 0, 0, fmt.Errorf("offset larger than int64: %v", boffset)
	}

	offset := int(boffset.Uint64())
	lengthBig := mbig.NewInt(0).SetBytes(output[offset-32 : offset])

	size := mbig.NewInt(0)
	size.Add(size, boffset)
	size.Add(size, lengthBig)
	if size.BitLen() > 63 {
		return 0, 0, fmt.Errorf("len larger than int64: %v", size)
	}

	if size.Cmp(boutputLen) > 0 {
		return 0, 0, fmt.Errorf("length insufficient %v require %v", boutputLen, size)
	}
	return int(boffset.Uint64()), int(lengthBig.Uint64()), nil
}
