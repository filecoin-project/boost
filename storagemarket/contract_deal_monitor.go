package storagemarket

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	mbig "math/big"
	"time"

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
	TopicHash = paddedEthHash(ethTopicHash("DealProposalCreate(bytes32,uint64,bool,uint256)")) // deals published on chain
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

	log.Infow("contract deals subscription", "maddr", c.maddr, "topic", TopicHash.String())

	fromEthAddr, err := ethtypes.ParseEthAddress(c.cfg.From)
	if err != nil {
		return fmt.Errorf("parsing `from` eth address failed: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			if err == context.Canceled {
				log.Infow("contract deal monitor context canceled, exiting...")
			} else {
				log.Warnw("contract deal monitor context closed, exiting...", "err", err)
			}
			return nil
		case resp := <-responseCh:
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Errorw("recovered from panic from handling eth_subscribe event", "recover", r)
					}
				}()

				err := func() error {
					event := resp.Result.(map[string]interface{})
					topicContractAddress := event["address"].(string)
					topicDealProposalID := event["topics"].([]interface{})[1].(string)

					// allowlist check
					if len(c.cfg.AllowlistContracts) != 0 && !slices.Contains(c.cfg.AllowlistContracts, topicContractAddress) {
						return fmt.Errorf("allowlist does not contain this contract address: %s", topicContractAddress)
					}

					res, err := c.getDealProposal(ctx, topicContractAddress, topicDealProposalID, fromEthAddr)
					if err != nil {
						return fmt.Errorf("eth call for get deal proposal failed: %w", err)
					}

					resParams, err := c.getExtraData(ctx, topicContractAddress, topicDealProposalID, fromEthAddr)
					if err != nil {
						return fmt.Errorf("eth call for extra data failed: %w", err)
					}

					var dpc market.DealProposal
					err = dpc.UnmarshalCBOR(bytes.NewReader(res))
					if err != nil {
						return fmt.Errorf("cbor unmarshal failed: %w", err)
					}

					var pv1 types.ContractParamsVersion1
					err = pv1.UnmarshalCBOR(bytes.NewReader(resParams))
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

					prop := market.DealProposal{
						PieceCID:     dpc.PieceCID,
						PieceSize:    dpc.PieceSize,
						VerifiedDeal: dpc.VerifiedDeal,
						Client:       dpc.Client,
						Provider:     c.maddr,

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
						RemoveUnsealedCopy: pv1.RemoveUnsealedCopy,
						SkipIPNIAnnounce:   pv1.SkipIpniAnnounce,
					}

					log.Infow("received contract deal proposal", "id", topicDealProposalID, "uuid", proposal.DealUUID, "client-peer", dpc.Client, "contract", topicContractAddress, "piece-cid", dpc.PieceCID.String())

					reason, err := c.prov.ExecuteDeal(context.Background(), &proposal, "")
					if err != nil {
						log.Warnw("contract deal proposal failed", "id", topicDealProposalID, "uuid", proposal.DealUUID, "err", err, "reason", reason.Reason)
						return nil
					}

					if reason.Accepted {
						log.Infow("contract deal proposal accepted", "id", topicDealProposalID, "uuid", proposal.DealUUID)
					} else {
						log.Warnw("contract deal proposal rejected", "id", topicDealProposalID, "uuid", proposal.DealUUID, "err", err, "reason", reason.Reason)
					}

					return nil
				}()
				if err != nil {
					log.Errorw("handling DealProposalCreate event erred", "err", err)
				}
			}()
		}
	}
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

func (c *ContractDealMonitor) getDealProposal(ctx context.Context, topicContractAddress string, topicDealProposalID string, fromEthAddr ethtypes.EthAddress) ([]byte, error) {
	defer func(now time.Time) {
		log.Debugw("contract getDealProposal elapsed", "took", time.Since(now))
	}(time.Now())

	// GetDealProposal is a free data retrieval call binding the contract method 0xf4b2e4d8.
	_params := "0xf4b2e4d8" + topicDealProposalID[2:] // cut 0x prefix

	toEthAddr, err := ethtypes.ParseEthAddress(topicContractAddress)
	if err != nil {
		return nil, fmt.Errorf("parsing `to` eth address failed: %w", err)
	}

	params, err := ethtypes.DecodeHexString(_params)
	if err != nil {
		return nil, fmt.Errorf("decoding params failed: %w", err)
	}

	latestID := "latest"
	latest := ethtypes.EthBlockNumberOrHash{
		PredefinedBlock: &latestID,
	}

	res, err := c.api.EthCall(ctx, ethtypes.EthCall{
		From: &fromEthAddr,
		To:   &toEthAddr,
		Data: params,
	}, latest)
	if err != nil {
		return nil, fmt.Errorf("eth call erred: %w", err)
	}

	begin, length, err := lengthPrefixPointsTo(res)
	if err != nil {
		return nil, fmt.Errorf("length prefix points erred: %w", err)
	}

	return res[begin : begin+length], nil
}

func (c *ContractDealMonitor) getExtraData(ctx context.Context, topicContractAddress string, topicDealProposalID string, fromEthAddr ethtypes.EthAddress) ([]byte, error) {
	defer func(now time.Time) {
		log.Debugw("contract getExtraData elapsed", "took", time.Since(now))
	}(time.Now())

	// GetExtraParams is a free data retrieval call binding the contract method 0x4634aed5.
	_params := "0x4634aed5" + topicDealProposalID[2:] // cut 0x prefix

	toEthAddr, err := ethtypes.ParseEthAddress(topicContractAddress)
	if err != nil {
		return nil, fmt.Errorf("parsing `to` eth address failed: %w", err)
	}

	params, err := ethtypes.DecodeHexString(_params)
	if err != nil {
		return nil, fmt.Errorf("decoding params failed: %w", err)
	}

	latestID := "latest"
	latest := ethtypes.EthBlockNumberOrHash{
		PredefinedBlock: &latestID,
	}

	res, err := c.api.EthCall(ctx, ethtypes.EthCall{
		From: &fromEthAddr,
		To:   &toEthAddr,
		Data: params,
	}, latest)
	if err != nil {
		return nil, fmt.Errorf("eth call erred: %w", err)
	}

	begin, length, err := lengthPrefixPointsTo(res)
	if err != nil {
		return nil, fmt.Errorf("length prefix points erred: %w", err)
	}

	return res[begin : begin+length], nil
}
