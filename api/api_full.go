package api

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	textselector "github.com/ipld/go-ipld-selector-text-lite"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"

	"github.com/filecoin-project/boost-gfm/retrievalmarket"
	"github.com/filecoin-project/boost-gfm/storagemarket"

	"github.com/filecoin-project/go-state-types/builtin/v8/paych"
	"github.com/filecoin-project/lotus/chain/actors/builtin"
	"github.com/filecoin-project/lotus/chain/actors/builtin/market"
	"github.com/filecoin-project/lotus/chain/actors/builtin/power"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/node/repo/imports"
)

//go:generate go run github.com/golang/mock/mockgen -destination=mocks/mock_full.go -package=mocks . FullNode

// ChainIO abstracts operations for accessing raw IPLD objects.
type ChainIO interface {
	ChainReadObj(context.Context, cid.Cid) ([]byte, error)
	ChainHasObj(context.Context, cid.Cid) (bool, error)
}

const LookbackNoLimit = abi.ChainEpoch(-1)

//                       MODIFYING THE API INTERFACE
//
// NOTE: This is the V1 (Unstable) API - to add methods to the V0 (Stable) API
// you'll have to add those methods to interfaces in `api/v0api`
//
// When adding / changing methods in this file:
// * Do the change here
// * Adjust implementation in `node/impl/`
// * Run `make gen` - this will:
//  * Generate proxy structs
//  * Generate mocks
//  * Generate markdown docs
//  * Generate openrpc blobs

type FileRef struct {
	Path  string
	IsCAR bool
}

type MinerSectors struct {
	// Live sectors that should be proven.
	Live uint64
	// Sectors actively contributing to power.
	Active uint64
	// Sectors with failed proofs.
	Faulty uint64
}

type ImportRes struct {
	Root     cid.Cid
	ImportID imports.ID
}

type Import struct {
	Key imports.ID
	Err string

	Root *cid.Cid

	// Source is the provenance of the import, e.g. "import", "unknown", else.
	// Currently useless but may be used in the future.
	Source string

	// FilePath is the path of the original file. It is important that the file
	// is retained at this path, because it will be referenced during
	// the transfer (when we do the UnixFS chunking, we don't duplicate the
	// leaves, but rather point to chunks of the original data through
	// positional references).
	FilePath string

	// CARPath is the path of the CAR file containing the DAG for this import.
	CARPath string
}

type DealInfo struct {
	ProposalCid cid.Cid
	State       storagemarket.StorageDealStatus
	Message     string // more information about deal state, particularly errors
	DealStages  *storagemarket.DealStages
	Provider    address.Address

	DataRef  *storagemarket.DataRef
	PieceCID cid.Cid
	Size     uint64

	PricePerEpoch types.BigInt
	Duration      uint64

	DealID abi.DealID

	CreationTime time.Time
	Verified     bool

	TransferChannelID *datatransfer.ChannelID
	DataTransfer      *DataTransferChannel
}

type MsgLookup struct {
	Message   cid.Cid // Can be different than requested, in case it was replaced, but only gas values changed
	Receipt   types.MessageReceipt
	ReturnDec interface{}
	TipSet    types.TipSetKey
	Height    abi.ChainEpoch
}

type MsgGasCost struct {
	Message            cid.Cid // Can be different than requested, in case it was replaced, but only gas values changed
	GasUsed            abi.TokenAmount
	BaseFeeBurn        abi.TokenAmount
	OverEstimationBurn abi.TokenAmount
	MinerPenalty       abi.TokenAmount
	MinerTip           abi.TokenAmount
	Refund             abi.TokenAmount
	TotalCost          abi.TokenAmount
}

// BlsMessages[x].cid = Cids[x]
// SecpkMessages[y].cid = Cids[BlsMessages.length + y]
type BlockMessages struct {
	BlsMessages   []*types.Message
	SecpkMessages []*types.SignedMessage

	Cids []cid.Cid
}

type Message struct {
	Cid     cid.Cid
	Message *types.Message
}

type ActorState struct {
	Balance types.BigInt
	Code    cid.Cid
	State   interface{}
}

type PCHDir int

const (
	PCHUndef PCHDir = iota
	PCHInbound
	PCHOutbound
)

type PaychStatus struct {
	ControlAddr address.Address
	Direction   PCHDir
}

type ChannelInfo struct {
	Channel      address.Address
	WaitSentinel cid.Cid
}

type ChannelAvailableFunds struct {
	// Channel is the address of the channel
	Channel *address.Address
	// From is the from address of the channel (channel creator)
	From address.Address
	// To is the to address of the channel
	To address.Address
	// ConfirmedAmt is the amount of funds that have been confirmed on-chain
	// for the channel
	ConfirmedAmt types.BigInt
	// PendingAmt is the amount of funds that are pending confirmation on-chain
	PendingAmt types.BigInt
	// PendingWaitSentinel can be used with PaychGetWaitReady to wait for
	// confirmation of pending funds
	PendingWaitSentinel *cid.Cid
	// QueuedAmt is the amount that is queued up behind a pending request
	QueuedAmt types.BigInt
	// VoucherRedeemedAmt is the amount that is redeemed by vouchers on-chain
	// and in the local datastore
	VoucherReedeemedAmt types.BigInt
}

type PaymentInfo struct {
	Channel      address.Address
	WaitSentinel cid.Cid
	Vouchers     []*paych.SignedVoucher
}

type VoucherSpec struct {
	Amount      types.BigInt
	TimeLockMin abi.ChainEpoch
	TimeLockMax abi.ChainEpoch
	MinSettle   abi.ChainEpoch

	Extra *paych.ModVerifyParams
}

// VoucherCreateResult is the response to calling PaychVoucherCreate
type VoucherCreateResult struct {
	// Voucher that was created, or nil if there was an error or if there
	// were insufficient funds in the channel
	Voucher *paych.SignedVoucher
	// Shortfall is the additional amount that would be needed in the channel
	// in order to be able to create the voucher
	Shortfall types.BigInt
}

type MinerPower struct {
	MinerPower  power.Claim
	TotalPower  power.Claim
	HasMinPower bool
}

type QueryOffer struct {
	Err string

	Root  cid.Cid
	Piece *cid.Cid

	Size                    uint64
	MinPrice                types.BigInt
	UnsealPrice             types.BigInt
	PaymentInterval         uint64
	PaymentIntervalIncrease uint64
	Miner                   address.Address
	MinerPeer               retrievalmarket.RetrievalPeer
}

func (o *QueryOffer) Order(client address.Address) RetrievalOrder {
	return RetrievalOrder{
		Root:                    o.Root,
		Piece:                   o.Piece,
		Size:                    o.Size,
		Total:                   o.MinPrice,
		UnsealPrice:             o.UnsealPrice,
		PaymentInterval:         o.PaymentInterval,
		PaymentIntervalIncrease: o.PaymentIntervalIncrease,
		Client:                  client,

		Miner:     o.Miner,
		MinerPeer: &o.MinerPeer,
	}
}

type MarketBalance struct {
	Escrow big.Int
	Locked big.Int
}

type MarketDeal struct {
	Proposal market.DealProposal
	State    market.DealState
}

type RetrievalOrder struct {
	// TODO: make this less unixfs specific
	Root                  cid.Cid
	Piece                 *cid.Cid
	DatamodelPathSelector *textselector.Expression
	Size                  uint64

	FromLocalCAR string // if specified, get data from a local CARv2 file.
	// TODO: support offset
	Total                   types.BigInt
	UnsealPrice             types.BigInt
	PaymentInterval         uint64
	PaymentIntervalIncrease uint64
	Client                  address.Address
	Miner                   address.Address
	MinerPeer               *retrievalmarket.RetrievalPeer
}

type InvocResult struct {
	MsgCid         cid.Cid
	Msg            *types.Message
	MsgRct         *types.MessageReceipt
	GasCost        MsgGasCost
	ExecutionTrace types.ExecutionTrace
	Error          string
	Duration       time.Duration
}

type MethodCall struct {
	types.MessageReceipt
	Error string
}

type IpldObject struct {
	Cid cid.Cid
	Obj interface{}
}

type ActiveSync struct {
	WorkerID uint64
	Base     *types.TipSet
	Target   *types.TipSet

	Stage  SyncStateStage
	Height abi.ChainEpoch

	Start   time.Time
	End     time.Time
	Message string
}

type SyncState struct {
	ActiveSyncs []ActiveSync

	VMApplied uint64
}

type SyncStateStage int

const (
	StageIdle = SyncStateStage(iota)
	StageHeaders
	StagePersistHeaders
	StageMessages
	StageSyncComplete
	StageSyncErrored
	StageFetchingMessages
)

func (v SyncStateStage) String() string {
	switch v {
	case StageIdle:
		return "idle"
	case StageHeaders:
		return "header sync"
	case StagePersistHeaders:
		return "persisting headers"
	case StageMessages:
		return "message sync"
	case StageSyncComplete:
		return "complete"
	case StageSyncErrored:
		return "error"
	case StageFetchingMessages:
		return "fetching messages"
	default:
		return fmt.Sprintf("<unknown: %d>", v)
	}
}

type MpoolChange int

const (
	MpoolAdd MpoolChange = iota
	MpoolRemove
)

type MpoolUpdate struct {
	Type    MpoolChange
	Message *types.SignedMessage
}

type ComputeStateOutput struct {
	Root  cid.Cid
	Trace []*InvocResult
}

type DealCollateralBounds struct {
	Min abi.TokenAmount
	Max abi.TokenAmount
}

type CirculatingSupply struct {
	FilVested           abi.TokenAmount
	FilMined            abi.TokenAmount
	FilBurnt            abi.TokenAmount
	FilLocked           abi.TokenAmount
	FilCirculating      abi.TokenAmount
	FilReserveDisbursed abi.TokenAmount
}

type MiningBaseInfo struct {
	MinerPower        types.BigInt
	NetworkPower      types.BigInt
	Sectors           []builtin.SectorInfo
	WorkerKey         address.Address
	SectorSize        abi.SectorSize
	PrevBeaconEntry   types.BeaconEntry
	BeaconEntries     []types.BeaconEntry
	EligibleForMining bool
}

type BlockTemplate struct {
	Miner            address.Address
	Parents          types.TipSetKey
	Ticket           *types.Ticket
	Eproof           *types.ElectionProof
	BeaconValues     []types.BeaconEntry
	Messages         []*types.SignedMessage
	Epoch            abi.ChainEpoch
	Timestamp        uint64
	WinningPoStProof []builtin.PoStProof
}

type DataSize struct {
	PayloadSize int64
	PieceSize   abi.PaddedPieceSize
}

type DataCIDSize struct {
	PayloadSize int64
	PieceSize   abi.PaddedPieceSize
	PieceCID    cid.Cid
}

type CommPRet struct {
	Root cid.Cid
	Size abi.UnpaddedPieceSize
}
type HeadChange struct {
	Type string
	Val  *types.TipSet
}

type MsigProposeResponse int

const (
	MsigApprove MsigProposeResponse = iota
	MsigCancel
)

type Deadline struct {
	PostSubmissions      bitfield.BitField
	DisputableProofCount uint64
}

type Partition struct {
	AllSectors        bitfield.BitField
	FaultySectors     bitfield.BitField
	RecoveringSectors bitfield.BitField
	LiveSectors       bitfield.BitField
	ActiveSectors     bitfield.BitField
}

type Fault struct {
	Miner address.Address
	Epoch abi.ChainEpoch
}

var EmptyVesting = MsigVesting{
	InitialBalance: types.EmptyInt,
	StartEpoch:     -1,
	UnlockDuration: -1,
}

type MsigVesting struct {
	InitialBalance abi.TokenAmount
	StartEpoch     abi.ChainEpoch
	UnlockDuration abi.ChainEpoch
}

type MessageMatch struct {
	To   address.Address
	From address.Address
}

type MsigTransaction struct {
	ID     int64
	To     address.Address
	Value  abi.TokenAmount
	Method abi.MethodNum
	Params []byte

	Approved []address.Address
}
