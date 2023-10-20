package storedask

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/markets/shared"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/storagemarket/types/legacytypes"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	logging "github.com/ipfs/go-log/v2"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
)

var log = logging.Logger("storedask")

// DefaultPrice is the default price for unverified deals (in attoFil / GiB / Epoch)
var DefaultPrice = abi.NewTokenAmount(500000000)

// DefaultVerifiedPrice is the default price for verified deals (in attoFil / GiB / Epoch)
var DefaultVerifiedPrice = abi.NewTokenAmount(50000000)

// DefaultDuration is the default number of epochs a storage ask is in effect for
const DefaultDuration abi.ChainEpoch = 1000000

// DefaultMinPieceSize is the minimum accepted piece size for data
const DefaultMinPieceSize abi.PaddedPieceSize = 256

// DefaultMaxPieceSize is the default maximum accepted size for pieces for deals
// TODO: It would be nice to default this to the miner's sector size
const DefaultMaxPieceSize abi.PaddedPieceSize = 1 << 20

type StoredAsk struct {
	askLk    sync.RWMutex
	asks     map[address.Address]*legacytypes.SignedStorageAsk
	fullNode api.FullNode
	db       *db.StorageAskDB
}

// NewStoredAsk returns a new instance of StoredAsk
// It will initialize a new SignedStorageAsk on disk if one is not set
// Otherwise it loads the current SignedStorageAsk from disk
func NewStoredAsk(cfg *config.Boost) func(lc fx.Lifecycle, db *db.StorageAskDB, fullNode api.FullNode) (*StoredAsk, error) {
	return func(lc fx.Lifecycle, db *db.StorageAskDB, fullNode api.FullNode) (*StoredAsk, error) {
		s := &StoredAsk{
			fullNode: fullNode,
			db:       db,
			asks:     make(map[address.Address]*legacytypes.SignedStorageAsk),
		}

		ctx := context.Background()

		var minerIDs []address.Address
		miner, err := address.NewFromString(cfg.Wallets.Miner)
		if err != nil {
			return nil, fmt.Errorf("converting miner ID from config: %w", err)
		}
		minerIDs = append(minerIDs, miner)

		for _, m := range minerIDs {
			ask, err := s.getSignedAsk(ctx, m)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					// If not found set everything to default
					serr := s.SetAsk(ctx, DefaultPrice, DefaultVerifiedPrice, DefaultDuration, m)
					if serr == nil {
						continue
					}
					return nil, fmt.Errorf("setting default ask for miner id %s: %w", m.String(), serr)
				}
				return nil, fmt.Errorf("failed to initialise AskStore: %w", err)
			}
			s.asks[m] = &ask
		}

		return s, nil
	}
}

func signBytes(ctx context.Context, signer address.Address, b []byte, f api.FullNode) (*crypto.Signature, error) {
	signer, err := f.StateAccountKey(ctx, signer, types.EmptyTSK)
	if err != nil {
		return nil, err
	}

	log.Debugf("signing the ask %s with address %s", string(b), signer.String())

	localSignature, err := f.WalletSign(ctx, signer, b)
	if err != nil {
		return nil, err
	}
	return localSignature, nil
}

func getMinerWorkerAddress(ctx context.Context, maddr address.Address, tok shared.TipSetToken, f api.FullNode) (address.Address, error) {
	tsk, err := types.TipSetKeyFromBytes(tok)
	if err != nil {
		return address.Undef, err
	}

	mi, err := f.StateMinerInfo(ctx, maddr, tsk)
	if err != nil {
		return address.Address{}, err
	}
	return mi.Worker, nil
}

func (s *StoredAsk) sign(ctx context.Context, ask *legacytypes.StorageAsk) (*crypto.Signature, error) {
	tok, err := s.fullNode.ChainHead(ctx)
	if err != nil {
		return nil, err
	}

	return signMinerData(ctx, ask, ask.Miner, tok.Key().Bytes(), s.fullNode)
}

// SignMinerData signs the given data structure with a signature for the given address
func signMinerData(ctx context.Context, data interface{}, address address.Address, tok shared.TipSetToken, f api.FullNode) (*crypto.Signature, error) {
	msg, err := cborutil.Dump(data)
	if err != nil {
		return nil, xerrors.Errorf("serializing: %w", err)
	}

	worker, err := getMinerWorkerAddress(ctx, address, tok, f)
	if err != nil {
		return nil, err
	}

	sig, err := signBytes(ctx, worker, msg, f)
	if err != nil {
		return nil, xerrors.Errorf("failed to sign: %w", err)
	}
	return sig, nil
}

func (s *StoredAsk) GetAsk(miner address.Address) *legacytypes.SignedStorageAsk {
	s.askLk.RLock()
	defer s.askLk.RUnlock()

	return s.asks[miner]
}

func (s *StoredAsk) SetAsk(ctx context.Context, price abi.TokenAmount, verifiedPrice abi.TokenAmount, duration abi.ChainEpoch, miner address.Address, options ...legacytypes.StorageAskOption) error {
	s.askLk.Lock()
	defer s.askLk.Unlock()
	var seqno uint64
	minPieceSize := DefaultMinPieceSize
	maxPieceSize := DefaultMaxPieceSize

	oldAsk, ok := s.asks[miner]
	if ok {
		seqno = oldAsk.Ask.SeqNo + 1
		minPieceSize = oldAsk.Ask.MinPieceSize
		maxPieceSize = oldAsk.Ask.MaxPieceSize
	}

	ts, err := s.fullNode.ChainHead(ctx)
	if err != nil {
		return err
	}
	ask := &legacytypes.StorageAsk{
		Price:         price,
		VerifiedPrice: verifiedPrice,
		Timestamp:     ts.Height(),
		Expiry:        ts.Height() + duration,
		Miner:         miner,
		SeqNo:         seqno,
		MinPieceSize:  minPieceSize,
		MaxPieceSize:  maxPieceSize,
	}

	for _, option := range options {
		option(ask)
	}

	sig, err := s.sign(ctx, ask)
	if err != nil {
		return err
	}

	s.asks[miner] = &legacytypes.SignedStorageAsk{
		Ask:       ask,
		Signature: sig,
	}
	return s.storeAsk(ctx, *ask)

}

func (s *StoredAsk) getSignedAsk(ctx context.Context, miner address.Address) (legacytypes.SignedStorageAsk, error) {
	ask, err := s.db.Get(ctx, miner)
	if err != nil {
		return legacytypes.SignedStorageAsk{}, err
	}
	ss, err := s.sign(ctx, &ask)
	if err != nil {
		return legacytypes.SignedStorageAsk{}, nil
	}

	return legacytypes.SignedStorageAsk{
		Ask:       &ask,
		Signature: ss,
	}, nil
}

func (s *StoredAsk) storeAsk(ctx context.Context, ask legacytypes.StorageAsk) error {
	return s.db.Update(ctx, ask)
}
