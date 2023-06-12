package gql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	gqltypes "github.com/filecoin-project/boost/gql/types"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/chain/consensus"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
)

type msg struct {
	To         string
	From       string
	Nonce      gqltypes.Uint64
	Value      gqltypes.BigInt
	GasFeeCap  gqltypes.BigInt
	GasLimit   gqltypes.Uint64
	GasPremium gqltypes.BigInt
	Method     string
	Params     string
	BaseFee    gqltypes.BigInt
}

// timeStampedMsg wraps the pending msg with a chainEpoch
type timeStampedMsg struct {
	m     msg
	added abi.ChainEpoch // Epoch when message was first noticed in mpool
}

type mpoolMonitor struct {
	ctx                context.Context
	cancel             context.CancelFunc
	fullNode           v1api.FullNode
	lk                 sync.Mutex
	pendingAlertEpochs abi.ChainEpoch
	msgs               map[cid.Cid]*timeStampedMsg
}

func newMpoolMonitor(fullNode v1api.FullNode, cfg config.GraphqlConfig) *mpoolMonitor {
	return &mpoolMonitor{
		fullNode:           fullNode,
		pendingAlertEpochs: abi.ChainEpoch(cfg.PendingAlertEpochs),
	}
}

func (mm *mpoolMonitor) start(ctx context.Context) error {
	log.Infow("Mpool monitor: starting")
	mmctx, cancelfunc := context.WithCancel(ctx)
	mm.ctx = mmctx
	mm.cancel = cancelfunc
	mm.lk.Lock()
	defer mm.lk.Unlock()
	err := mm.update(ctx)
	if err != nil {
		return fmt.Errorf("failed to start mpool monitor: %w", err)
	}
	go mm.startMonitoring(mmctx)
	return nil
}

func (mm *mpoolMonitor) startMonitoring(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			mm.lk.Lock()
			err := mm.update(ctx)
			if err != nil {
				log.Errorf("failed to get messages from mpool: %s", err)
			}
			mm.lk.Unlock()
		}
	}
}

// update gets the current pending messages from mpool. It updated the local
// copy of a message(not timeStampedMsg.added) if CID is already present in MpoolMonitor.msgs
// Otherwise, it inserts a new key value pair for the message. It removed any msgs not found in the
// latest output of MpoolPending
func (mm *mpoolMonitor) update(ctx context.Context) error {
	msgs, err := mm.fullNode.MpoolPending(ctx, types.EmptyTSK)
	if err != nil {
		return fmt.Errorf("getting mpool messages: %w", err)
	}

	ts, err := mm.fullNode.ChainHead(ctx)
	if err != nil {
		return fmt.Errorf("failed to get chain head: %w", err)
	}
	baseFee := ts.Blocks()[0].ParentBaseFee

	newMsgs := make(map[cid.Cid]*timeStampedMsg)

	for _, pm := range msgs {
		var params string
		methodName := pm.Message.Method.String()
		toact, err := mm.fullNode.StateGetActor(ctx, pm.Message.To, types.EmptyTSK)
		if err == nil {
			method, ok := consensus.NewActorRegistry().Methods[toact.Code][pm.Message.Method]
			if ok {
				methodName = method.Name

				params = string(pm.Message.Params)
				p, ok := reflect.New(method.Params.Elem()).Interface().(cbg.CBORUnmarshaler)
				if ok {
					if err := p.UnmarshalCBOR(bytes.NewReader(pm.Message.Params)); err == nil {
						b, err := json.MarshalIndent(p, "", "  ")
						if err == nil {
							params = string(b)
						}
					}
				}
			}
		}

		newMsgs[pm.Cid()] = &timeStampedMsg{
			m: msg{
				To:         pm.Message.To.String(),
				From:       pm.Message.From.String(),
				Nonce:      gqltypes.Uint64(pm.Message.Nonce),
				Value:      gqltypes.BigInt{Int: pm.Message.Value},
				GasFeeCap:  gqltypes.BigInt{Int: pm.Message.GasFeeCap},
				GasLimit:   gqltypes.Uint64(uint64(pm.Message.GasLimit)),
				GasPremium: gqltypes.BigInt{Int: pm.Message.GasPremium},
				Method:     methodName,
				Params:     params,
				BaseFee:    gqltypes.BigInt{Int: baseFee},
			},
			added: ts.Height(),
		}
	}

	mm.lk.Lock()
	defer mm.lk.Unlock()

	// Add new msgs, update older msgs with any changes in values
	for k, v := range newMsgs {
		_, ok := mm.msgs[k]
		if ok {
			mm.msgs[k].m = v.m
		} else {
			mm.msgs[k] = v
		}
	}

	// Remove msgs from mm.msgs if they are not found in updated list
	for k := range mm.msgs {
		_, ok := newMsgs[k]
		if !ok {
			delete(mm.msgs, k)
		}
	}

	return nil
}

func (mm *mpoolMonitor) pendingLocal(ctx context.Context) ([]*msg, error) {
	localAddr := make(map[string]struct{})
	var ret []*msg

	addrs, err := mm.fullNode.WalletList(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting local addresses: %w", err)
	}

	for _, a := range addrs {
		localAddr[a.String()] = struct{}{}
	}

	mm.lk.Lock()
	defer mm.lk.Unlock()

	for _, msg := range mm.msgs {
		if _, has := localAddr[msg.m.From]; !has {
			continue
		}
		ret = append(ret, &msg.m)
	}

	return ret, nil
}

func (mm *mpoolMonitor) pendingAll() ([]*msg, error) {
	var ret []*msg

	mm.lk.Lock()
	defer mm.lk.Unlock()

	for _, msg := range mm.msgs {
		ret = append(ret, &msg.m)
	}

	return ret, nil
}

//func (mm *mpoolMonitor) getAlertMsgs(ctx context.Context) ([]*timeStampedMsg, error) {
//	var ret []*timeStampedMsg
//	ts, err := mm.fullNode.ChainHead(ctx)
//	if err != nil {
//		return nil, fmt.Errorf("failed to get chain head: %w", err)
//	}
//
//	for mcid := range mm.msgs {
//		if mm.msgs[mcid].added+mm.pendingAlertEpochs <= ts.Height() {
//			ret = append(ret, mm.msgs[mcid])
//		}
//	}
//
//	return ret, nil
//}

func (mm *mpoolMonitor) stop() {
	mm.cancel()
}
