package mpoolMonitor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("mpoolmonitor")

// timeStampedMsg wraps the pending msg with a chainEpoch
type timeStampedMsg struct {
	m     types.SignedMessage
	added abi.ChainEpoch // Epoch when message was first noticed in mpool
}

type MpoolMonitor struct {
	ctx                context.Context
	cancel             context.CancelFunc
	fullNode           v1api.FullNode
	lk                 sync.Mutex
	pendingAlertEpochs abi.ChainEpoch
	msgs               map[cid.Cid]*timeStampedMsg
}

func NewMonitor(fullNode v1api.FullNode, cfg config.GraphqlConfig) *MpoolMonitor {
	return &MpoolMonitor{
		fullNode:           fullNode,
		pendingAlertEpochs: abi.ChainEpoch(cfg.PendingAlertEpochs),
		msgs:               make(map[cid.Cid]*timeStampedMsg),
	}
}

func (mm *MpoolMonitor) Start(ctx context.Context) error {
	log.Infow("Mpool monitor: starting")
	mmctx, cancel := context.WithCancel(ctx)
	mm.ctx = mmctx
	mm.cancel = cancel
	err := mm.update(ctx)
	if err != nil {
		return fmt.Errorf("failed to start mpool monitor: %w", err)
	}
	go mm.startMonitoring(mmctx)
	return nil
}

func (mm *MpoolMonitor) startMonitoring(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := mm.update(ctx)
			if err != nil {
				log.Errorf("failed to get messages from mpool: %s", err)
			}
		}
	}
}

// update gets the current pending messages from mpool. It updated the local
// copy of a message(not timeStampedMsg.added) if CID is already present in MpoolMonitor.msgs
// Otherwise, it inserts a new key value pair for the message. It removed any msgs not found in the
// latest output of MpoolPending
func (mm *MpoolMonitor) update(ctx context.Context) error {
	ts, err := mm.fullNode.ChainHead(ctx)
	if err != nil {
		return fmt.Errorf("failed to get chain head: %w", err)
	}
	msgs, err := mm.fullNode.MpoolPending(ctx, types.EmptyTSK)
	if err != nil {
		return fmt.Errorf("getting mpool messages: %w", err)
	}

	newMsgs := make(map[cid.Cid]*timeStampedMsg)

	for _, pm := range msgs {
		newMsgs[pm.Cid()] = &timeStampedMsg{
			m:     *pm,
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

func (mm *MpoolMonitor) PendingLocal(ctx context.Context) ([]*types.SignedMessage, error) {
	localAddr := make(map[string]struct{})
	var ret []*types.SignedMessage

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
		if _, has := localAddr[msg.m.Message.From.String()]; !has {
			continue
		}
		ret = append(ret, &msg.m)
	}

	return ret, nil
}

func (mm *MpoolMonitor) PendingAll() ([]*types.SignedMessage, error) {
	var ret []*types.SignedMessage

	mm.lk.Lock()
	defer mm.lk.Unlock()

	for _, msg := range mm.msgs {
		ret = append(ret, &msg.m)
	}

	return ret, nil
}

func (mm *MpoolMonitor) Alerts(ctx context.Context) ([]cid.Cid, error) {
	var ret []cid.Cid
	ts, err := mm.fullNode.ChainHead(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get chain head: %w", err)
	}

	mm.lk.Lock()
	defer mm.lk.Unlock()

	for mcid := range mm.msgs {
		if mm.msgs[mcid].added+mm.pendingAlertEpochs <= ts.Height() {
			ret = append(ret, mcid)
		}
	}

	return ret, nil
}

func (mm *MpoolMonitor) Stop(ctx context.Context) error {
	mm.cancel()
	return nil
}
