package mpoolmonitor

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("mpoolmonitor")

// TimeStampedMsg wraps the pending msg with a chainEpoch
type TimeStampedMsg struct {
	SignedMessage types.SignedMessage
	Added         abi.ChainEpoch // Epoch when message was first noticed in mpool
}

type MpoolMonitor struct {
	ctx              context.Context
	cancel           context.CancelFunc
	fullNode         v1api.FullNode
	lk               sync.Mutex
	mpoolAlertEpochs abi.ChainEpoch
	msgs             map[cid.Cid]*TimeStampedMsg
}

func NewMonitor(fullNode v1api.FullNode, mpoolAlertEpochs int64) *MpoolMonitor {
	return &MpoolMonitor{
		fullNode:         fullNode,
		mpoolAlertEpochs: abi.ChainEpoch(mpoolAlertEpochs),
		msgs:             make(map[cid.Cid]*TimeStampedMsg),
	}
}

func (mm *MpoolMonitor) Start(ctx context.Context) error {
	log.Infow("Mpool monitor: starting")
	mmctx, cancel := context.WithCancel(ctx)
	mm.ctx = mmctx
	mm.cancel = cancel
	go mm.startMonitoring(mmctx)
	return nil
}

func (mm *MpoolMonitor) startMonitoring(ctx context.Context) {
	err := mm.update(ctx)
	if err != nil {
		log.Errorf("failed to start mpool monitor: %s", err)
	}

	// Run 3 times in an epoch
	ticker := time.NewTicker(time.Duration(math.Floor(float64(build.BlockDelaySecs)/3)) * time.Second)
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
// copy of a message(not TimeStampedMsg.added) if CID is already present in MpoolMonitor.msgs
// Otherwise, it inserts a new key value pair for the message. It removed any msgs not found in the
// latest output of MpoolPending
func (mm *MpoolMonitor) update(ctx context.Context) error {
	localAddr := make(map[string]struct{})

	addrs, err := mm.fullNode.WalletList(ctx)
	if err != nil {
		return fmt.Errorf("getting local addresses: %w", err)
	}

	for _, a := range addrs {
		localAddr[a.String()] = struct{}{}
	}

	ts, err := mm.fullNode.ChainHead(ctx)
	if err != nil {
		return fmt.Errorf("failed to get chain head: %w", err)
	}
	msgs, err := mm.fullNode.MpoolPending(ctx, types.EmptyTSK)
	if err != nil {
		return fmt.Errorf("getting mpool messages: %w", err)
	}

	newMsgs := make(map[cid.Cid]*TimeStampedMsg)

	for _, pm := range msgs {

		if _, has := localAddr[pm.Message.From.String()]; !has {
			continue
		}

		newMsgs[pm.Cid()] = &TimeStampedMsg{
			SignedMessage: *pm,
			Added:         ts.Height(),
		}
	}

	mm.lk.Lock()
	defer mm.lk.Unlock()

	// Add new msgs, update older msgs with any changes in values
	for k, v := range newMsgs {
		_, ok := mm.msgs[k]
		if ok {
			mm.msgs[k].SignedMessage = v.SignedMessage
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

// PendingLocal generates a list of all local pending messages in lotus node
func (mm *MpoolMonitor) PendingLocal(ctx context.Context) ([]*TimeStampedMsg, error) {
	var ret []*TimeStampedMsg

	mm.lk.Lock()
	defer mm.lk.Unlock()

	for _, msg := range mm.msgs {
		ret = append(ret, msg)
	}

	return ret, nil
}

// Alerts generate a list if messages stuck in local mpool for more than mm.mpoolAlertEpochs epochs
func (mm *MpoolMonitor) Alerts(ctx context.Context) ([]*TimeStampedMsg, error) {
	var ret []*TimeStampedMsg
	ts, err := mm.fullNode.ChainHead(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get chain head: %w", err)
	}

	mm.lk.Lock()
	defer mm.lk.Unlock()
	for _, msg := range mm.msgs {
		if msg.Added+mm.mpoolAlertEpochs <= ts.Height() {
			ret = append(ret, msg)
		}
	}

	return ret, nil
}

func (mm *MpoolMonitor) Stop(ctx context.Context) error {
	mm.cancel()
	return nil
}

func (mm *MpoolMonitor) MsgInMpool(msgCid cid.Cid) bool {
	mm.lk.Lock()
	defer mm.lk.Unlock()
	_, ok := mm.msgs[msgCid]
	return ok
}

func (mm *MpoolMonitor) MsgExecElapsedEpochs(ctx context.Context, msgCid cid.Cid) (bool, abi.ChainEpoch, error) {
	found := mm.MsgInMpool(msgCid)
	if found {
		return found, 0, nil
	}
	x, err := mm.fullNode.StateSearchMsg(ctx, types.EmptyTSK, msgCid, abi.ChainEpoch(20), true)
	// check for nil is required as the StateSearchMsg / ChainHead sometimes return a nil pointer
	// without an error (TODO: investigate) that has caused panics in boost
	if x == nil {
		return found, 0, fmt.Errorf("Message not yet found in state store") // nolint:staticcheck
	}
	if err != nil {
		return found, 0, fmt.Errorf("searching message: %w", err)
	}
	c, err := mm.fullNode.ChainHead(ctx)
	if c == nil {
		return found, 0, fmt.Errorf("chain head is nil")
	}
	if err != nil {
		return found, 0, fmt.Errorf("getting chain head: %w", err)
	}
	return found, c.Height() - x.Height, nil
}
