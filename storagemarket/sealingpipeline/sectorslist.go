package sealingpipeline

import (
	"context"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	sealing "github.com/filecoin-project/lotus/storage/pipeline"
	logging "github.com/ipfs/go-log/v2"
	"sort"
	"sync"
	"time"
)

var log = logging.Logger("sectors-list")

type SectorState struct {
	SectorNumber  abi.SectorNumber
	State         api.SectorState
	Unsealed      bool
	Deals         []abi.DealID
	Active        bool
	DealWeight    big.Int
	VerifiedPower big.Int
	Expiration    abi.ChainEpoch
	OnChain       bool
}

type onUpdateCb func(error)

type SectorsList struct {
	listLk      sync.Mutex
	list        []SectorState
	initOnce    sync.Once
	initialized chan struct{}
	updatedAt   time.Time

	syncUpdate chan onUpdateCb

	fullNode v1api.FullNode
	spApi    API
}

func NewSectorsList(fullNode v1api.FullNode, spApi API) *SectorsList {
	return &SectorsList{
		initialized: make(chan struct{}),
		syncUpdate:  make(chan onUpdateCb, 256),
		fullNode:    fullNode,
		spApi:       spApi,
	}
}

func (l *SectorsList) Run(ctx context.Context) {
	timer := time.NewTicker(10 * time.Minute)
	defer timer.Stop()

	update := func() error {
		err := l.update(ctx)
		if err != nil {
			log.Warnf("failed to update sectors list: %s", err)
		} else {
			log.Debugf("updated sectors list")
		}
		return err
	}

	// Update the list immediately
	_ = update()

	// Update the list on every tick
	for ctx.Err() == nil {
		var err error
		select {
		case <-ctx.Done():
			return
		case onComplete := <-l.syncUpdate:
			err = l.update(ctx)
			onComplete(err)
		case <-timer.C:
			err = update()
		}

		// If there are any other threads waiting for an update, call
		// their callbacks also
		moreCallbacks := true
		for moreCallbacks {
			select {
			case onComplete := <-l.syncUpdate:
				onComplete(err)
			default:
				moreCallbacks = false
			}
		}
	}
}

// Synchronously refresh the list
func (l *SectorsList) Refresh(ctx context.Context) error {
	done := make(chan error, 1)
	onUpdate := func(err error) {
		done <- err
	}
	select {
	case l.syncUpdate <- onUpdate:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *SectorsList) update(ctx context.Context) error {
	head, err := l.fullNode.ChainHead(ctx)
	if err != nil {
		return err
	}

	maddr, err := l.spApi.ActorAddress(ctx)
	if err != nil {
		return err
	}

	activeSet, err := l.fullNode.StateMinerActiveSectors(ctx, maddr, head.Key())
	if err != nil {
		return err
	}
	activeIDs := make(map[abi.SectorNumber]struct{}, len(activeSet))
	for _, info := range activeSet {
		activeIDs[info.SectorNumber] = struct{}{}
	}

	sset, err := l.fullNode.StateMinerSectors(ctx, maddr, nil, head.Key())
	if err != nil {
		return err
	}
	commitedIDs := make(map[abi.SectorNumber]struct{}, len(sset))
	for _, info := range sset {
		commitedIDs[info.SectorNumber] = struct{}{}
	}

	apiList, err := l.spApi.SectorsList(ctx)
	if err != nil {
		return err
	}

	sort.Slice(apiList, func(i, j int) bool {
		return apiList[i] < apiList[j]
	})

	sectorsList := make([]SectorState, 0, len(apiList))
	for _, sectorNum := range apiList {
		st, err := l.spApi.SectorsStatus(ctx, sectorNum, true)
		if err != nil {
			return err
		}

		//
		// Copied from code for lotus-miner sectors list command
		//
		const verifiedPowerGainMul = 9
		dw := big.NewInt(0)
		vp := big.NewInt(0)
		estimate := (st.Expiration-st.Activation <= 0) || sealing.IsUpgradeState(sealing.SectorState(st.State))
		if !estimate {
			rdw := big.Add(st.DealWeight, st.VerifiedDealWeight)
			dw = big.Div(rdw, big.NewInt(int64(st.Expiration-st.Activation)))
			vp = big.Div(big.Mul(st.VerifiedDealWeight, big.NewInt(verifiedPowerGainMul)), big.NewInt(int64(st.Expiration-st.Activation)))
		} else {
			for _, piece := range st.Pieces {
				if piece.DealInfo != nil {
					dw = big.Add(dw, big.NewInt(int64(piece.Piece.Size)))
					if piece.DealInfo.DealProposal != nil && piece.DealInfo.DealProposal.VerifiedDeal {
						vp = big.Add(vp, big.Mul(big.NewInt(int64(piece.Piece.Size)), big.NewInt(verifiedPowerGainMul)))
					}
				}
			}
		}

		_, active := activeIDs[sectorNum]
		_, onChain := commitedIDs[sectorNum]
		sectorsList = append(sectorsList, SectorState{
			SectorNumber: sectorNum,
			State:        st.State,
			// TODO: get Unsealed state from unsealed state manager once this PR has landed:
			// https://github.com/filecoin-project/boost/pull/1463
			Unsealed:      true,
			Deals:         st.Deals,
			Active:        active,
			DealWeight:    dw,
			VerifiedPower: vp,
			Expiration:    st.Expiration,
			OnChain:       onChain,
		})
	}

	l.listLk.Lock()
	defer l.listLk.Unlock()
	l.list = sectorsList
	l.updatedAt = time.Now()
	l.initOnce.Do(func() { close(l.initialized) })

	return nil
}

type SectorsListSnapshot struct {
	List       []SectorState
	TotalCount int
	More       bool
	At         time.Time
}

func (l *SectorsList) Snapshot(ctx context.Context, offset int, limit int) (SectorsListSnapshot, error) {
	select {
	case <-ctx.Done():
		return SectorsListSnapshot{}, ctx.Err()
	case <-l.initialized:
	}

	l.listLk.Lock()
	defer l.listLk.Unlock()

	list := l.list
	if offset < len(list) {
		list = list[offset:]
	}

	more := len(list) > limit
	if more {
		// Truncate list to limit
		list = list[:limit]
	}

	return SectorsListSnapshot{List: list, TotalCount: len(l.list), More: more, At: l.updatedAt}, nil
}
