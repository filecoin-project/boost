package requestcounter

import (
	"sync"

	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

type requestKey struct {
	p peer.ID
	c cid.Cid
}

type RequestCounter struct {
	requestsLk           sync.RWMutex
	requestsCountsByPeer map[peer.ID]uint64
	requestsInProgress   map[requestKey]struct{}
}

func NewRequestCounter() *RequestCounter {
	return &RequestCounter{
		requestsCountsByPeer: make(map[peer.ID]uint64),
		requestsInProgress:   make(map[requestKey]struct{}),
	}
}

type ServerState struct {
	TotalRequestsInProgress   uint64
	RequestsInProgressForPeer uint64
}

func (rc *RequestCounter) StateForPeer(p peer.ID) ServerState {
	rc.requestsLk.RLock()
	defer rc.requestsLk.RUnlock()
	return ServerState{
		TotalRequestsInProgress:   uint64(len(rc.requestsInProgress)),
		RequestsInProgressForPeer: rc.requestsCountsByPeer[p],
	}
}

func (rc *RequestCounter) AddRequest(p peer.ID, c cid.Cid) {
	rc.requestsLk.Lock()
	defer rc.requestsLk.Unlock()
	_, existing := rc.requestsInProgress[requestKey{p, c}]
	if existing {
		return
	}
	rc.requestsInProgress[requestKey{p, c}] = struct{}{}
	rc.requestsCountsByPeer[p] += 1
}

func (rc *RequestCounter) RemoveRequest(p peer.ID, c cid.Cid) {
	rc.requestsLk.Lock()
	defer rc.requestsLk.Unlock()
	_, existing := rc.requestsInProgress[requestKey{p, c}]
	if !existing {
		return
	}
	delete(rc.requestsInProgress, requestKey{p, c})
	rc.requestsCountsByPeer[p] -= 1
}
