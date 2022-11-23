package filters

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

// PeerListType is either an allow list or a deny list
type PeerListType string

// AllowList is a peer list where only the specified peers are allowed to serve retrievals
const AllowList PeerListType = "allowlist"

// DenyList is a peer list where the specified peers cannot serve retrievals, but all others can
const DenyList PeerListType = "denylist"

// PeerFilter manages updating a deny list and checking for CID inclusion in that list
type PeerFilter struct {
	peerListLk   sync.RWMutex
	peerList     map[peer.ID]struct{}
	peerListType PeerListType
}

// NewPeerFilter constructs a new peer filter
func NewPeerFilter() *PeerFilter {
	return &PeerFilter{
		peerListType: DenyList,
		peerList:     make(map[peer.ID]struct{}),
	}
}

// FulfillRequest checks if a given peer is in the allow/deny list and decides
// whether to fulfill the request
func (pf *PeerFilter) FulfillRequest(p peer.ID, c cid.Cid) (bool, error) {
	pf.peerListLk.RLock()
	defer pf.peerListLk.RUnlock()
	_, has := pf.peerList[p]
	return (pf.peerListType == DenyList) != has, nil
}

// parse an AllowDenyList to get a new set of allowed/denied peers
// it uses streaming JSON decoding to avoid an intermediate copy of the entire response
// lenSuggestion is used to avoid a large number of allocations as the list grows
func (pf *PeerFilter) parseAllowDenyList(response io.Reader) (PeerListType, map[peer.ID]struct{}, error) {
	type allowDenyList struct {
		Type    string   `json:"Type"`
		PeerIDs []string `json:"PeerIDs"`
	}
	type responseType struct {
		AllowDenyList allowDenyList `json:"AllowDenyList"`
	}
	jsonResponse := json.NewDecoder(response)
	// initialize a json decoder
	var decodedResponse responseType
	err := jsonResponse.Decode(&decodedResponse)
	// read open bracket
	if err != nil {
		return "", nil, fmt.Errorf("parsing response: %w", err)
	}

	if decodedResponse.AllowDenyList.Type != string(DenyList) && decodedResponse.AllowDenyList.Type != string(AllowList) {
		return "", nil, fmt.Errorf("parsing response: 'Type' must be either '%s' or '%s'", AllowList, DenyList)
	}
	peerList := make(map[peer.ID]struct{}, len(decodedResponse.AllowDenyList.PeerIDs))
	// while the array contains values
	for _, peerString := range decodedResponse.AllowDenyList.PeerIDs {
		peerID, err := peer.Decode(peerString)
		if err != nil {
			return "", nil, fmt.Errorf("parsing response: %w", err)
		}
		peerList[peerID] = struct{}{}
	}

	return PeerListType(decodedResponse.AllowDenyList.Type), peerList, nil
}

// ParseUpdate parses and updates the Peer filter list based on an endpoint response
func (pf *PeerFilter) ParseUpdate(stream io.Reader) error {
	peerListType, peerList, err := pf.parseAllowDenyList(stream)
	if err != nil {
		return err
	}
	pf.peerListLk.Lock()
	pf.peerListType = peerListType
	pf.peerList = peerList
	pf.peerListLk.Unlock()
	return nil
}
