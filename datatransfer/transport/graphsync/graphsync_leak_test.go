package graphsync

import (
	"errors"
	"testing"

	"github.com/filecoin-project/boost-graphsync"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/traversal"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"

	"github.com/filecoin-project/boost/datatransfer"
	"github.com/filecoin-project/boost/datatransfer/message/message1_1"
	"github.com/filecoin-project/boost/datatransfer/transport/graphsync/extension"
	"github.com/filecoin-project/boost/retrievalmarket/types/legacyretrievaltypes"
	"github.com/filecoin-project/go-state-types/abi"
)

const (
	testLocalPeer  peer.ID = "12D3KooWLocalPeerXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
	testRemotePeer peer.ID = "12D3KooWRemotePeerXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
)

var errTestRejected = errors.New("validation rejected")

// fakeEvents embeds the interface so that only the methods the hook actually
// calls need to be implemented.
type fakeEvents struct {
	datatransfer.EventsHandler
	onRequestReceivedErr  error
	onResponseReceivedErr error
	requestReceived       int
	responseReceived      int
}

func (f *fakeEvents) OnRequestReceived(chid datatransfer.ChannelID, msg datatransfer.Request) (datatransfer.Response, error) {
	f.requestReceived++
	return nil, f.onRequestReceivedErr
}

func (f *fakeEvents) OnResponseReceived(chid datatransfer.ChannelID, msg datatransfer.Response) error {
	f.responseReceived++
	return f.onResponseReceivedErr
}

type fakeHookActions struct {
	terminated error
	validated  bool
	paused     bool
}

func (f *fakeHookActions) SendExtensionData(graphsync.ExtensionData) {}
func (f *fakeHookActions) UsePersistenceOption(string)               {}
func (f *fakeHookActions) UseLinkTargetNodePrototypeChooser(traversal.LinkTargetNodePrototypeChooser) {
}
func (f *fakeHookActions) TerminateWithError(err error) { f.terminated = err }
func (f *fakeHookActions) ValidateRequest()             { f.validated = true }
func (f *fakeHookActions) PauseResponse()               { f.paused = true }

type fakeRequestData struct {
	id   graphsync.RequestID
	root cid.Cid
	sel  ipld.Node
	exts map[graphsync.ExtensionName]datamodel.Node
}

func (f *fakeRequestData) ID() graphsync.RequestID      { return f.id }
func (f *fakeRequestData) Root() cid.Cid                { return f.root }
func (f *fakeRequestData) Selector() ipld.Node          { return f.sel }
func (f *fakeRequestData) Priority() graphsync.Priority { return 0 }
func (f *fakeRequestData) Type() graphsync.RequestType  { return graphsync.RequestTypeNew }

func (f *fakeRequestData) Extension(name graphsync.ExtensionName) (datamodel.Node, bool) {
	nd, ok := f.exts[name]
	return nd, ok
}

func testRequestData(t *testing.T, transferID uint64, reqID graphsync.RequestID) *fakeRequestData {
	t.Helper()

	mh, err := multihash.Sum([]byte("boost-dt-graphsync-leak-test"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	root := cid.NewCidV1(cid.DagCBOR, mh)
	sel := selectorparse.CommonSelector_ExploreAllRecursively

	voucher := &legacyretrievaltypes.DealProposal{
		PayloadCID: root,
		ID:         legacyretrievaltypes.DealID(transferID),
		Params: legacyretrievaltypes.Params{
			PricePerByte:    abi.NewTokenAmount(1),
			UnsealPrice:     abi.NewTokenAmount(1),
			PaymentInterval: 1 << 20,
		},
	}
	msg, err := message1_1.NewRequest(datatransfer.TransferID(transferID), false, true, voucher.Type(), voucher, root, sel)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	exts, err := extension.ToExtensionData(msg, []graphsync.ExtensionName{extension.ExtensionDataTransfer1_1})
	if err != nil {
		t.Fatalf("building extension: %v", err)
	}
	if len(exts) != 1 {
		t.Fatalf("expected 1 extension, got %d", len(exts))
	}

	return &fakeRequestData{
		id:   reqID,
		root: root,
		sel:  sel,
		exts: map[graphsync.ExtensionName]datamodel.Node{exts[0].Name: exts[0].Data},
	}
}

func testResponseData(t *testing.T, transferID uint64, reqID graphsync.RequestID) *fakeRequestData {
	t.Helper()

	mh, err := multihash.Sum([]byte("boost-dt-graphsync-leak-test"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	root := cid.NewCidV1(cid.DagCBOR, mh)
	sel := selectorparse.CommonSelector_ExploreAllRecursively

	voucherResult := &legacyretrievaltypes.DealProposal{
		PayloadCID: root,
		ID:         legacyretrievaltypes.DealID(transferID),
		Params: legacyretrievaltypes.Params{
			PricePerByte:    abi.NewTokenAmount(1),
			UnsealPrice:     abi.NewTokenAmount(1),
			PaymentInterval: 1 << 20,
		},
	}
	msg, err := message1_1.NewResponse(datatransfer.TransferID(transferID), false, false, voucherResult.Type(), voucherResult)
	if err != nil {
		t.Fatalf("building response: %v", err)
	}
	exts, err := extension.ToExtensionData(msg, []graphsync.ExtensionName{extension.ExtensionDataTransfer1_1})
	if err != nil {
		t.Fatalf("building extension: %v", err)
	}

	return &fakeRequestData{
		id:   reqID,
		root: root,
		sel:  sel,
		exts: map[graphsync.ExtensionName]datamodel.Node{exts[0].Name: exts[0].Data},
	}
}

func newTestTransport(t *testing.T, events datatransfer.EventsHandler) *Transport {
	t.Helper()
	tr := NewTransport(testLocalPeer, nil)
	// SetEventHandler would register hooks on the (nil) graphsync exchange,
	// which these tests don't exercise, so wire the handler directly.
	tr.events = events
	return tr
}

func (tr *Transport) channelCount() int {
	tr.dtChannelsLk.RLock()
	defer tr.dtChannelsLk.RUnlock()
	return len(tr.dtChannels)
}

func (tr *Transport) channel(chid datatransfer.ChannelID) (*dtChannel, bool) {
	tr.dtChannelsLk.RLock()
	defer tr.dtChannelsLk.RUnlock()
	ch, ok := tr.dtChannels[chid]
	return ch, ok
}

// A request that fails validation must not leave an entry in dtChannels: no
// other path can ever reclaim it.
func TestRejectedRequestDoesNotLeakChannel(t *testing.T) {
	events := &fakeEvents{onRequestReceivedErr: errTestRejected}
	tr := newTestTransport(t, events)

	req := testRequestData(t, 1, graphsync.NewRequestID())
	actions := &fakeHookActions{}

	tr.gsReqRecdHook(testRemotePeer, req, actions)

	if events.requestReceived != 1 {
		t.Fatalf("expected OnRequestReceived to be called once, got %d", events.requestReceived)
	}
	if actions.terminated == nil {
		t.Fatal("expected the request to be terminated with an error")
	}
	if got := tr.channelCount(); got != 0 {
		t.Fatalf("leaked %d dtChannels entries after a rejected request", got)
	}
}

// The push / response branch tracks the channel before validation too, and
// must clean up on the same early-return path.
func TestRejectedResponseDoesNotLeakChannel(t *testing.T) {
	events := &fakeEvents{onResponseReceivedErr: errTestRejected}
	tr := newTestTransport(t, events)

	tr.gsReqRecdHook(testRemotePeer, testResponseData(t, 2, graphsync.NewRequestID()), &fakeHookActions{})

	if events.responseReceived != 1 {
		t.Fatalf("expected OnResponseReceived to be called once, got %d", events.responseReceived)
	}
	if got := tr.channelCount(); got != 0 {
		t.Fatalf("leaked %d dtChannels entries after a rejected response", got)
	}
}

// Many distinct rejected transfer IDs must not grow the map.
func TestManyRejectedRequestsDoNotAccumulate(t *testing.T) {
	events := &fakeEvents{onRequestReceivedErr: errTestRejected}
	tr := newTestTransport(t, events)

	const n = 1000
	for i := 0; i < n; i++ {
		tr.gsReqRecdHook(testRemotePeer, testRequestData(t, uint64(i+1), graphsync.NewRequestID()), &fakeHookActions{})
	}

	if got := tr.channelCount(); got != 0 {
		t.Fatalf("leaked %d dtChannels entries after %d rejected requests", got, n)
	}
}

// A rejected follow-up request for a channel that is already active must not
// tear down the live channel.
func TestRejectedFollowUpKeepsActiveChannel(t *testing.T) {
	events := &fakeEvents{}
	tr := newTestTransport(t, events)

	reqID := graphsync.NewRequestID()
	tr.gsReqRecdHook(testRemotePeer, testRequestData(t, 9, reqID), &fakeHookActions{})

	chid := datatransfer.ChannelID{ID: 9, Initiator: testRemotePeer, Responder: testLocalPeer}
	if _, ok := tr.channel(chid); !ok {
		t.Fatal("expected the accepted channel to be tracked")
	}

	events.onRequestReceivedErr = errTestRejected
	tr.gsReqRecdHook(testRemotePeer, testRequestData(t, 9, graphsync.NewRequestID()), &fakeHookActions{})

	if _, ok := tr.channel(chid); !ok {
		t.Fatal("a rejected follow-up request removed the active channel")
	}
}

// A request that passes validation must keep the tracked channel, with the
// request ID registered so later cancel/complete callbacks can find it.
func TestAcceptedRequestKeepsChannel(t *testing.T) {
	events := &fakeEvents{}
	tr := newTestTransport(t, events)

	reqID := graphsync.NewRequestID()
	req := testRequestData(t, 7, reqID)
	actions := &fakeHookActions{}

	tr.gsReqRecdHook(testRemotePeer, req, actions)

	if !actions.validated {
		t.Fatal("expected the request to be validated")
	}
	chid := datatransfer.ChannelID{ID: 7, Initiator: testRemotePeer, Responder: testLocalPeer}
	ch, ok := tr.channel(chid)
	if !ok {
		t.Fatal("expected the accepted channel to remain tracked")
	}
	if !ch.isOpen || ch.requestID == nil || *ch.requestID != reqID {
		t.Fatalf("expected accepted channel to be open with request ID registered, got isOpen=%v requestID=%v", ch.isOpen, ch.requestID)
	}
	if _, ok := tr.requestIDToChannelID.load(reqID); !ok {
		t.Fatal("expected request ID to be registered for later callbacks")
	}
}
