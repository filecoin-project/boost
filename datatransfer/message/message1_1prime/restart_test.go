package message1_1

import (
	"bytes"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"

	datatransfer2 "github.com/filecoin-project/boost/datatransfer"
)

func TestRestartChannelIDValidation(t *testing.T) {
	self := peer.ID("self-peer")
	other := peer.ID("other-peer")
	oversized := peer.ID(strings.Repeat("x", maxPeerIDLen+1))

	tests := []struct {
		name    string
		chid    datatransfer2.ChannelID
		wantErr bool
	}{
		{"valid", datatransfer2.ChannelID{Initiator: self, Responder: other, ID: 1}, false},
		{"ids at limit", datatransfer2.ChannelID{Initiator: peer.ID(strings.Repeat("x", maxPeerIDLen)), Responder: other, ID: 1}, false},
		{"oversized initiator", datatransfer2.ChannelID{Initiator: oversized, Responder: other, ID: 1}, true},
		{"oversized responder", datatransfer2.ChannelID{Initiator: self, Responder: oversized, ID: 1}, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := RestartExistingChannelRequest(tc.chid).RestartChannelId()
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected an error for an oversized channel id")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if got != tc.chid {
				t.Fatalf("got %v, want %v", got, tc.chid)
			}
		})
	}
}

func TestRestartRequestRoundTrip(t *testing.T) {
	chid := datatransfer2.ChannelID{
		Initiator: peer.ID("self-peer"),
		Responder: peer.ID("other-peer"),
		ID:        7,
	}

	var buf bytes.Buffer
	if err := RestartExistingChannelRequest(chid).ToNet(&buf); err != nil {
		t.Fatalf("encoding restart request: %s", err)
	}

	msg, err := FromNet(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("decoding restart request: %s", err)
	}
	req, ok := msg.(datatransfer2.Request)
	if !ok {
		t.Fatalf("decoded message is %T, want datatransfer.Request", msg)
	}
	got, err := req.RestartChannelId()
	if err != nil {
		t.Fatalf("restart channel id: %s", err)
	}
	if got != chid {
		t.Fatalf("got %v, want %v", got, chid)
	}
}
