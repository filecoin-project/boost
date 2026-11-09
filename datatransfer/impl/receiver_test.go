package impl

import (
	"context"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/filecoin-project/boost/datatransfer"
	"github.com/filecoin-project/boost/datatransfer/message"
)

func TestValidRestartChannelID(t *testing.T) {
	self := peer.ID("self-peer")
	sender := peer.ID("sender-peer")

	tests := []struct {
		name  string
		chid  datatransfer.ChannelID
		valid bool
	}{
		{"legitimate request", datatransfer.ChannelID{Initiator: self, Responder: sender, ID: 1}, true},
		{"foreign initiator", datatransfer.ChannelID{Initiator: peer.ID("someone-else"), Responder: sender, ID: 1}, false},
		{"foreign responder", datatransfer.ChannelID{Initiator: self, Responder: peer.ID("someone-else"), ID: 1}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := validRestartChannelID(tc.chid, self, sender); got != tc.valid {
				t.Fatalf("validRestartChannelID = %t, want %t", got, tc.valid)
			}
		})
	}
}

// TestReceiveRestartExistingChannelRequestRejectsBeforeUse checks that a
// restart request whose channel id does not name a channel this node initiated
// with the sender as counterparty is rejected before the id is formatted or
// looked up. The manager deliberately has no spans index, channels or
// transport: reaching any of them would panic, so returning without a panic
// proves the cheap validation happened first.
func TestReceiveRestartExistingChannelRequestRejectsBeforeUse(t *testing.T) {
	self := peer.ID("self-peer")
	sender := peer.ID("sender-peer")
	r := &receiver{manager: &manager{peerID: self}}

	oversized := peer.ID(strings.Repeat("x", 4096))
	tests := []struct {
		name string
		chid datatransfer.ChannelID
	}{
		{"foreign initiator", datatransfer.ChannelID{Initiator: peer.ID("someone-else"), Responder: sender, ID: 1}},
		{"foreign responder", datatransfer.ChannelID{Initiator: self, Responder: peer.ID("someone-else"), ID: 1}},
		{"oversized initiator", datatransfer.ChannelID{Initiator: oversized, Responder: sender, ID: 1}},
		{"oversized responder", datatransfer.ChannelID{Initiator: self, Responder: oversized, ID: 1}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := message.RestartExistingChannelRequest(tc.chid)
			r.ReceiveRestartExistingChannelRequest(context.Background(), sender, req)
		})
	}
}
