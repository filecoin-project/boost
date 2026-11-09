package tracing

import (
	"context"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/filecoin-project/boost/datatransfer"
)

func TestEndChannelSpanRemovesEntry(t *testing.T) {
	si := NewSpansIndex()
	chid := datatransfer.ChannelID{Initiator: peer.ID("a"), Responder: peer.ID("b"), ID: 1}

	si.SpanForChannel(context.Background(), chid)
	if len(si.spans) != 1 {
		t.Fatalf("spans index has %d entries, want 1", len(si.spans))
	}

	si.EndChannelSpan(chid)
	if len(si.spans) != 0 {
		t.Fatalf("spans index has %d entries after cleanup, want 0", len(si.spans))
	}

	// CleanupChannel may be called more than once for the same channel.
	si.EndChannelSpan(chid)
}
