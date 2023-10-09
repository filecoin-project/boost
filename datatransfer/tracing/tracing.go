package tracing

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/filecoin-project/boost/datatransfer"
)

type SpansIndex struct {
	spansLk sync.RWMutex
	spans   map[datatransfer.ChannelID]trace.Span
}

func NewSpansIndex() *SpansIndex {
	return &SpansIndex{
		spans: make(map[datatransfer.ChannelID]trace.Span),
	}
}

func (si *SpansIndex) SpanForChannel(ctx context.Context, chid datatransfer.ChannelID) (context.Context, trace.Span) {
	si.spansLk.RLock()
	span, ok := si.spans[chid]
	si.spansLk.RUnlock()
	if ok {
		return trace.ContextWithSpan(ctx, span), span
	}
	si.spansLk.Lock()
	defer si.spansLk.Unlock()
	// need to recheck under the write lock
	span, ok = si.spans[chid]
	if ok {
		return trace.ContextWithSpan(ctx, span), span
	}
	ctx, span = otel.Tracer("data-transfer").Start(ctx, "transfer", trace.WithAttributes(
		attribute.String("channelID", chid.String()),
	))
	si.spans[chid] = span
	return ctx, span
}

func (si *SpansIndex) EndChannelSpan(chid datatransfer.ChannelID) {
	si.spansLk.Lock()
	defer si.spansLk.Unlock()
	span, ok := si.spans[chid]
	if ok {
		span.End()
		delete(si.spans, chid)
	}
}

func (si *SpansIndex) EndAll() {
	si.spansLk.Lock()
	defer si.spansLk.Unlock()
	for _, span := range si.spans {
		span.End()
	}
	// reset in case someone continues to use the span index
	si.spans = make(map[datatransfer.ChannelID]trace.Span)
}
