package tracing

import (
	"go.opentelemetry.io/otel"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	trace "go.opentelemetry.io/otel/trace"
)

const (
	tracerName = "github.com/filecoin-project/boost"
)

var (
	Tracer = otel.GetTracerProvider().Tracer(
		tracerName,
		trace.WithSchemaURL(semconv.SchemaURL),
	)
)

type Tracing struct{}
