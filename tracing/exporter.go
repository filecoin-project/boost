package tracing

import (
	"context"
	"fmt"

	logging "github.com/ipfs/go-log/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
)

var log = logging.Logger("tracing")

// Registers a Tracer provider globally and returns it's shutdown function
func New(ctx context.Context) (func(context.Context) error, error) {
	client := otlptracehttp.NewClient()
	exporter, err := otlptrace.New(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP trace exporter: %w", err)
	}

	// Represents the entity producing the telemetry
	resource, _ := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("boost"),
		),
	)

	// Centralized entity providing Tracers and funnels telemetry to exporters
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource),
	)

	// Sets the global Tracer provider
	otel.SetTracerProvider(provider)

	return provider.Shutdown, nil
}
