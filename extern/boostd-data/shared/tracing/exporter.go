package tracing

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
)

// Registers a Tracer provider globally and returns it's shutdown function
func New(service, endpoint string) (func(context.Context) error, error) {
	provider, err := tracerProvider(endpoint, service)
	if err != nil {
		return nil, err
	}

	// Sets the global Tracer provider
	otel.SetTracerProvider(provider)

	// Opencensus bridge so that older OC spans are compatible with OT
	// This is specifically to support capturing traces in contexts provided to go-jsonrpc,
	// as it creates spans with OC instead of OT
	//
	// TODO: Commenting this out for now as it causes dependency issues:
	// github.com/filecoin-project/boost/tracing imports
	//	go.opentelemetry.io/otel/bridge/opencensus imports
	//	go.opentelemetry.io/otel/sdk/metric/export: module go.opentelemetry.io/otel/sdk/metric@latest found (v0.34.0), but does not contain package go.opentelemetry.io/otel/sdk/metric/export
	// ...
	//octrace.DefaultTracer = opencensus.NewTracer(Tracer)

	return provider.Shutdown, nil
}

// tracerProvider returns an OpenTelemetry TracerProvider configured to use
// the Jaeger exporter that will send spans to the provided url. The returned
// TracerProvider will also use a Resource configured with all the information
// about the application.
func tracerProvider(url, service string) (*sdktrace.TracerProvider, error) {
	// Create the Jaeger exporter
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(url)))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		// Always be sure to batch in production.
		sdktrace.WithBatcher(exp),
		// Record information about this application in a Resource.
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(service),
		)),
	)
	return tp, nil
}
