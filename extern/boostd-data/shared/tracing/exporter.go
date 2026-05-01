package tracing

import (
	"context"
	"net"
	"net/url"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
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
// an OTLP HTTP exporter that will send spans to the provided endpoint. Legacy
// Jaeger collector URLs are translated to the equivalent OTLP HTTP endpoint so
// the existing tracing flag default keeps working. The returned TracerProvider
// will also use a Resource configured with all the information about the
// application.
func tracerProvider(url, service string) (*sdktrace.TracerProvider, error) {
	exp, err := newExporter(context.Background(), url)
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

func newExporter(ctx context.Context, endpoint string) (sdktrace.SpanExporter, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return otlptracehttp.New(ctx)
	}

	if strings.Contains(endpoint, "://") {
		return newExporterFromURL(ctx, normalizeTracingEndpoint(endpoint))
	}

	return otlptracehttp.New(ctx, otlptracehttp.WithEndpoint(endpoint))
}

func newExporterFromURL(ctx context.Context, endpoint string) (sdktrace.SpanExporter, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}

	opts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(u.Host),
	}
	if u.Scheme == "http" {
		opts = append(opts, otlptracehttp.WithInsecure())
	}
	if u.Path != "" && u.Path != "/" {
		opts = append(opts, otlptracehttp.WithURLPath(u.Path))
	}

	return otlptracehttp.New(ctx, opts...)
}

func normalizeTracingEndpoint(endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil || u.Path != "/api/traces" {
		return endpoint
	}

	u.Path = "/v1/traces"
	if u.Port() == "14268" {
		u.Host = net.JoinHostPort(u.Hostname(), "4318")
	}

	return u.String()
}
