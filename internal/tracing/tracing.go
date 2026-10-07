// Package tracing sets up OpenTelemetry. Redis only carries job IDs, so a
// trace crosses the queue as a traceparent stored on the job row.
package tracing

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Tracer returns the tracer for QueueForge spans. It is a no-op until Setup.
func Tracer() trace.Tracer {
	return otel.Tracer("queueforge")
}

var propagator = propagation.TraceContext{}

// Setup exports spans over OTLP/HTTP if OTEL_EXPORTER_OTLP_ENDPOINT is set.
// The returned function flushes buffered spans.
func Setup(ctx context.Context, service string) (shutdown func(context.Context) error, err error) {
	otel.SetTextMapPropagator(propagator)
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}
	exp, err := otlptracehttp.New(ctx) // reads the endpoint from the environment
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", service))),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// TraceParent returns the W3C traceparent for the span in ctx, or "" if
// there is no sampled span.
func TraceParent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// WithTraceParent returns ctx carrying the remote span described by
// traceparent, so spans started from it continue that trace.
func WithTraceParent(ctx context.Context, traceparent string) context.Context {
	if traceparent == "" {
		return ctx
	}
	return propagator.Extract(ctx, propagation.MapCarrier{"traceparent": traceparent})
}

// Extract continues a trace from an incoming traceparent header.
func Extract(ctx context.Context, h propagation.HeaderCarrier) context.Context {
	return propagator.Extract(ctx, h)
}

// TraceID returns the trace ID of the span in ctx for log correlation, or "".
func TraceID(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.HasTraceID() {
		return ""
	}
	return sc.TraceID().String()
}
