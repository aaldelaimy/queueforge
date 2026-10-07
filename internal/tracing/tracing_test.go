package tracing

import (
	"context"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// The API stores TraceParent(ctx) on the job; the worker calls
// WithTraceParent on it. A span started there must be in the same trace,
// as a child of the API span.
func TestTraceParentRoundTrip(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	tracer := tp.Tracer("test")

	apiCtx, apiSpan := tracer.Start(context.Background(), "POST /jobs")
	traceparent := TraceParent(apiCtx)
	apiSpan.End()
	if traceparent == "" {
		t.Fatal("TraceParent is empty for a sampled span")
	}

	workerCtx := WithTraceParent(context.Background(), traceparent)
	_, workerSpan := tracer.Start(workerCtx, "job.process")
	workerSpan.End()

	spans := rec.Ended()
	if len(spans) != 2 {
		t.Fatalf("got %d spans, want 2", len(spans))
	}
	api, worker := spans[0], spans[1]
	if worker.SpanContext().TraceID() != api.SpanContext().TraceID() {
		t.Error("worker span is in a different trace")
	}
	if worker.Parent().SpanID() != api.SpanContext().SpanID() {
		t.Error("worker span's parent is not the API span")
	}
}

func TestNoSpanMeansNoTraceParent(t *testing.T) {
	if got := TraceParent(context.Background()); got != "" {
		t.Errorf("TraceParent: got %q, want empty", got)
	}
	if got := TraceID(context.Background()); got != "" {
		t.Errorf("TraceID: got %q, want empty", got)
	}
	ctx := WithTraceParent(context.Background(), "")
	if ctx != context.Background() {
		t.Error("empty traceparent should return ctx unchanged")
	}
}
