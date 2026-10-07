package api

import (
	"net/http"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/aaldelaimy/queueforge/internal/job"
)

// POST /jobs must store the request span's traceparent on the job, which
// is how the worker, in another process, joins the same trace.
func TestCreateJobStoresTraceParent(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	store := job.NewMemoryStore()
	h := NewServer(store, newQueue(), testJobTypes).Routes()
	created := decode[job.Job](t, do(h, http.MethodPost, "/jobs", `{"type":"a"}`))

	stored, err := store.Get(t.Context(), created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.TraceParent == "" {
		t.Fatal("job has no traceparent")
	}

	var request sdktrace.ReadOnlySpan
	names := map[string]bool{}
	for _, s := range rec.Ended() {
		names[s.Name()] = true
		if s.Name() == "POST /jobs" {
			request = s
		}
	}
	for _, want := range []string{"POST /jobs", "store.create", "queue.enqueue"} {
		if !names[want] {
			t.Errorf("missing span %q; got %v", want, names)
		}
	}
	if request == nil {
		t.FailNow()
	}
	// traceparent is version-traceid-spanid-flags.
	want := "00-" + request.SpanContext().TraceID().String() + "-" + request.SpanContext().SpanID().String() + "-01"
	if stored.TraceParent != want {
		t.Errorf("traceparent: got %s, want %s (the request span)", stored.TraceParent, want)
	}
}
