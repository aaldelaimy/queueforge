package api

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/aaldelaimy/queueforge/internal/job"
	"github.com/aaldelaimy/queueforge/internal/worker"
)

// Submits a job that takes 500ms through the real API and a real worker.
// The POST must answer while the job is still unfinished, and the job must
// complete later on its own.
func TestRequestReturnsBeforeWorkFinishes(t *testing.T) {
	store, q := job.NewMemoryStore(), newQueue()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go worker.New(store, q, worker.DefaultHandlers(), worker.DefaultConfig()).Run(ctx)

	h := NewServer(store, q, []string{"sleep"}).Routes()

	start := time.Now()
	rec := do(h, http.MethodPost, "/jobs", `{"type":"sleep","payload":{"duration_ms":500}}`)
	postTook := time.Since(start)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status: got %d, want 202", rec.Code)
	}
	created := decode[job.Job](t, rec)
	if created.Status != job.StatusQueued {
		t.Errorf("POST response status: got %s, want queued", created.Status)
	}
	if postTook >= 500*time.Millisecond {
		t.Errorf("POST took %v; it waited for the work", postTook)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		got := decode[job.Job](t, do(h, http.MethodGet, "/jobs/"+created.ID, ""))
		if got.Status == job.StatusCompleted {
			if string(got.Result) != `{"slept_ms":500}` {
				t.Errorf("result: got %s", got.Result)
			}
			return
		}
		if got.Status == job.StatusFailed || time.Now().After(deadline) {
			t.Fatalf("job did not complete: status %s, error %q", got.Status, got.Error)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
