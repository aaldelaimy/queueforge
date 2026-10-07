package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/aaldelaimy/queueforge/internal/job"
	"github.com/aaldelaimy/queueforge/internal/metrics"
)

func TestReplayDoesNotCountAsSubmitted(t *testing.T) {
	m := metrics.New(prometheus.NewRegistry())
	h := NewServer(job.NewMemoryStore(), newQueue(), testJobTypes).WithMetrics(m).Routes()

	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"type":"a"}`))
	req.Header.Set("Idempotency-Key", "same")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("first: %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"type":"a"}`))
	req.Header.Set("Idempotency-Key", "same")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("replay: %d", rec.Code)
	}

	text := scrape(t, h)
	if !strings.Contains(text, "queueforge_jobs_submitted_total 1") {
		t.Errorf("replay must not increment submitted:\n%s", text)
	}
}

func TestEnqueueFailureDoesNotCountAsSubmitted(t *testing.T) {
	m := metrics.New(prometheus.NewRegistry())
	h := NewServer(job.NewMemoryStore(), brokenQueue{}, testJobTypes).WithMetrics(m).Routes()
	if rec := do(h, http.MethodPost, "/jobs", `{"type":"a"}`); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status: %d", rec.Code)
	}
	if text := scrape(t, h); strings.Contains(text, "queueforge_jobs_submitted_total 1") {
		t.Errorf("503 must not count as submitted:\n%s", text)
	}
}

func TestHTTPDurationUsesRoutePatternNotJobID(t *testing.T) {
	m := metrics.New(prometheus.NewRegistry())
	h := NewServer(job.NewMemoryStore(), newQueue(), testJobTypes).WithMetrics(m).Routes()

	a := decode[job.Job](t, do(h, http.MethodPost, "/jobs", `{"type":"a"}`))
	b := decode[job.Job](t, do(h, http.MethodPost, "/jobs", `{"type":"a"}`))
	do(h, http.MethodGet, "/jobs/"+a.ID, "")
	do(h, http.MethodGet, "/jobs/"+b.ID, "")

	text := scrape(t, h)
	if strings.Contains(text, a.ID) || strings.Contains(text, b.ID) {
		t.Errorf("job IDs must not appear as metric labels:\n%s", text)
	}
	if !strings.Contains(text, `route="GET /jobs/{id}"`) {
		t.Errorf("expected route template:\n%s", text)
	}
	if !strings.Contains(text, `route="POST /jobs"`) {
		t.Errorf("expected POST /jobs route:\n%s", text)
	}
}

func scrape(t *testing.T, h http.Handler) string {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics status: %d", rec.Code)
	}
	body, err := io.ReadAll(rec.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}
