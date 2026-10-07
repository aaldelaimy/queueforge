package worker

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/aaldelaimy/queueforge/internal/job"
	"github.com/aaldelaimy/queueforge/internal/metrics"
)

func TestWorkerCompletedIncrementsCounter(t *testing.T) {
	m := metrics.New(prometheus.NewRegistry())
	store := job.NewMemoryStore()
	cfg := testConfig
	cfg.Metrics = m
	w := New(store, nil, DefaultHandlers(), cfg)

	j := mustCreate(t, store, "hash", json.RawMessage(`{"input":"hello"}`))
	w.process(t.Context(), j.ID)

	text := scrape(t, m)
	if !strings.Contains(text, "queueforge_jobs_completed_total 1") {
		t.Errorf("completed:\n%s", text)
	}
	if !strings.Contains(text, "queueforge_jobs_started_total 1") {
		t.Errorf("started:\n%s", text)
	}
	if !strings.Contains(text, "queueforge_job_duration_seconds_count 1") {
		t.Errorf("duration:\n%s", text)
	}
	if strings.Contains(text, j.ID) {
		t.Errorf("job_id must not be a metric label:\n%s", text)
	}
}

func TestPermanentFailureCountsFailedNotRetry(t *testing.T) {
	m := metrics.New(prometheus.NewRegistry())
	store := job.NewMemoryStore()
	cfg := testConfig
	cfg.Metrics = m
	w := New(store, nil, DefaultHandlers(), cfg)

	j := mustCreate(t, store, "hash", json.RawMessage(`{}`))
	w.process(t.Context(), j.ID)

	text := scrape(t, m)
	if !strings.Contains(text, "queueforge_jobs_failed_total 1") {
		t.Errorf("failed:\n%s", text)
	}
	if !strings.Contains(text, "queueforge_jobs_retries_total 0") {
		t.Errorf("retries should stay 0:\n%s", text)
	}
}

func TestTransientFailuresCountRetriesThenDeadLettered(t *testing.T) {
	m := metrics.New(prometheus.NewRegistry())
	store, q := job.NewMemoryStore(), newQueue()
	cfg := testConfig
	cfg.Metrics = m
	startWorkers(t, 1, store, q, DefaultHandlers(), cfg)
	runInBackground(t, NewSweeper(store, q, SweeperConfig{
		Interval:     2 * time.Millisecond,
		RequeueAfter: time.Hour,
		Backoff:      cfg.Backoff,
		Metrics:      m,
	}).Run)

	j := submit(t, store, q, "fail", nil)
	got := waitForTerminal(t, store, j.ID)
	if got.Status != job.StatusDeadLettered {
		t.Fatalf("status %s", got.Status)
	}

	text := scrape(t, m)
	// Three attempts: two retries, then dead_lettered.
	if !strings.Contains(text, "queueforge_jobs_retries_total 2") {
		t.Errorf("retries:\n%s", text)
	}
	if !strings.Contains(text, "queueforge_jobs_dead_lettered_total 1") {
		t.Errorf("dead lettered:\n%s", text)
	}
	if !strings.Contains(text, "queueforge_jobs_failed_total 0") {
		t.Errorf("failed is for permanent errors only:\n%s", text)
	}
	if !strings.Contains(text, "queueforge_jobs_started_total 3") {
		t.Errorf("started:\n%s", text)
	}
}

func TestSweeperAbandonedCountsRetry(t *testing.T) {
	m := metrics.New(prometheus.NewRegistry())
	store, q := job.NewMemoryStore(), newQueue()
	s := NewSweeper(store, q, SweeperConfig{
		Interval:     time.Hour,
		RequeueAfter: time.Hour,
		Backoff:      Backoff{},
		Metrics:      m,
	})
	j := mustCreate(t, store, "hash", json.RawMessage(`{"input":"x"}`))
	claimAndDie(t, store, j.ID)
	s.sweep(t.Context())

	text := scrape(t, m)
	if !strings.Contains(text, "queueforge_jobs_retries_total 1") {
		t.Errorf("abandoned should count as a retry:\n%s", text)
	}
	if strings.Contains(text, "queueforge_job_duration_seconds_count 1") {
		t.Errorf("sweeper must not record processing duration:\n%s", text)
	}
}

func scrape(t *testing.T, m *metrics.Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics status: %d", rec.Code)
	}
	body, err := io.ReadAll(rec.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestStaleRequeueIsCounted(t *testing.T) {
	m := metrics.New(prometheus.NewRegistry())
	store, q := job.NewMemoryStore(), newQueue()
	s := NewSweeper(store, q, SweeperConfig{Interval: time.Hour, RequeueAfter: -time.Second, Metrics: m})
	mustCreate(t, store, "hash", nil)
	s.sweep(t.Context())

	if text := scrape(t, m); !strings.Contains(text, "queueforge_jobs_stale_requeued_total 1") {
		t.Errorf("stale requeue:\n%s", text)
	}
}
