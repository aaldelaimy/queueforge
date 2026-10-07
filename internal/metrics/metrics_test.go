package metrics

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestCountersAndGauge(t *testing.T) {
	m := New(prometheus.NewRegistry())

	m.JobSubmitted()
	m.JobSubmitted()
	m.JobStarted()
	m.JobCompleted()
	m.JobFailed()
	m.JobDeadLettered()
	m.JobRequeuedStale()
	m.JobRetried()
	m.JobRetried()
	m.SetQueueDepth(7)

	if got := testutil.ToFloat64(m.submitted); got != 2 {
		t.Errorf("submitted: %v", got)
	}
	if got := testutil.ToFloat64(m.started); got != 1 {
		t.Errorf("started: %v", got)
	}
	if got := testutil.ToFloat64(m.deadLettered); got != 1 {
		t.Errorf("dead lettered: %v", got)
	}
	if got := testutil.ToFloat64(m.completed); got != 1 {
		t.Errorf("completed: %v", got)
	}
	if got := testutil.ToFloat64(m.failed); got != 1 {
		t.Errorf("failed: %v", got)
	}
	if got := testutil.ToFloat64(m.retries); got != 2 {
		t.Errorf("retries: %v", got)
	}
	if got := testutil.ToFloat64(m.queueDepth); got != 7 {
		t.Errorf("queue depth: %v", got)
	}
}

func TestNilMetricsAreNoop(t *testing.T) {
	var m *Metrics
	m.JobSubmitted()
	m.JobStarted()
	m.JobCompleted()
	m.JobFailed()
	m.JobDeadLettered()
	m.JobRequeuedStale()
	m.JobRetried()
	m.SetQueueDepth(1)
	m.ObserveJobDuration(time.Millisecond)
	m.ObserveHTTP("GET", "/jobs", 200, time.Millisecond)
}

func TestHTTPDurationUsesRouteNotURL(t *testing.T) {
	m := New(prometheus.NewRegistry())
	m.ObserveHTTP("GET", "GET /jobs/{id}", 200, 10*time.Millisecond)
	m.ObserveHTTP("GET", "GET /jobs/{id}", 200, 20*time.Millisecond)
	m.ObserveHTTP("POST", "POST /jobs", 202, 5*time.Millisecond)

	count := testutil.CollectAndCount(m.httpDuration)
	if count != 2 {
		t.Errorf("series: got %d, want 2 (one per method+route+code, not per job id)", count)
	}
}

func TestMetricsHandlerPrometheusText(t *testing.T) {
	m := New(prometheus.NewRegistry())
	m.JobSubmitted()

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	text := string(body)
	if !strings.Contains(text, "queueforge_jobs_submitted_total") {
		t.Errorf("missing submitted counter:\n%s", text)
	}
	if !strings.Contains(text, `TYPE queueforge_jobs_submitted_total counter`) {
		t.Errorf("missing TYPE line:\n%s", text)
	}
}

func TestPollQueueDepth(t *testing.T) {
	m := New(prometheus.NewRegistry())
	var n atomic.Int64
	n.Store(3)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go PollQueueDepth(ctx, m, func(context.Context) (int64, error) { return n.Load(), nil }, 5*time.Millisecond)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if testutil.ToFloat64(m.queueDepth) == 3 {
			n.Store(1)
			for time.Now().Before(deadline) {
				if testutil.ToFloat64(m.queueDepth) == 1 {
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
			t.Fatalf("depth did not drop to 1")
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("depth was never 3")
}

// Workers never poll the queue, so they must not export a depth of 0.
func TestQueueDepthExportedOnlyWhenPolled(t *testing.T) {
	m := New(prometheus.NewRegistry())
	if text := scrapeText(t, m); strings.Contains(text, "queueforge_queue_depth") {
		t.Fatal("queue depth exported without PollQueueDepth")
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go PollQueueDepth(ctx, m, func(context.Context) (int64, error) { return 5, nil }, time.Millisecond)

	deadline := time.Now().Add(time.Second)
	for !strings.Contains(scrapeText(t, m), "queueforge_queue_depth 5") {
		if time.Now().After(deadline) {
			t.Fatal("queue depth never exported")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func scrapeText(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	return string(body)
}

func TestJobDurationHistogramCountsSamples(t *testing.T) {
	m := New(prometheus.NewRegistry())
	m.ObserveJobDuration(100 * time.Millisecond)
	m.ObserveJobDuration(2 * time.Second)
	if got := testutil.CollectAndCount(m.jobDuration); got != 1 {
		t.Errorf("job duration series: %d", got)
	}
	if err := testutil.CollectAndCompare(m.jobDuration, strings.NewReader(`
# HELP queueforge_job_duration_seconds Wall time of one worker attempt, from after Claim until the outcome is recorded. Does not include time spent queued.
# TYPE queueforge_job_duration_seconds histogram
queueforge_job_duration_seconds_bucket{le="0.005"} 0
queueforge_job_duration_seconds_bucket{le="0.01"} 0
queueforge_job_duration_seconds_bucket{le="0.025"} 0
queueforge_job_duration_seconds_bucket{le="0.05"} 0
queueforge_job_duration_seconds_bucket{le="0.1"} 1
queueforge_job_duration_seconds_bucket{le="0.25"} 1
queueforge_job_duration_seconds_bucket{le="0.5"} 1
queueforge_job_duration_seconds_bucket{le="1"} 1
queueforge_job_duration_seconds_bucket{le="2.5"} 2
queueforge_job_duration_seconds_bucket{le="5"} 2
queueforge_job_duration_seconds_bucket{le="10"} 2
queueforge_job_duration_seconds_bucket{le="30"} 2
queueforge_job_duration_seconds_bucket{le="60"} 2
queueforge_job_duration_seconds_bucket{le="120"} 2
queueforge_job_duration_seconds_bucket{le="+Inf"} 2
queueforge_job_duration_seconds_sum 2.1
queueforge_job_duration_seconds_count 2
`)); err != nil {
		t.Error(err)
	}
}
