// Package metrics defines QueueForge's Prometheus instruments. Job IDs are
// never labels (one series per job); they go in logs.
package metrics

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds every instrument. A nil *Metrics is a no-op.
type Metrics struct {
	submitted    prometheus.Counter
	started      prometheus.Counter
	completed    prometheus.Counter
	failed       prometheus.Counter
	deadLettered prometheus.Counter
	retries      prometheus.Counter
	staleRequeue prometheus.Counter
	queueDepth   prometheus.Gauge
	jobDuration  prometheus.Histogram
	httpDuration *prometheus.HistogramVec
	registry     *prometheus.Registry
	// queueDepth is registered only by the process that polls it; see
	// PollQueueDepth.
	depthOnce sync.Once
}

// New registers the instruments on reg. Use a dedicated registry so tests
// don't share series.
func New(reg *prometheus.Registry) *Metrics {
	m := &Metrics{
		submitted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "queueforge_jobs_submitted_total",
			Help: "Jobs accepted by POST /jobs (replays of an Idempotency-Key are not counted).",
		}),
		started: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "queueforge_jobs_started_total",
			Help: "Attempts started: a worker claimed the job (queued -> running).",
		}),
		completed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "queueforge_jobs_completed_total",
			Help: "Jobs that reached status completed.",
		}),
		failed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "queueforge_jobs_failed_total",
			Help: "Jobs that reached status failed: a permanent error, not retried.",
		}),
		deadLettered: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "queueforge_jobs_dead_lettered_total",
			Help: "Jobs that reached status dead_lettered: transient failures until attempts ran out.",
		}),
		retries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "queueforge_jobs_retries_total",
			Help: "Attempts recorded as retrying (transient failure with attempts left).",
		}),
		staleRequeue: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "queueforge_jobs_stale_requeued_total",
			Help: "IDs the sweeper pushed again because a queued job waited too long (its ID was presumably lost). Each one that was not really lost becomes a skipped duplicate.",
		}),
		queueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "queueforge_queue_depth",
			Help: "IDs waiting in the Redis queue. A gauge: it goes down as workers dequeue. Exported by the API only.",
		}),
		jobDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "queueforge_job_duration_seconds",
			Help:    "Wall time of one worker attempt, from after Claim until the outcome is recorded. Does not include time spent queued.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120},
		}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "queueforge_http_request_duration_seconds",
			Help:    "HTTP handler time. Labels are the route pattern, not the URL, so /jobs/{id} is one series.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route", "code"}),
		registry: reg,
	}
	reg.MustRegister(m.submitted, m.started, m.completed, m.failed, m.deadLettered, m.retries, m.staleRequeue, m.jobDuration, m.httpDuration)
	return m
}

// Handler serves the Prometheus text format at /metrics.
func (m *Metrics) Handler() http.Handler {
	if m == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) JobSubmitted() {
	if m != nil {
		m.submitted.Inc()
	}
}

func (m *Metrics) JobStarted() {
	if m != nil {
		m.started.Inc()
	}
}

func (m *Metrics) JobCompleted() {
	if m != nil {
		m.completed.Inc()
	}
}

func (m *Metrics) JobFailed() {
	if m != nil {
		m.failed.Inc()
	}
}

func (m *Metrics) JobDeadLettered() {
	if m != nil {
		m.deadLettered.Inc()
	}
}

func (m *Metrics) JobRetried() {
	if m != nil {
		m.retries.Inc()
	}
}

func (m *Metrics) JobRequeuedStale() {
	if m != nil {
		m.staleRequeue.Inc()
	}
}

func (m *Metrics) SetQueueDepth(n float64) {
	if m != nil {
		m.queueDepth.Set(n)
	}
}

func (m *Metrics) ObserveJobDuration(d time.Duration) {
	if m != nil {
		m.jobDuration.Observe(d.Seconds())
	}
}

func (m *Metrics) ObserveHTTP(method, route string, status int, d time.Duration) {
	if m != nil {
		m.httpDuration.WithLabelValues(method, route, strconv.Itoa(status)).Observe(d.Seconds())
	}
}

// PollQueueDepth sets the queue-depth gauge from depth() until ctx is done.
// Only a process that calls it exports the gauge, so workers don't report a
// fake 0 next to the API's real value.
func PollQueueDepth(ctx context.Context, m *Metrics, depth func(context.Context) (int64, error), every time.Duration) {
	if m == nil {
		return
	}
	m.depthOnce.Do(func() { m.registry.MustRegister(m.queueDepth) })
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		if n, err := depth(ctx); err == nil {
			m.SetQueueDepth(float64(n))
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
