package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/aaldelaimy/queueforge/internal/applog"
	"github.com/aaldelaimy/queueforge/internal/job"
	"github.com/aaldelaimy/queueforge/internal/metrics"
	"github.com/aaldelaimy/queueforge/internal/queue"
	"github.com/aaldelaimy/queueforge/internal/tracing"
)

// Queue is where a Worker gets job IDs. Dequeue returns queue.ErrEmpty if
// nothing arrived within its wait.
type Queue interface {
	Dequeue(ctx context.Context) (string, error)
}

const DefaultJobTimeout = 90 * time.Second

// Config controls how a Worker runs and retries jobs.
type Config struct {
	// JobTimeout is how long one attempt may run before it is cancelled and
	// counted as a transient failure.
	JobTimeout time.Duration
	// Backoff decides the wait before a failed job's next attempt.
	Backoff Backoff
	// Logger receives structured events. nil uses slog.Default.
	Logger *slog.Logger
	// Metrics records attempt outcomes. nil is a no-op.
	Metrics *metrics.Metrics
}

func DefaultConfig() Config {
	return Config{JobTimeout: DefaultJobTimeout, Backoff: DefaultBackoff()}
}

// Worker takes job IDs off a queue and runs each job with the handler
// registered for its type.
type Worker struct {
	store      job.Store
	queue      Queue
	handlers   map[string]Handler
	cfg        Config
	logger     *slog.Logger
	metrics    *metrics.Metrics
	retryDelay time.Duration
}

func New(store job.Store, q Queue, handlers map[string]Handler, cfg Config) *Worker {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Worker{store: store, queue: q, handlers: handlers, cfg: cfg, logger: log, metrics: cfg.Metrics, retryDelay: time.Second}
}

// Run processes jobs one at a time until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	w.RunUntil(ctx, ctx)
}

// RunUntil stops taking jobs once stop is cancelled. The job in progress
// keeps running under jobs, which the caller cancels after a grace period.
func (w *Worker) RunUntil(stop, jobs context.Context) {
	for stop.Err() == nil {
		id, err := w.queue.Dequeue(stop)
		switch {
		case err == nil:
			// The ID is already off the queue, so run it even if stop was just
			// cancelled.
			w.process(jobs, id)
		case errors.Is(err, queue.ErrEmpty), stop.Err() != nil:
		default:
			// Queue unreachable: back off instead of spinning.
			w.logger.Error("dequeue failed",
				applog.AttrEvent, "queue.dequeue_failed",
				applog.AttrError, err.Error())
			select {
			case <-stop.Done():
			case <-time.After(w.retryDelay):
			}
		}
	}
}

func (w *Worker) process(ctx context.Context, id string) {
	log := w.logger.With(applog.AttrJobID, id)

	// Claim fails if the job isn't queued any more (duplicate delivery, or
	// another worker has it). The lease is 2x the timeout so a live worker
	// always finishes before the sweeper considers it dead.
	claimedAt := time.Now()
	j, err := w.store.Claim(ctx, id, 2*w.cfg.JobTimeout)
	if err != nil {
		log.Info("skipping job",
			applog.AttrEvent, "job.skipped",
			applog.AttrError, err.Error())
		return
	}
	w.metrics.JobStarted()

	// Continue the submitting request's trace; retries join it too.
	ctx, span := tracing.Tracer().Start(tracing.WithTraceParent(ctx, j.TraceParent), "job.process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithTimestamp(claimedAt),
		trace.WithAttributes(
			attribute.String("job.id", j.ID),
			attribute.String("job.type", j.Type),
			attribute.Int("job.attempt", j.Attempts),
			attribute.Int("job.max_attempts", j.MaxAttempts),
		))
	defer span.End()
	if traceID := tracing.TraceID(ctx); traceID != "" {
		log = log.With(applog.AttrTraceID, traceID)
	}

	start := time.Now()
	result, err := w.run(ctx, j)
	d := time.Since(start)

	// Record the outcome even if shutdown started, or the job would sit in
	// running until its lease expires.
	recordCtx := context.WithoutCancel(ctx)

	if err != nil {
		updated, rerr := recordFailure(recordCtx, w.store, w.cfg.Backoff, j, err)
		if rerr != nil {
			log.Error("recording failure",
				applog.AttrEvent, "job.record_failed",
				applog.AttrError, rerr.Error())
			return
		}
		recordAttemptMetrics(w.metrics, updated.Status, d)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(attribute.String("job.status", string(updated.Status)))
		log.Info("job attempt failed",
			applog.AttrEvent, "job.failed",
			"job_type", j.Type,
			"attempt", j.Attempts,
			"max_attempts", j.MaxAttempts,
			"status", string(updated.Status),
			applog.AttrError, err.Error())
		return
	}

	if _, err := w.store.Complete(recordCtx, id, result); err != nil {
		log.Error("recording result",
			applog.AttrEvent, "job.complete_failed",
			applog.AttrError, err.Error())
		return
	}
	recordAttemptMetrics(w.metrics, job.StatusCompleted, d)
	span.SetAttributes(attribute.String("job.status", string(job.StatusCompleted)))
	log.Info("job completed",
		applog.AttrEvent, "job.completed",
		"job_type", j.Type,
		"attempt", j.Attempts)
}

// run calls the handler with a JobTimeout deadline and turns panics into
// permanent errors.
func (w *Worker) run(ctx context.Context, j job.Job) (result json.RawMessage, err error) {
	h, ok := w.handlers[j.Type]
	if !ok {
		return nil, Permanent(fmt.Errorf("unknown job type %q", j.Type))
	}

	ctx, cancel := context.WithTimeout(ctx, w.cfg.JobTimeout)
	defer cancel()

	defer func() {
		// The same input would most likely panic again, so don't retry.
		if r := recover(); r != nil {
			err = Permanent(fmt.Errorf("handler panicked: %v", r))
		}
	}()

	result, err = h(ctx, j)
	if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		err = fmt.Errorf("timed out after %s: %w", w.cfg.JobTimeout, err)
	}
	return result, err
}

// recordFailure records a failed attempt. Transient failures with attempts
// left are retried after a backoff delay; anything else fails the job.
func recordFailure(ctx context.Context, store job.Store, b Backoff, j job.Job, cause error) (job.Job, error) {
	switch {
	case isPermanent(cause):
		return store.Fail(ctx, j.ID, cause.Error())
	case j.Attempts >= j.MaxAttempts:
		return store.DeadLetter(ctx, j.ID, fmt.Sprintf("%v (gave up after %d attempts)", cause, j.Attempts))
	default:
		return store.Retry(ctx, j.ID, cause.Error(), b.Delay(j.Attempts))
	}
}

func recordAttemptMetrics(m *metrics.Metrics, status job.Status, d time.Duration) {
	m.ObserveJobDuration(d)
	recordAttemptOutcome(m, status)
}

func recordAttemptOutcome(m *metrics.Metrics, status job.Status) {
	switch status {
	case job.StatusCompleted:
		m.JobCompleted()
	case job.StatusRetrying:
		m.JobRetried()
	case job.StatusFailed:
		m.JobFailed()
	case job.StatusDeadLettered:
		m.JobDeadLettered()
	}
}
