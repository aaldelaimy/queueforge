package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/aaldelaimy/queueforge/internal/applog"
	"github.com/aaldelaimy/queueforge/internal/job"
	"github.com/aaldelaimy/queueforge/internal/metrics"
)

// SweeperQueue is where the sweeper sends IDs that need running again.
// Len tells it whether there is a backlog.
type SweeperQueue interface {
	Enqueue(ctx context.Context, id string) error
	Len(ctx context.Context) (int64, error)
}

const (
	DefaultSweeperInterval     = time.Second
	DefaultRequeueAfter        = 30 * time.Second
	DefaultBacklogRequeueAfter = 10 * time.Minute
)

// SweeperConfig controls how often the sweeper runs and what it recovers.
type SweeperConfig struct {
	Interval time.Duration
	// RequeueAfter: re-push a queued job's ID after this long, if the queue
	// is empty (so the ID can't still be waiting in line).
	RequeueAfter time.Duration
	// BacklogRequeueAfter: the same limit during a backlog, when old queued
	// jobs are usually just waiting. A backstop, so keep it long.
	BacklogRequeueAfter time.Duration
	Backoff             Backoff
	// Logger receives structured events. nil uses slog.Default.
	Logger *slog.Logger
	// Metrics records recovered attempts. nil is a no-op.
	Metrics *metrics.Metrics
}

func DefaultSweeperConfig() SweeperConfig {
	return SweeperConfig{
		Interval:            DefaultSweeperInterval,
		RequeueAfter:        DefaultRequeueAfter,
		BacklogRequeueAfter: DefaultBacklogRequeueAfter,
		Backoff:             DefaultBackoff(),
	}
}

// Sweeper periodically repairs what the queue loses: expired leases, due
// retries, and lost IDs. Safe to run in every worker process.
type Sweeper struct {
	store   job.Store
	queue   SweeperQueue
	cfg     SweeperConfig
	logger  *slog.Logger
	metrics *metrics.Metrics
}

func NewSweeper(store job.Store, q SweeperQueue, cfg SweeperConfig) *Sweeper {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	// Zero would make every queued job stale during a backlog.
	if cfg.BacklogRequeueAfter == 0 {
		cfg.BacklogRequeueAfter = DefaultBacklogRequeueAfter
	}
	return &Sweeper{store: store, queue: q, cfg: cfg, logger: log, metrics: cfg.Metrics}
}

// Run sweeps every Interval until ctx is cancelled.
func (s *Sweeper) Run(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()
	for {
		s.sweep(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

var errAbandoned = errors.New("abandoned: worker did not finish before its lease expired")

func (s *Sweeper) sweep(ctx context.Context) {
	// Expired lease: the worker is presumed dead. Counting it as an attempt
	// stops a job that crashes every worker from looping forever.
	expired, err := s.store.ExpiredLeases(ctx)
	if err != nil {
		s.logger.Error("finding expired leases",
			applog.AttrEvent, "lease.query_failed",
			applog.AttrError, err.Error())
	}
	for _, j := range expired {
		updated, err := recordFailure(ctx, s.store, s.cfg.Backoff, j, errAbandoned)
		if err != nil {
			s.logger.Error("recovering abandoned job",
				applog.AttrEvent, "job.recover_failed",
				applog.AttrJobID, j.ID,
				applog.AttrError, err.Error())
			continue
		}
		s.logger.Info("job abandoned",
			applog.AttrEvent, "job.abandoned",
			applog.AttrJobID, j.ID,
			"attempt", j.Attempts,
			"max_attempts", j.MaxAttempts,
			"status", string(updated.Status))
		recordAttemptOutcome(s.metrics, updated.Status)
	}

	due, err := s.store.PromoteDue(ctx)
	if err != nil {
		s.logger.Error("promoting due retries",
			applog.AttrEvent, "retry.promote_failed",
			applog.AttrError, err.Error())
	}
	s.enqueue(ctx, due)

	// Queued jobs whose ID was probably lost. During a backlog, old queued
	// jobs are usually just waiting, and re-pushing them flooded the queue
	// in a load test (docs/postmortems), so the short limit only applies
	// when the queue is empty.
	olderThan := s.cfg.RequeueAfter
	if n, err := s.queue.Len(ctx); err != nil || n > 0 {
		olderThan = s.cfg.BacklogRequeueAfter
	}
	stale, err := s.store.RequeueStale(ctx, olderThan)
	if err != nil {
		s.logger.Error("finding stale queued jobs",
			applog.AttrEvent, "stale.query_failed",
			applog.AttrError, err.Error())
	}
	for _, id := range stale {
		s.logger.Info("stale job requeued",
			applog.AttrEvent, "job.stale_requeued",
			applog.AttrJobID, id,
			"requeue_after", olderThan.String())
		s.metrics.JobRequeuedStale()
	}
	s.enqueue(ctx, stale)
}

// enqueue pushes IDs onto the queue. If a push fails, the job stays queued
// in Postgres, and RequeueStale picks it up again later.
func (s *Sweeper) enqueue(ctx context.Context, ids []string) {
	for _, id := range ids {
		if err := s.queue.Enqueue(ctx, id); err != nil {
			s.logger.Error("enqueueing job",
				applog.AttrEvent, "job.enqueue_failed",
				applog.AttrJobID, id,
				applog.AttrError, err.Error())
		}
	}
}
