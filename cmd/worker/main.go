// Command worker takes job IDs off the Redis queue and runs the jobs,
// recording outcomes in Postgres. Run as many copies as needed.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/aaldelaimy/queueforge/internal/applog"
	"github.com/aaldelaimy/queueforge/internal/db"
	"github.com/aaldelaimy/queueforge/internal/health"
	"github.com/aaldelaimy/queueforge/internal/job"
	"github.com/aaldelaimy/queueforge/internal/metrics"
	"github.com/aaldelaimy/queueforge/internal/queue"
	"github.com/aaldelaimy/queueforge/internal/tracing"
	"github.com/aaldelaimy/queueforge/internal/worker"
)

func main() {
	concurrency := flag.Int("concurrency", 4, "jobs this process runs at the same time")
	jobTimeout := flag.Duration("job-timeout", worker.DefaultJobTimeout, "how long one attempt may run before it is cancelled")
	sweeperInterval := flag.Duration("sweeper-interval", worker.DefaultSweeperInterval, "how often to look for abandoned or lost jobs")
	requeueAfter := flag.Duration("requeue-after", worker.DefaultRequeueAfter, "how long a job may sit queued, while the queue is empty, before its ID is pushed again")
	backlogRequeueAfter := flag.Duration("backlog-requeue-after", worker.DefaultBacklogRequeueAfter, "the same limit while the queue has a backlog")
	shutdownGrace := flag.Duration("shutdown-grace", 20*time.Second, "after SIGTERM, how long running jobs get to finish before they are cancelled; keep it below the orchestrator's kill timeout")
	metricsAddr := flag.String("metrics-addr", ":9091", "address for /metrics, /health and /ready")
	flag.Parse()

	log := applog.New(os.Stdout, "worker")

	// stopCtx is cancelled on SIGTERM: stop taking new jobs.
	stopCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// jobsCtx is the parent of every job attempt. It is cancelled only after
	// the grace period, so running jobs get a chance to finish.
	jobsCtx, cancelJobs := context.WithCancel(context.Background())
	defer cancelJobs()

	shutdownTracing, err := tracing.Setup(stopCtx, "queueforge-worker")
	if err != nil {
		fatal(log, "setting up tracing", err)
	}

	pool, err := db.Connect(stopCtx, requireEnv(log, "DATABASE_URL"))
	if err != nil {
		fatal(log, "connecting to database", err)
	}
	defer pool.Close()
	if err := db.Migrate(stopCtx, pool); err != nil {
		fatal(log, "migrating database", err)
	}

	q, err := queue.NewRedis(stopCtx, requireEnv(log, "REDIS_URL"), queue.Key)
	if err != nil {
		fatal(log, "connecting to redis", err)
	}
	defer q.Close()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := metrics.New(reg)

	ready := health.NewChecker(time.Second).Add("postgres", pool.Ping).Add("redis", q.Ping)
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", m.Handler())
	mux.HandleFunc("GET /health", health.Live)
	mux.Handle("GET /ready", health.ReadyHandler(ready, func() bool { return stopCtx.Err() != nil }))
	go func() {
		srv := &http.Server{Addr: *metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fatal(log, "metrics server", err)
		}
	}()

	store := job.NewPostgresStore(pool)
	handlers := worker.DefaultHandlers()
	cfg := worker.Config{JobTimeout: *jobTimeout, Backoff: worker.DefaultBackoff(), Logger: log, Metrics: m}

	var wg sync.WaitGroup
	wg.Go(func() {
		worker.NewSweeper(store, q, worker.SweeperConfig{
			Interval:            *sweeperInterval,
			RequeueAfter:        *requeueAfter,
			BacklogRequeueAfter: *backlogRequeueAfter,
			Backoff:             cfg.Backoff,
			Logger:              log,
			Metrics:             m,
		}).Run(stopCtx)
	})

	log.Info("started",
		applog.AttrEvent, "worker.started",
		"concurrency", *concurrency,
		"job_timeout", jobTimeout.String(),
		"metrics_addr", *metricsAddr,
		"pid", os.Getpid())
	for range *concurrency {
		wg.Go(func() { worker.New(store, q, handlers, cfg).RunUntil(stopCtx, jobsCtx) })
	}

	<-stopCtx.Done()
	log.Info("shutting down", applog.AttrEvent, "worker.draining", "shutdown_grace", shutdownGrace.String())

	// After the grace period, cancel whatever is still running. Those
	// attempts are recorded as transient failures and retried.
	grace := time.AfterFunc(*shutdownGrace, func() {
		log.Warn("grace period over, cancelling running jobs", applog.AttrEvent, "worker.grace_expired")
		cancelJobs()
	})
	wg.Wait()
	grace.Stop()

	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := shutdownTracing(flushCtx); err != nil {
		log.Error("flushing traces", applog.AttrEvent, "tracing.flush_failed", applog.AttrError, err.Error())
	}
	log.Info("stopped", applog.AttrEvent, "worker.stopped")
}

func requireEnv(log *slog.Logger, name string) string {
	v := os.Getenv(name)
	if v == "" {
		log.Error(name+" is required", applog.AttrEvent, "startup.failed")
		os.Exit(1)
	}
	return v
}

func fatal(log *slog.Logger, msg string, err error) {
	log.Error(msg, applog.AttrEvent, "startup.failed", applog.AttrError, err.Error())
	os.Exit(1)
}
