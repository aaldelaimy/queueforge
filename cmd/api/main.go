// Command api serves the HTTP API: it records jobs in Postgres and pushes
// their IDs onto the Redis queue. It runs no jobs itself; see cmd/worker.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/aaldelaimy/queueforge/internal/api"
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
	addr := flag.String("addr", ":8080", "HTTP listen address")
	drainDelay := flag.Duration("drain-delay", 0, "after SIGTERM, how long /ready fails before the server stops accepting connections; set it longer than the load balancer's health-check interval")
	shutdownTimeout := flag.Duration("shutdown-timeout", 10*time.Second, "how long in-flight requests get to finish on shutdown")
	flag.Parse()

	log := applog.New(os.Stdout, "api")

	// Cancelled on SIGTERM or Ctrl-C.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := tracing.Setup(ctx, "queueforge-api")
	if err != nil {
		fatal(log, "setting up tracing", err)
	}

	pool, err := db.Connect(ctx, requireEnv(log, "DATABASE_URL"))
	if err != nil {
		fatal(log, "connecting to database", err)
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool); err != nil {
		fatal(log, "migrating database", err)
	}

	q, err := queue.NewRedis(ctx, requireEnv(log, "REDIS_URL"), queue.Key)
	if err != nil {
		fatal(log, "connecting to redis", err)
	}
	defer q.Close()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := metrics.New(reg)
	// Only the API exports queue depth; see metrics.PollQueueDepth.
	go metrics.PollQueueDepth(ctx, m, q.Len, time.Second)

	ready := health.NewChecker(time.Second).Add("postgres", pool.Ping).Add("redis", q.Ping)
	jobTypes := slices.Sorted(maps.Keys(worker.DefaultHandlers()))
	server := api.NewServer(job.NewPostgresStore(pool), q, jobTypes).
		WithLogger(log).
		WithMetrics(m).
		WithReadiness(ready)
	srv := &http.Server{
		Addr:              *addr,
		Handler:           server.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.ListenAndServe() }()
	log.Info("listening", applog.AttrEvent, "api.listening", "addr", *addr)

	select {
	case err := <-serveErr:
		fatal(log, "http server", err)
	case <-ctx.Done():
	}

	// Fail /ready first so the load balancer stops routing here, then let
	// in-flight requests finish.
	log.Info("shutting down", applog.AttrEvent, "api.draining", "drain_delay", drainDelay.String())
	server.Drain()
	time.Sleep(*drainDelay)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("http shutdown", applog.AttrEvent, "api.shutdown_failed", applog.AttrError, err.Error())
	}
	if err := shutdownTracing(shutdownCtx); err != nil {
		log.Error("flushing traces", applog.AttrEvent, "tracing.flush_failed", applog.AttrError, err.Error())
	}
	log.Info("stopped", applog.AttrEvent, "api.stopped")
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
