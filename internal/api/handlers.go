package api

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/aaldelaimy/queueforge/internal/applog"
	"github.com/aaldelaimy/queueforge/internal/health"
	"github.com/aaldelaimy/queueforge/internal/job"
	"github.com/aaldelaimy/queueforge/internal/metrics"
	"github.com/aaldelaimy/queueforge/internal/tracing"
)

const (
	maxBodyBytes         = 1 << 20 // 1 MiB
	maxIdempotencyKeyLen = 256
	maxRequestIDLen      = 128
	headerRequestID      = "X-Request-ID"
)

// Enqueuer is where the API sends the IDs of newly created jobs.
type Enqueuer interface {
	Enqueue(ctx context.Context, id string) error
}

type contextKey struct{}

type Server struct {
	store    job.Store
	queue    Enqueuer
	jobTypes map[string]bool
	logger   *slog.Logger
	metrics  *metrics.Metrics
	ready    *health.Checker
	draining atomic.Bool
}

// NewServer returns a Server that records jobs in store and sends their IDs
// to queue. Only the listed job types are accepted.
func NewServer(store job.Store, queue Enqueuer, jobTypes []string) *Server {
	types := make(map[string]bool, len(jobTypes))
	for _, t := range jobTypes {
		types[t] = true
	}
	return &Server{store: store, queue: queue, jobTypes: types, logger: slog.Default()}
}

// WithLogger sets the logger for access and event lines.
func (s *Server) WithLogger(l *slog.Logger) *Server {
	if l != nil {
		s.logger = l
	}
	return s
}

// WithMetrics records counters and HTTP durations. nil is a no-op.
func (s *Server) WithMetrics(m *metrics.Metrics) *Server {
	s.metrics = m
	return s
}

// WithReadiness sets the dependency checks behind GET /ready.
func (s *Server) WithReadiness(c *health.Checker) *Server {
	s.ready = c
	return s
}

// Drain makes /ready fail so load balancers stop routing here. Call it
// before http.Server.Shutdown.
func (s *Server) Drain() {
	s.draining.Store(true)
}

// Routes returns the HTTP handler with every endpoint registered.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	if s.metrics != nil {
		mux.Handle("GET /metrics", s.metrics.Handler())
	}
	mux.HandleFunc("GET /health", health.Live)
	mux.Handle("GET /ready", health.ReadyHandler(s.ready, s.draining.Load))
	mux.HandleFunc("POST /jobs", s.createJob)
	mux.HandleFunc("GET /jobs", s.listJobs)
	mux.HandleFunc("GET /jobs/{id}", s.getJob)
	mux.HandleFunc("POST /jobs/{id}/retry", s.retryJob)
	return s.withRequestLog(mux)
}

type createJobRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type listJobsResponse struct {
	Jobs []job.Job `json:"jobs"`
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	log := loggerFrom(r)
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	var req createJobRequest
	if err := dec.Decode(&req); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if strings.TrimSpace(req.Type) == "" {
		writeError(w, http.StatusBadRequest, "type is required")
		return
	}
	if !s.jobTypes[req.Type] {
		writeError(w, http.StatusBadRequest, "unknown job type")
		return
	}

	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(key) > maxIdempotencyKeyLen {
		writeError(w, http.StatusBadRequest, "Idempotency-Key is too long")
		return
	}

	// Store before enqueueing, or a fast worker could pop an ID with no row.
	ctx, span := tracing.Tracer().Start(r.Context(), "store.create")
	j, created, err := s.store.Create(ctx, job.NewJob{
		Type:           req.Type,
		Payload:        req.Payload,
		IdempotencyKey: key,
		// The request span, not store.create, is the worker's parent.
		TraceParent: tracing.TraceParent(r.Context()),
	})
	endSpan(span, err)
	if errors.Is(err, job.ErrIdempotencyConflict) {
		log.Info("idempotency conflict",
			applog.AttrEvent, "job.conflict")
		writeError(w, http.StatusConflict, "Idempotency-Key reused with a different request")
		return
	}
	if err != nil {
		log.Error("creating job",
			applog.AttrEvent, "job.create_failed",
			applog.AttrError, err.Error())
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	log = log.With(applog.AttrJobID, j.ID)
	trace.SpanFromContext(r.Context()).SetAttributes(
		attribute.String("job.id", j.ID),
		attribute.String("job.type", j.Type),
		attribute.Bool("job.created", created),
	)

	// Replay: the job is already queued or done, so don't enqueue it again.
	if !created {
		log.Info("job replayed", applog.AttrEvent, "job.replayed")
		w.Header().Set("Location", "/jobs/"+j.ID)
		writeJSON(w, http.StatusAccepted, j)
		return
	}

	// Queue down: undo the insert and ask the client to retry.
	ctx, span = tracing.Tracer().Start(r.Context(), "queue.enqueue",
		trace.WithSpanKind(trace.SpanKindProducer))
	err = s.queue.Enqueue(ctx, j.ID)
	endSpan(span, err)
	if err != nil {
		log.Error("enqueue failed",
			applog.AttrEvent, "job.enqueue_failed",
			applog.AttrError, err.Error())
		// Must run even if the client has disconnected.
		if err := s.store.Delete(context.WithoutCancel(r.Context()), j.ID); err != nil {
			log.Error("job left queued but not enqueued",
				applog.AttrEvent, "job.stranded",
				applog.AttrError, err.Error())
		}
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "queue unavailable, try again later")
		return
	}

	s.metrics.JobSubmitted()
	log.Info("job created", applog.AttrEvent, "job.created")
	w.Header().Set("Location", "/jobs/"+j.ID)
	writeJSON(w, http.StatusAccepted, j)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	j, err := s.store.Get(r.Context(), r.PathValue("id"))
	if errors.Is(err, job.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		loggerFrom(r).Error("getting job",
			applog.AttrEvent, "job.get_failed",
			applog.AttrError, err.Error())
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	var status job.Status
	if raw := r.URL.Query().Get("status"); raw != "" {
		parsed, ok := job.ParseStatus(raw)
		if !ok {
			writeError(w, http.StatusBadRequest, "unknown status")
			return
		}
		status = parsed
	}
	jobs, err := s.store.List(r.Context(), status)
	if err != nil {
		loggerFrom(r).Error("listing jobs",
			applog.AttrEvent, "job.list_failed",
			applog.AttrError, err.Error())
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, listJobsResponse{Jobs: jobs})
}

// retryJob reopens a failed or dead-lettered job so a worker can try again.
func (s *Server) retryJob(w http.ResponseWriter, r *http.Request) {
	log := loggerFrom(r)
	j, err := s.store.Reopen(r.Context(), r.PathValue("id"))
	if errors.Is(err, job.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if errors.Is(err, job.ErrInvalidTransition) {
		writeError(w, http.StatusConflict, "job cannot be retried in its current state")
		return
	}
	if err != nil {
		log.Error("reopening job",
			applog.AttrEvent, "job.reopen_failed",
			applog.AttrError, err.Error())
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	log = log.With(applog.AttrJobID, j.ID)
	if err := s.queue.Enqueue(r.Context(), j.ID); err != nil {
		// The job is already queued in the store. The sweeper will push it.
		log.Warn("reopened job but enqueue failed",
			applog.AttrEvent, "job.retry_enqueue_failed",
			applog.AttrError, err.Error())
	}

	log.Info("job reopened", applog.AttrEvent, "job.reopened")
	w.Header().Set("Location", "/jobs/"+j.ID)
	writeJSON(w, http.StatusAccepted, j)
}

func (s *Server) withRequestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip scrapes and probes: frequent, and not user traffic.
		switch r.URL.Path {
		case "/metrics", "/health", "/ready":
			next.ServeHTTP(w, r)
			return
		}

		id := requestIDFrom(r)
		w.Header().Set(headerRequestID, id)

		// Continue the caller's trace if it sent a traceparent header.
		ctx := tracing.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := tracing.Tracer().Start(ctx, r.Method,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("http.request.method", r.Method),
				attribute.String("url.path", r.URL.Path),
				attribute.String("request_id", id),
			))
		defer span.End()

		log := s.logger.With(applog.AttrRequestID, id)
		if traceID := tracing.TraceID(ctx); traceID != "" {
			log = log.With(applog.AttrTraceID, traceID)
		}
		r = r.WithContext(context.WithValue(ctx, contextKey{}, log))

		rec := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(rec, r)
		d := time.Since(start)

		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		s.metrics.ObserveHTTP(r.Method, route, rec.status, d)
		span.SetName(route)
		span.SetAttributes(
			attribute.String("http.route", route),
			attribute.Int("http.response.status_code", rec.status))
		if rec.status >= 500 {
			span.SetStatus(codes.Error, http.StatusText(rec.status))
		}

		level := slog.LevelInfo
		if rec.status >= 500 {
			level = slog.LevelError
		}
		log.Log(r.Context(), level, "http request",
			applog.AttrEvent, "http.request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"duration_ms", d.Milliseconds(),
		)
	})
}

// endSpan marks span failed if err is non-nil, then ends it.
func endSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

func loggerFrom(r *http.Request) *slog.Logger {
	if l, ok := r.Context().Value(contextKey{}).(*slog.Logger); ok && l != nil {
		return l
	}
	return slog.Default()
}

func requestIDFrom(r *http.Request) string {
	id := strings.TrimSpace(r.Header.Get(headerRequestID))
	if id == "" || len(id) > maxRequestIDLen {
		return rand.Text()
	}
	return id
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// Headers are already sent; an encode error can't be reported.
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
