package api

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/aaldelaimy/queueforge/internal/health"
	"github.com/aaldelaimy/queueforge/internal/job"
)

func TestHealthIsAlwaysOK(t *testing.T) {
	if rec := do(newHandler(), http.MethodGet, "/health", ""); rec.Code != http.StatusOK {
		t.Errorf("status %d", rec.Code)
	}
}

func TestReadyReflectsDependencies(t *testing.T) {
	redisUp := true
	checker := health.NewChecker(time.Second).Add("redis", func(context.Context) error {
		if !redisUp {
			return errors.New("connection refused")
		}
		return nil
	})
	h := NewServer(job.NewMemoryStore(), newQueue(), testJobTypes).WithReadiness(checker).Routes()

	if rec := do(h, http.MethodGet, "/ready", ""); rec.Code != http.StatusOK {
		t.Errorf("ready with redis up: %d", rec.Code)
	}
	redisUp = false
	if rec := do(h, http.MethodGet, "/ready", ""); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("ready with redis down: %d", rec.Code)
	}
	// Liveness must not follow dependencies: restarting the API won't fix Redis.
	if rec := do(h, http.MethodGet, "/health", ""); rec.Code != http.StatusOK {
		t.Errorf("health with redis down: %d", rec.Code)
	}
}

// After Drain, /ready fails so the load balancer stops routing here, but
// requests that still arrive are served normally.
func TestDrainFailsReadinessButStillServes(t *testing.T) {
	s := NewServer(job.NewMemoryStore(), newQueue(), testJobTypes)
	h := s.Routes()
	s.Drain()

	if rec := do(h, http.MethodGet, "/ready", ""); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("ready while draining: %d", rec.Code)
	}
	if rec := do(h, http.MethodPost, "/jobs", `{"type":"a"}`); rec.Code != http.StatusAccepted {
		t.Errorf("POST while draining: %d", rec.Code)
	}
}
