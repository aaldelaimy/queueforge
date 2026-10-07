package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func ready(t *testing.T, c *Checker, draining bool) (int, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	ReadyHandler(c, func() bool { return draining }).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))
	var body map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return rec.Code, body
}

func TestReadyWhenAllChecksPass(t *testing.T) {
	c := NewChecker(time.Second).
		Add("postgres", func(context.Context) error { return nil }).
		Add("redis", func(context.Context) error { return nil })
	if code, body := ready(t, c, false); code != http.StatusOK || body["status"] != "ok" {
		t.Errorf("got %d %v", code, body)
	}
}

func TestNotReadyWhenADependencyFails(t *testing.T) {
	c := NewChecker(time.Second).
		Add("postgres", func(context.Context) error { return nil }).
		Add("redis", func(context.Context) error { return errors.New("connection refused") })
	code, body := ready(t, c, false)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d, want 503", code)
	}
	checks := body["checks"].(map[string]any)
	if checks["redis"] != "connection refused" || checks["postgres"] != "ok" {
		t.Errorf("checks: %v", checks)
	}
}

// A hung dependency must fail the probe within the timeout, not hang it.
func TestHungCheckTimesOut(t *testing.T) {
	c := NewChecker(20*time.Millisecond).Add("postgres", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	start := time.Now()
	code, _ := ready(t, c, false)
	if code != http.StatusServiceUnavailable || time.Since(start) > time.Second {
		t.Errorf("got %d after %v", code, time.Since(start))
	}
}

func TestNotReadyWhileDraining(t *testing.T) {
	code, body := ready(t, NewChecker(time.Second), true)
	if code != http.StatusServiceUnavailable || body["status"] != "draining" {
		t.Errorf("got %d %v", code, body)
	}
}

func TestLiveChecksNothing(t *testing.T) {
	rec := httptest.NewRecorder()
	Live(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("status %d", rec.Code)
	}
}
