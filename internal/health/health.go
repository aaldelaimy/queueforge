// Package health serves the /health (liveness) and /ready (readiness) probes.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// Check reports whether one dependency is usable.
type Check func(ctx context.Context) error

// Checker runs named dependency checks with a shared timeout.
type Checker struct {
	checks  map[string]Check
	timeout time.Duration
}

// NewChecker returns a Checker whose checks share one timeout per round.
func NewChecker(timeout time.Duration) *Checker {
	return &Checker{checks: map[string]Check{}, timeout: timeout}
}

// Add registers a dependency check under name.
func (c *Checker) Add(name string, check Check) *Checker {
	c.checks[name] = check
	return c
}

// Run executes every check and returns "ok" or the error text for each.
func (c *Checker) Run(ctx context.Context) (results map[string]string, ok bool) {
	results = make(map[string]string, len(c.checks))
	ok = true
	if c == nil {
		return results, ok
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	for name, check := range c.checks {
		if err := check(ctx); err != nil {
			results[name] = err.Error()
			ok = false
			continue
		}
		results[name] = "ok"
	}
	return results, ok
}

// Live always answers 200. It checks no dependencies on purpose: restarting
// the process can't fix a database outage.
func Live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ReadyHandler answers 200 when every check passes and the process is not
// draining, else 503 with the failing checks.
func ReadyHandler(c *Checker, draining func() bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if draining != nil && draining() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "draining"})
			return
		}
		results, ok := c.Run(r.Context())
		status, code := "ok", http.StatusOK
		if !ok {
			status, code = "unavailable", http.StatusServiceUnavailable
		}
		writeJSON(w, code, map[string]any{"status": status, "checks": results})
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
