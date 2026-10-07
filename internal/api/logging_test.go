package api

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/aaldelaimy/queueforge/internal/applog"
	"github.com/aaldelaimy/queueforge/internal/job"
)

func TestMain(m *testing.M) {
	// Existing tests don't inject a logger; discard the default so every
	// request doesn't print a JSON access line to the test output.
	slog.SetDefault(slog.New(slog.DiscardHandler))
	os.Exit(m.Run())
}

func TestRequestIDIsGeneratedAndEchoed(t *testing.T) {
	h := NewServer(job.NewMemoryStore(), newQueue(), testJobTypes).Routes()
	rec := do(h, http.MethodGet, "/jobs", "")
	id := rec.Header().Get("X-Request-ID")
	if id == "" {
		t.Fatal("expected X-Request-ID on the response")
	}

	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	req.Header.Set("X-Request-ID", "client-id-123")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if got := rec.Header().Get("X-Request-ID"); got != "client-id-123" {
		t.Errorf("X-Request-ID: got %q, want client-id-123", got)
	}
}

func TestAccessLogIsStructuredJSON(t *testing.T) {
	var buf bytes.Buffer
	h := NewServer(job.NewMemoryStore(), newQueue(), testJobTypes).
		WithLogger(applog.New(&buf, "api")).
		Routes()

	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"type":"a"}`))
	req.Header.Set("X-Request-ID", "req-access")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status: got %d, want 202", rec.Code)
	}

	created := decode[job.Job](t, rec)
	access := mustFindEvent(t, parseLogs(t, &buf), "http.request")
	if access["service"] != "api" || access["request_id"] != "req-access" {
		t.Errorf("service/request_id: %v", access)
	}
	if access["method"] != "POST" || access["path"] != "/jobs" {
		t.Errorf("method/path: %v", access)
	}
	if access["status"] != float64(202) {
		t.Errorf("status: %v", access["status"])
	}
	if _, ok := access["duration_ms"]; !ok {
		t.Error("missing duration_ms")
	}
	if access["timestamp"] == nil || access["level"] == nil {
		t.Errorf("missing timestamp or level: %v", access)
	}

	createdLog := mustFindEvent(t, parseLogs(t, &buf), "job.created")
	if createdLog["job_id"] != created.ID || createdLog["request_id"] != "req-access" {
		t.Errorf("job.created: %v", createdLog)
	}
}

func TestReplayLogsEventWithoutReenqueue(t *testing.T) {
	var buf bytes.Buffer
	store, q := job.NewMemoryStore(), newQueue()
	h := NewServer(store, q, testJobTypes).WithLogger(applog.New(&buf, "api")).Routes()

	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"type":"a"}`))
	req.Header.Set("Idempotency-Key", "same")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	buf.Reset()
	req = httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"type":"a"}`))
	req.Header.Set("Idempotency-Key", "same")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status: got %d, want 202", rec.Code)
	}

	if _, ok := findEvent(parseLogs(t, &buf), "job.replayed"); !ok {
		t.Fatalf("missing job.replayed in %s", buf.String())
	}
	if _, ok := findEvent(parseLogs(t, &buf), "job.created"); ok {
		t.Error("replay must not log job.created")
	}
}

func TestStoreErrorLogIncludesErrorField(t *testing.T) {
	var buf bytes.Buffer
	h := NewServer(brokenStore{}, newQueue(), testJobTypes).
		WithLogger(applog.New(&buf, "api")).
		Routes()

	rec := do(h, http.MethodPost, "/jobs", `{"type":"a"}`)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status: got %d, want 500", rec.Code)
	}

	line := mustFindEvent(t, parseLogs(t, &buf), "job.create_failed")
	if line["error"] != errDatabaseDown.Error() {
		t.Errorf("error field: %v", line["error"])
	}
	if line["level"] != "ERROR" {
		t.Errorf("level: %v", line["level"])
	}
}

func parseLogs(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	dec := json.NewDecoder(bytes.NewReader(buf.Bytes()))
	for {
		var m map[string]any
		if err := dec.Decode(&m); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("decoding log %q: %v", buf.String(), err)
		}
		out = append(out, m)
	}
	return out
}

func findEvent(logs []map[string]any, event string) (map[string]any, bool) {
	for _, line := range logs {
		if line["event"] == event {
			return line, true
		}
	}
	return nil, false
}

func mustFindEvent(t *testing.T, logs []map[string]any, event string) map[string]any {
	t.Helper()
	line, ok := findEvent(logs, event)
	if !ok {
		t.Fatalf("missing event %s in %v", event, logs)
	}
	return line
}
