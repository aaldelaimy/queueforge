package worker

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/aaldelaimy/queueforge/internal/applog"
	"github.com/aaldelaimy/queueforge/internal/job"
)

func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.DiscardHandler))
	os.Exit(m.Run())
}

func TestWorkerLogsCompletedJobID(t *testing.T) {
	var buf bytes.Buffer
	store := job.NewMemoryStore()
	cfg := testConfig
	cfg.Logger = applog.New(&buf, "worker")
	w := New(store, nil, DefaultHandlers(), cfg)

	j := mustCreate(t, store, "hash", json.RawMessage(`{"input":"hello"}`))
	w.process(t.Context(), j.ID)

	line := mustFindEvent(t, parseLogs(t, &buf), "job.completed")
	if line["service"] != "worker" || line["job_id"] != j.ID {
		t.Errorf("got %v", line)
	}
	if line["timestamp"] == nil || line["level"] != "INFO" {
		t.Errorf("missing timestamp/level: %v", line)
	}
	if line["attempt"] != float64(1) {
		t.Errorf("attempt: %v", line["attempt"])
	}
}

func TestWorkerLogsFailureWithError(t *testing.T) {
	var buf bytes.Buffer
	store := job.NewMemoryStore()
	cfg := testConfig
	cfg.Logger = applog.New(&buf, "worker")
	w := New(store, nil, DefaultHandlers(), cfg)

	j := mustCreate(t, store, "hash", json.RawMessage(`{}`)) // missing input: permanent fail
	w.process(t.Context(), j.ID)

	line := mustFindEvent(t, parseLogs(t, &buf), "job.failed")
	if line["job_id"] != j.ID {
		t.Errorf("job_id: %v", line["job_id"])
	}
	if line["error"] == nil || line["error"] == "" {
		t.Errorf("missing error: %v", line)
	}
	if line["status"] != string(job.StatusFailed) {
		t.Errorf("status: %v", line["status"])
	}
}

func TestSweeperLogsAbandonedJobID(t *testing.T) {
	var buf bytes.Buffer
	store, q := job.NewMemoryStore(), newQueue()
	s := NewSweeper(store, q, SweeperConfig{
		Interval:     time.Hour,
		RequeueAfter: time.Hour,
		Backoff:      Backoff{},
		Logger:       applog.New(&buf, "worker"),
	})
	j := mustCreate(t, store, "hash", json.RawMessage(`{"input":"x"}`))
	claimAndDie(t, store, j.ID)

	s.sweep(t.Context())

	line := mustFindEvent(t, parseLogs(t, &buf), "job.abandoned")
	if line["job_id"] != j.ID || line["service"] != "worker" {
		t.Errorf("got %v", line)
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

func mustFindEvent(t *testing.T, logs []map[string]any, event string) map[string]any {
	t.Helper()
	for _, line := range logs {
		if line["event"] == event {
			return line
		}
	}
	t.Fatalf("missing event %s in %v", event, logs)
	return nil
}
