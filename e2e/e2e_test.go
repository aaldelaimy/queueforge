//go:build e2e

// Package e2e drives a running QueueForge stack over HTTP, the way a client
// would: real API, real workers, real Postgres and Redis.
//
//	docker compose up -d --build
//	go test -tags e2e ./e2e/ -v
//
// QUEUEFORGE_API_URL overrides the default http://localhost:8080.
package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func baseURL() string {
	if u := os.Getenv("QUEUEFORGE_API_URL"); u != "" {
		return strings.TrimRight(u, "/")
	}
	return "http://localhost:8080"
}

type jobView struct {
	ID          string          `json:"job_id"`
	Type        string          `json:"type"`
	Status      string          `json:"status"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	Result      json.RawMessage `json:"result"`
	Error       string          `json:"error"`
}

func request(t *testing.T, method, path, body string, header map[string]string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, baseURL()+path, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v (is the stack running? docker compose up -d)", method, path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func submit(t *testing.T, body string, header map[string]string) jobView {
	t.Helper()
	code, b := request(t, http.MethodPost, "/jobs", body, header)
	if code != http.StatusAccepted {
		t.Fatalf("POST /jobs: %d %s", code, b)
	}
	var j jobView
	if err := json.Unmarshal(b, &j); err != nil {
		t.Fatal(err)
	}
	if j.Status != "queued" {
		t.Fatalf("new job status %s, want queued", j.Status)
	}
	return j
}

func get(t *testing.T, id string) jobView {
	t.Helper()
	code, b := request(t, http.MethodGet, "/jobs/"+id, "", nil)
	if code != http.StatusOK {
		t.Fatalf("GET /jobs/%s: %d %s", id, code, b)
	}
	var j jobView
	if err := json.Unmarshal(b, &j); err != nil {
		t.Fatal(err)
	}
	return j
}

// waitFor polls until the job reaches want, failing if it reaches another
// terminal status or time runs out.
func waitFor(t *testing.T, id, want string, within time.Duration) jobView {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		j := get(t, id)
		if j.Status == want {
			return j
		}
		terminal := j.Status == "completed" || j.Status == "failed" || j.Status == "dead_lettered"
		if terminal || time.Now().After(deadline) {
			t.Fatalf("job %s: status %s (attempts %d, error %q), want %s", id, j.Status, j.Attempts, j.Error, want)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func uniqueKey(t *testing.T) string {
	return fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
}

func TestHealthAndReady(t *testing.T) {
	if code, b := request(t, http.MethodGet, "/health", "", nil); code != http.StatusOK {
		t.Errorf("/health: %d %s", code, b)
	}
	if code, b := request(t, http.MethodGet, "/ready", "", nil); code != http.StatusOK {
		t.Errorf("/ready: %d %s", code, b)
	}
}

func TestHashJobCompletes(t *testing.T) {
	j := submit(t, `{"type":"hash","payload":{"input":"hello"}}`, nil)
	done := waitFor(t, j.ID, "completed", 10*time.Second)
	const want = `{"sha256":"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"}`
	if string(done.Result) != want {
		t.Errorf("result: got %s, want %s", done.Result, want)
	}
	if done.Attempts != 1 {
		t.Errorf("attempts: got %d, want 1", done.Attempts)
	}
}

// The job fails twice with a transient error and succeeds on attempt 3,
// after two backoff delays (about 0.5-1s, then 1-2s).
func TestTransientFailureIsRetried(t *testing.T) {
	j := submit(t, `{"type":"flaky","payload":{"fail_times":2}}`, nil)
	done := waitFor(t, j.ID, "completed", 20*time.Second)
	if done.Attempts != 3 {
		t.Errorf("attempts: got %d, want 3", done.Attempts)
	}
	if string(done.Result) != `{"succeeded_on_attempt":3}` {
		t.Errorf("result: %s", done.Result)
	}
}

// A malformed payload can never succeed, so it fails on the first attempt.
func TestPermanentFailureIsNotRetried(t *testing.T) {
	j := submit(t, `{"type":"hash","payload":{"wrong":"field"}}`, nil)
	done := waitFor(t, j.ID, "failed", 10*time.Second)
	if done.Attempts != 1 {
		t.Errorf("attempts: got %d, want 1", done.Attempts)
	}
	if done.Error == "" {
		t.Error("failed job has no error")
	}
}

// Always-failing transient job: three attempts, then dead_lettered. A
// manual retry reopens it with a fresh budget.
func TestDeadLetterAndManualRetry(t *testing.T) {
	j := submit(t, `{"type":"fail"}`, nil)
	dead := waitFor(t, j.ID, "dead_lettered", 30*time.Second)
	if dead.Attempts != dead.MaxAttempts {
		t.Errorf("attempts: got %d, want %d", dead.Attempts, dead.MaxAttempts)
	}

	code, b := request(t, http.MethodGet, "/jobs?status=dead_lettered", "", nil)
	if code != http.StatusOK || !strings.Contains(string(b), j.ID) {
		t.Errorf("dead-letter listing does not include the job: %d %s", code, b)
	}

	code, b = request(t, http.MethodPost, "/jobs/"+j.ID+"/retry", "", nil)
	if code != http.StatusAccepted {
		t.Fatalf("retry: %d %s", code, b)
	}
	waitFor(t, j.ID, "dead_lettered", 30*time.Second)
}

func TestIdempotencyKeyReturnsSameJob(t *testing.T) {
	key := map[string]string{"Idempotency-Key": uniqueKey(t)}
	body := `{"type":"hash","payload":{"input":"once"}}`
	first := submit(t, body, key)

	// The replay returns the existing job as it is now, which may already be
	// running or completed, so only the ID is compared.
	code, b := request(t, http.MethodPost, "/jobs", body, key)
	if code != http.StatusAccepted {
		t.Fatalf("replay: %d %s", code, b)
	}
	var second jobView
	if err := json.Unmarshal(b, &second); err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID {
		t.Fatalf("same key gave two jobs: %s and %s", first.ID, second.ID)
	}
	if done := waitFor(t, first.ID, "completed", 10*time.Second); done.Attempts != 1 {
		t.Errorf("attempts: got %d, want 1 (the replay must not run it again)", done.Attempts)
	}

	code, _ = request(t, http.MethodPost, "/jobs", `{"type":"hash","payload":{"input":"different"}}`, key)
	if code != http.StatusConflict {
		t.Errorf("same key, different body: got %d, want 409", code)
	}
}

func TestBadRequests(t *testing.T) {
	for _, tc := range []struct{ body string }{
		{`not json`},
		{`{"type":"no-such-type"}`},
		{`{"payload":{}}`},
	} {
		if code, _ := request(t, http.MethodPost, "/jobs", tc.body, nil); code != http.StatusBadRequest {
			t.Errorf("%s: got %d, want 400", tc.body, code)
		}
	}
	if code, _ := request(t, http.MethodGet, "/jobs/does-not-exist", "", nil); code != http.StatusNotFound {
		t.Errorf("unknown job: got %d, want 404", code)
	}
}

func TestMetricsAreExposed(t *testing.T) {
	submit(t, `{"type":"hash","payload":{"input":"metrics"}}`, nil)
	code, b := request(t, http.MethodGet, "/metrics", "", nil)
	if code != http.StatusOK {
		t.Fatalf("/metrics: %d", code)
	}
	for _, name := range []string{"queueforge_jobs_submitted_total", "queueforge_queue_depth", "queueforge_http_request_duration_seconds"} {
		if !strings.Contains(string(b), name) {
			t.Errorf("missing %s", name)
		}
	}
}
