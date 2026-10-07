package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aaldelaimy/queueforge/internal/job"
	"github.com/aaldelaimy/queueforge/internal/queue"
)

// do sends a request through the real router, so routing, methods, and
// path parameters are tested too, not just the handler functions.
func do(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

var testJobTypes = []string{"a", "b", "image-processing"}

func newQueue() *queue.Memory { return queue.NewMemory(10, 10*time.Millisecond) }

func newHandler() http.Handler {
	return NewServer(job.NewMemoryStore(), newQueue(), testJobTypes).Routes()
}

// brokenQueue fails every Enqueue, standing in for Redis being down.
type brokenQueue struct{}

func (brokenQueue) Enqueue(context.Context, string) error { return errors.New("connection refused") }

// brokenStore fails every call, standing in for a database that is down.
type brokenStore struct{ job.Store }

var errDatabaseDown = errors.New("database is down")

func (brokenStore) Create(context.Context, job.NewJob) (job.Job, bool, error) {
	return job.Job{}, false, errDatabaseDown
}
func (brokenStore) Get(context.Context, string) (job.Job, error) { return job.Job{}, errDatabaseDown }
func (brokenStore) List(context.Context, job.Status) ([]job.Job, error) {
	return nil, errDatabaseDown
}

func TestStoreErrorsReturn500(t *testing.T) {
	h := NewServer(brokenStore{}, newQueue(), testJobTypes).Routes()
	cases := map[string]struct{ method, path, body string }{
		"create": {http.MethodPost, "/jobs", `{"type":"a"}`},
		"get":    {http.MethodGet, "/jobs/some-id", ""},
		"list":   {http.MethodGet, "/jobs", ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rec := do(h, tc.method, tc.path, tc.body)
			if rec.Code != http.StatusInternalServerError {
				t.Fatalf("status: got %d, want 500", rec.Code)
			}
			// The client must not see internal error details.
			if body := decode[map[string]string](t, rec); body["error"] != "internal error" {
				t.Errorf("got %v", body)
			}
		})
	}
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.NewDecoder(rec.Body).Decode(&v); err != nil {
		t.Fatalf("decoding response %q: %v", rec.Body.String(), err)
	}
	return v
}

func TestCreateJob(t *testing.T) {
	h := newHandler()

	rec := do(h, http.MethodPost, "/jobs", `{"type":"image-processing","payload":{"input":"example"}}`)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status: got %d, want 202; body %s", rec.Code, rec.Body)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type: got %q", ct)
	}
	j := decode[job.Job](t, rec)
	if j.ID == "" || j.Status != job.StatusQueued || j.Attempts != 0 {
		t.Errorf("unexpected job: %+v", j)
	}
	if string(j.Payload) != `{"input":"example"}` {
		t.Errorf("payload: got %s", j.Payload)
	}
	if loc := rec.Header().Get("Location"); loc != "/jobs/"+j.ID {
		t.Errorf("Location: got %q", loc)
	}
}

func TestCreateJobRejectsBadInput(t *testing.T) {
	cases := map[string]struct {
		body string
		want int
	}{
		"empty body":    {``, http.StatusBadRequest},
		"malformed":     {`{"type":`, http.StatusBadRequest},
		"missing type":  {`{"payload":{}}`, http.StatusBadRequest},
		"blank type":    {`{"type":"   "}`, http.StatusBadRequest},
		"wrong type":    {`{"type":123}`, http.StatusBadRequest},
		"unknown field": {`{"type":"a","priority":1}`, http.StatusBadRequest},
		"too large":     {`{"type":"a","payload":"` + strings.Repeat("x", maxBodyBytes) + `"}`, http.StatusRequestEntityTooLarge},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rec := do(newHandler(), http.MethodPost, "/jobs", tc.body)
			if rec.Code != tc.want {
				t.Fatalf("status: got %d, want %d", rec.Code, tc.want)
			}
			if body := decode[map[string]string](t, rec); body["error"] == "" {
				t.Errorf("expected an error message, got %v", body)
			}
		})
	}
}

func TestCreateJobEnqueuesID(t *testing.T) {
	q := newQueue()
	h := NewServer(job.NewMemoryStore(), q, testJobTypes).Routes()

	created := decode[job.Job](t, do(h, http.MethodPost, "/jobs", `{"type":"a"}`))

	id, err := q.Dequeue(t.Context())
	if err != nil {
		t.Fatalf("nothing was enqueued: %v", err)
	}
	if id != created.ID {
		t.Errorf("queued %q, want %q", id, created.ID)
	}
}

func TestCreateJobUnknownType(t *testing.T) {
	rec := do(newHandler(), http.MethodPost, "/jobs", `{"type":"not-a-real-type"}`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", rec.Code)
	}
	if body := decode[map[string]string](t, rec); body["error"] != "unknown job type" {
		t.Errorf("got %v", body)
	}
}

func TestCreateJobQueueUnavailable(t *testing.T) {
	store := job.NewMemoryStore()
	h := NewServer(store, brokenQueue{}, testJobTypes).Routes()

	rec := do(h, http.MethodPost, "/jobs", `{"type":"a"}`)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d, want 503", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("expected a Retry-After header")
	}
	if jobs, _ := store.List(t.Context(), ""); len(jobs) != 0 {
		t.Errorf("store has %d jobs, want 0: the rejected job must not be left behind", len(jobs))
	}
}

func TestGetJob(t *testing.T) {
	h := newHandler()
	created := decode[job.Job](t, do(h, http.MethodPost, "/jobs", `{"type":"a"}`))

	rec := do(h, http.MethodGet, "/jobs/"+created.ID, "")

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", rec.Code)
	}
	if got := decode[job.Job](t, rec); got.ID != created.ID || got.Status != job.StatusQueued {
		t.Errorf("got %+v", got)
	}
}

func TestGetUnknownJobReturns404(t *testing.T) {
	rec := do(newHandler(), http.MethodGet, "/jobs/nope", "")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status: got %d, want 404", rec.Code)
	}
	if body := decode[map[string]string](t, rec); body["error"] != "job not found" {
		t.Errorf("got %v", body)
	}
}

func TestListJobs(t *testing.T) {
	h := newHandler()

	rec := do(h, http.MethodGet, "/jobs", "")
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != `{"jobs":[]}` {
		t.Fatalf("empty list: got %d %s", rec.Code, rec.Body)
	}

	do(h, http.MethodPost, "/jobs", `{"type":"a"}`)
	do(h, http.MethodPost, "/jobs", `{"type":"b"}`)

	got := decode[listJobsResponse](t, do(h, http.MethodGet, "/jobs", ""))
	if len(got.Jobs) != 2 {
		t.Errorf("got %d jobs, want 2", len(got.Jobs))
	}
}

func TestListJobsFilterByStatus(t *testing.T) {
	h := newHandler()
	do(h, http.MethodPost, "/jobs", `{"type":"a"}`)

	got := decode[listJobsResponse](t, do(h, http.MethodGet, "/jobs?status=queued", ""))
	if len(got.Jobs) != 1 {
		t.Fatalf("queued: got %d", len(got.Jobs))
	}
	if got := decode[listJobsResponse](t, do(h, http.MethodGet, "/jobs?status=completed", "")); len(got.Jobs) != 0 {
		t.Errorf("completed: got %d", len(got.Jobs))
	}
	if rec := do(h, http.MethodGet, "/jobs?status=bogus", ""); rec.Code != http.StatusBadRequest {
		t.Errorf("bogus status: got %d, want 400", rec.Code)
	}
}

func TestRetryDeadLetteredJob(t *testing.T) {
	store := job.NewMemoryStore()
	q := newQueue()
	h := NewServer(store, q, testJobTypes).Routes()
	created := decode[job.Job](t, do(h, http.MethodPost, "/jobs", `{"type":"a"}`))
	q.Dequeue(t.Context()) // drop the original enqueue
	store.Claim(t.Context(), created.ID, time.Hour)
	store.DeadLetter(t.Context(), created.ID, "gave up")

	rec := do(h, http.MethodPost, "/jobs/"+created.ID+"/retry", "")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status: got %d, want 202; %s", rec.Code, rec.Body)
	}
	got := decode[job.Job](t, rec)
	if got.Status != job.StatusQueued || got.Attempts != 0 {
		t.Errorf("got status %s attempts %d", got.Status, got.Attempts)
	}
	if id, err := q.Dequeue(t.Context()); err != nil || id != created.ID {
		t.Errorf("retry did not enqueue: %q, %v", id, err)
	}
}

func TestRetryRejectsQueuedJob(t *testing.T) {
	h := newHandler()
	created := decode[job.Job](t, do(h, http.MethodPost, "/jobs", `{"type":"a"}`))

	rec := do(h, http.MethodPost, "/jobs/"+created.ID+"/retry", "")
	if rec.Code != http.StatusConflict {
		t.Fatalf("status: got %d, want 409", rec.Code)
	}
}

func TestRetryUnknownJob(t *testing.T) {
	rec := do(newHandler(), http.MethodPost, "/jobs/nope/retry", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status: got %d, want 404", rec.Code)
	}
}

func post(h http.Handler, body, idempotencyKey string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body))
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestCreateJobIsIdempotent(t *testing.T) {
	q := newQueue()
	h := NewServer(job.NewMemoryStore(), q, testJobTypes).Routes()
	body := `{"type":"a","payload":{"n":1}}`

	first := decode[job.Job](t, post(h, body, "req-1"))
	second := decode[job.Job](t, post(h, body, "req-1"))

	if first.ID != second.ID {
		t.Errorf("replay created %s, want %s", second.ID, first.ID)
	}
	id, err := q.Dequeue(t.Context())
	if err != nil || id != first.ID {
		t.Fatalf("first enqueue: got %q, %v", id, err)
	}
	if id, err := q.Dequeue(t.Context()); err == nil {
		t.Errorf("replay enqueued %s; a duplicate must not go on the queue", id)
	}
}

func TestCreateJobIdempotencyConflict(t *testing.T) {
	h := newHandler()
	post(h, `{"type":"a","payload":{"n":1}}`, "req-1")

	rec := post(h, `{"type":"a","payload":{"n":2}}`, "req-1")
	if rec.Code != http.StatusConflict {
		t.Fatalf("status: got %d, want 409", rec.Code)
	}
}

func TestCreateJobIdempotencyKeyTooLong(t *testing.T) {
	rec := post(newHandler(), `{"type":"a"}`, strings.Repeat("k", maxIdempotencyKeyLen+1))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", rec.Code)
	}
}

func TestConcurrentCreateJobSameKey(t *testing.T) {
	store := job.NewMemoryStore()
	q := newQueue()
	h := NewServer(store, q, testJobTypes).Routes()

	start := make(chan struct{})
	var wg sync.WaitGroup
	ids := make([]string, 20)
	for i := range ids {
		wg.Go(func() {
			<-start
			rec := post(h, `{"type":"a"}`, "shared")
			if rec.Code != http.StatusAccepted {
				t.Errorf("status %d", rec.Code)
				return
			}
			ids[i] = decode[job.Job](t, rec).ID
		})
	}
	close(start)
	wg.Wait()

	for _, id := range ids {
		if id == "" || id != ids[0] {
			t.Fatalf("got IDs %v", ids)
		}
	}
	if jobs, _ := store.List(t.Context(), ""); len(jobs) != 1 {
		t.Errorf("store has %d jobs, want 1", len(jobs))
	}
}

func TestCreateJobWithoutKeyCreatesTwoJobs(t *testing.T) {
	h := newHandler()
	a := decode[job.Job](t, do(h, http.MethodPost, "/jobs", `{"type":"a"}`))
	b := decode[job.Job](t, do(h, http.MethodPost, "/jobs", `{"type":"a"}`))
	if a.ID == b.ID {
		t.Error("two POSTs without a key produced the same job")
	}
}

func TestWrongMethodReturns405(t *testing.T) {
	rec := do(newHandler(), http.MethodDelete, "/jobs", "")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status: got %d, want 405", rec.Code)
	}
}
