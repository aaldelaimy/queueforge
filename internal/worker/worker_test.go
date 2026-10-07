package worker

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aaldelaimy/queueforge/internal/job"
	"github.com/aaldelaimy/queueforge/internal/queue"
)

// Tiny delays keep retry tests fast; the logic is the same at any scale.
var testConfig = Config{
	JobTimeout: time.Second,
	Backoff:    Backoff{Base: time.Millisecond, Max: 5 * time.Millisecond},
}

func newQueue() *queue.Memory { return queue.NewMemory(100, 20*time.Millisecond) }

// runInBackground starts fn and, when the test ends, cancels its context
// and waits for it to return.
func runInBackground(t *testing.T, fn func(ctx context.Context)) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { fn(ctx) })
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
}

func startWorkers(t *testing.T, n int, store job.Store, q *queue.Memory, handlers map[string]Handler, cfg Config) {
	t.Helper()
	for range n {
		runInBackground(t, New(store, q, handlers, cfg).Run)
	}
}

// startSweeper runs a sweeper that checks every few milliseconds, so retries
// are promoted almost as soon as their backoff ends.
func startSweeper(t *testing.T, store job.Store, q *queue.Memory) {
	t.Helper()
	runInBackground(t, NewSweeper(store, q, SweeperConfig{
		Interval:     2 * time.Millisecond,
		RequeueAfter: time.Hour,
		Backoff:      testConfig.Backoff,
	}).Run)
}

// startSystem runs one worker and a sweeper: everything a job needs to be
// processed and retried.
func startSystem(t *testing.T, store job.Store, q *queue.Memory, handlers map[string]Handler, cfg Config) {
	t.Helper()
	startWorkers(t, 1, store, q, handlers, cfg)
	startSweeper(t, store, q)
}

// submit creates a job and enqueues its ID, like the API does.
func submit(t *testing.T, store job.Store, q *queue.Memory, jobType string, payload json.RawMessage) job.Job {
	t.Helper()
	j := mustCreate(t, store, jobType, payload)
	if err := q.Enqueue(t.Context(), j.ID); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	return j
}

func mustCreate(t *testing.T, store job.Store, jobType string, payload json.RawMessage) job.Job {
	t.Helper()
	j, _, err := store.Create(t.Context(), job.NewJob{Type: jobType, Payload: payload})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return j
}

// waitForTerminal polls until the job is completed or failed.
func waitForTerminal(t *testing.T, store job.Store, id string) job.Job {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		j, err := store.Get(t.Context(), id)
		if err != nil {
			t.Fatalf("Get(%s): %v", id, err)
		}
		if j.Status == job.StatusCompleted || j.Status == job.StatusFailed || j.Status == job.StatusDeadLettered {
			return j
		}
		time.Sleep(2 * time.Millisecond)
	}
	j, _ := store.Get(t.Context(), id)
	t.Fatalf("job %s did not finish in time; last state %+v", id, j)
	return job.Job{}
}

func TestWorkerCompletesJob(t *testing.T) {
	store, q := job.NewMemoryStore(), newQueue()
	startSystem(t, store, q, DefaultHandlers(), testConfig)

	j := submit(t, store, q, "hash", json.RawMessage(`{"input":"hello"}`))

	got := waitForTerminal(t, store, j.ID)
	if got.Status != job.StatusCompleted || got.Attempts != 1 {
		t.Fatalf("got status %s attempts %d, error %q", got.Status, got.Attempts, got.Error)
	}
	if !strings.Contains(string(got.Result), "2cf24dba") {
		t.Errorf("unexpected result %s", got.Result)
	}
}

func TestTransientFailureIsRetriedUntilItSucceeds(t *testing.T) {
	store, q := job.NewMemoryStore(), newQueue()
	startSystem(t, store, q, DefaultHandlers(), testConfig)

	j := submit(t, store, q, "flaky", json.RawMessage(`{"fail_times":2}`))

	got := waitForTerminal(t, store, j.ID)
	if got.Status != job.StatusCompleted || got.Attempts != 3 {
		t.Fatalf("got status %s attempts %d, error %q; want completed on attempt 3", got.Status, got.Attempts, got.Error)
	}
	if string(got.Result) != `{"succeeded_on_attempt":3}` || got.Error != "" {
		t.Errorf("result %s error %q", got.Result, got.Error)
	}
}

func TestTransientFailureStopsAtMaxAttempts(t *testing.T) {
	store, q := job.NewMemoryStore(), newQueue()
	startSystem(t, store, q, DefaultHandlers(), testConfig)

	j := submit(t, store, q, "fail", nil)

	got := waitForTerminal(t, store, j.ID)
	if got.Status != job.StatusDeadLettered || got.Attempts != job.DefaultMaxAttempts {
		t.Fatalf("got status %s attempts %d; want dead_lettered after %d", got.Status, got.Attempts, job.DefaultMaxAttempts)
	}
	if !strings.Contains(got.Error, "gave up after 3 attempts") {
		t.Errorf("error: %q", got.Error)
	}
}

func TestReopenedDeadLetterRunsAgain(t *testing.T) {
	store := job.NewMemoryStore()
	w := New(store, nil, DefaultHandlers(), testConfig)

	j := mustCreate(t, store, "hash", json.RawMessage(`{"input":"hello"}`))
	store.Claim(t.Context(), j.ID, time.Hour)
	store.DeadLetter(t.Context(), j.ID, "gave up")

	reopened, err := store.Reopen(t.Context(), j.ID)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	w.process(t.Context(), reopened.ID)

	got, _ := store.Get(t.Context(), j.ID)
	if got.Status != job.StatusCompleted || got.Attempts != 1 {
		t.Errorf("got status %s attempts %d", got.Status, got.Attempts)
	}
}

func TestPermanentFailureIsNotRetried(t *testing.T) {
	store, q := job.NewMemoryStore(), newQueue()
	startSystem(t, store, q, DefaultHandlers(), testConfig)

	j := submit(t, store, q, "hash", json.RawMessage(`{"wrong":"field"}`))

	got := waitForTerminal(t, store, j.ID)
	if got.Status != job.StatusFailed || got.Attempts != 1 {
		t.Errorf("got status %s attempts %d; want failed after 1", got.Status, got.Attempts)
	}
}

func TestUnknownTypeIsPermanent(t *testing.T) {
	store := job.NewMemoryStore()
	w := New(store, nil, DefaultHandlers(), testConfig)

	j := mustCreate(t, store, "no-such-type", nil)
	w.process(t.Context(), j.ID)

	got, _ := store.Get(t.Context(), j.ID)
	if got.Status != job.StatusFailed || got.Attempts != 1 || !strings.Contains(got.Error, "unknown job type") {
		t.Errorf("got status %s attempts %d error %q", got.Status, got.Attempts, got.Error)
	}
}

func TestSlowJobTimesOutAndIsRetried(t *testing.T) {
	store, q := job.NewMemoryStore(), newQueue()
	cfg := testConfig
	cfg.JobTimeout = 20 * time.Millisecond
	startSystem(t, store, q, DefaultHandlers(), cfg)

	j := submit(t, store, q, "sleep", json.RawMessage(`{"duration_ms":5000}`))

	got := waitForTerminal(t, store, j.ID)
	if got.Status != job.StatusDeadLettered || got.Attempts != 3 {
		t.Fatalf("got status %s attempts %d; want dead_lettered after 3", got.Status, got.Attempts)
	}
	if !strings.Contains(got.Error, "timed out after 20ms") {
		t.Errorf("error: %q", got.Error)
	}
}

func TestWorkerSurvivesPanickingHandler(t *testing.T) {
	store, q := job.NewMemoryStore(), newQueue()
	handlers := DefaultHandlers()
	handlers["panic"] = func(context.Context, job.Job) (json.RawMessage, error) {
		panic("bad handler")
	}
	startSystem(t, store, q, handlers, testConfig)

	bad := submit(t, store, q, "panic", nil)
	good := submit(t, store, q, "hash", json.RawMessage(`{"input":"x"}`))

	got := waitForTerminal(t, store, bad.ID)
	if got.Status != job.StatusFailed || got.Attempts != 1 || !strings.Contains(got.Error, "panicked") {
		t.Errorf("panicking job: got status %s attempts %d error %q", got.Status, got.Attempts, got.Error)
	}
	// The same single worker must still be alive to process the next job.
	if got := waitForTerminal(t, store, good.ID); got.Status != job.StatusCompleted {
		t.Errorf("next job: got status %s", got.Status)
	}
}

// The same ID delivered twice must only run once.
func TestWorkerIgnoresDuplicateDelivery(t *testing.T) {
	store := job.NewMemoryStore()
	var runs atomic.Int32
	w := New(store, nil, map[string]Handler{
		"count": func(context.Context, job.Job) (json.RawMessage, error) {
			runs.Add(1)
			return json.RawMessage(`{}`), nil
		},
	}, testConfig)

	j := mustCreate(t, store, "count", nil)
	w.process(t.Context(), j.ID)
	w.process(t.Context(), j.ID)

	if n := runs.Load(); n != 1 {
		t.Errorf("handler ran %d times, want 1", n)
	}
	if got, _ := store.Get(t.Context(), j.ID); got.Attempts != 1 {
		t.Errorf("attempts: got %d, want 1", got.Attempts)
	}
}

// A job that finishes just as shutdown begins must still be recorded.
func TestWorkerRecordsOutcomeAfterCancellation(t *testing.T) {
	store := job.NewMemoryStore()
	ctx, cancel := context.WithCancel(t.Context())
	w := New(store, nil, map[string]Handler{
		"finish-late": func(context.Context, job.Job) (json.RawMessage, error) {
			cancel() // shutdown begins while the job is still running
			return json.RawMessage(`{"done":true}`), nil
		},
	}, testConfig)

	j := mustCreate(t, store, "finish-late", nil)
	w.process(ctx, j.ID)

	if got, _ := store.Get(t.Context(), j.ID); got.Status != job.StatusCompleted {
		t.Errorf("got status %s, want completed", got.Status)
	}
}

// A job interrupted by shutdown didn't fail on its merits, so it must be
// retried rather than marked failed.
func TestJobInterruptedByShutdownIsRetried(t *testing.T) {
	store := job.NewMemoryStore()
	ctx, cancel := context.WithCancel(t.Context())
	w := New(store, nil, map[string]Handler{
		"interrupted": func(ctx context.Context, _ job.Job) (json.RawMessage, error) {
			cancel()
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}, testConfig)

	j := mustCreate(t, store, "interrupted", nil)
	w.process(ctx, j.ID)

	if got, _ := store.Get(t.Context(), j.ID); got.Status != job.StatusRetrying {
		t.Errorf("got status %s error %q, want retrying", got.Status, got.Error)
	}
}

// Each job blocks until all of them are running at once. If the workers
// processed jobs one at a time, no job would ever be released.
func TestWorkersRunJobsConcurrently(t *testing.T) {
	const n = 4
	store, q := job.NewMemoryStore(), newQueue()

	var running atomic.Int32
	allRunning := make(chan struct{})
	handlers := map[string]Handler{
		"wait": func(ctx context.Context, _ job.Job) (json.RawMessage, error) {
			if running.Add(1) == n {
				close(allRunning)
			}
			select {
			case <-allRunning:
				return json.RawMessage(`{}`), nil
			case <-time.After(time.Second):
				return nil, Permanent(errors.New("jobs did not overlap"))
			}
		},
	}
	startWorkers(t, n, store, q, handlers, testConfig)

	ids := make([]string, n)
	for i := range ids {
		ids[i] = submit(t, store, q, "wait", nil).ID
	}

	for _, id := range ids {
		if got := waitForTerminal(t, store, id); got.Status != job.StatusCompleted {
			t.Errorf("job %s: got %s (%s)", id, got.Status, got.Error)
		}
	}
}

// flakyQueue fails its first failures calls, then delegates to the real queue.
type flakyQueue struct {
	Queue
	failures atomic.Int32
}

func (f *flakyQueue) Dequeue(ctx context.Context) (string, error) {
	if f.failures.Add(-1) >= 0 {
		return "", errors.New("connection refused")
	}
	return f.Queue.Dequeue(ctx)
}

// A queue outage must not kill the worker; it keeps trying and picks up
// work once the queue is back.
func TestWorkerContinuesAfterQueueErrors(t *testing.T) {
	store, q := job.NewMemoryStore(), newQueue()
	flaky := &flakyQueue{Queue: q}
	flaky.failures.Store(3)

	w := New(store, flaky, DefaultHandlers(), testConfig)
	w.retryDelay = time.Millisecond
	runInBackground(t, w.Run)

	j := submit(t, store, q, "hash", json.RawMessage(`{"input":"x"}`))

	if got := waitForTerminal(t, store, j.ID); got.Status != job.StatusCompleted {
		t.Errorf("got status %s", got.Status)
	}
}

func TestRunReturnsWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		New(job.NewMemoryStore(), newQueue(), nil, testConfig).Run(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return")
	}
}

// Graceful shutdown: once stop is cancelled the worker takes no new job,
// but the job already running finishes and is recorded as completed.
func TestRunUntilFinishesJobInProgress(t *testing.T) {
	store, q := job.NewMemoryStore(), newQueue()
	started := make(chan struct{})
	release := make(chan struct{})
	w := New(store, q, map[string]Handler{
		"slow": func(ctx context.Context, _ job.Job) (json.RawMessage, error) {
			close(started)
			select {
			case <-release:
				return json.RawMessage(`{}`), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}, testConfig)

	stop, cancelStop := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		w.RunUntil(stop, t.Context())
		close(done)
	}()

	slow := submit(t, store, q, "slow", nil)
	<-started
	cancelStop() // SIGTERM arrives mid-job
	next := submit(t, store, q, "slow", nil)
	close(release)
	<-done

	if got, _ := store.Get(t.Context(), slow.ID); got.Status != job.StatusCompleted {
		t.Errorf("in-progress job: got %s, want completed", got.Status)
	}
	if got, _ := store.Get(t.Context(), next.ID); got.Status != job.StatusQueued {
		t.Errorf("job submitted after stop: got %s, want queued (left for another worker)", got.Status)
	}
}
