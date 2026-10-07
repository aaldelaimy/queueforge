package worker

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aaldelaimy/queueforge/internal/job"
	"github.com/aaldelaimy/queueforge/internal/queue"
)

func newTestSweeper(store job.Store, q *queue.Memory, requeueAfter time.Duration) *Sweeper {
	return NewSweeper(store, q, SweeperConfig{
		Interval:            time.Hour, // tests call sweep directly
		RequeueAfter:        requeueAfter,
		BacklogRequeueAfter: time.Hour,
		Backoff:             Backoff{}, // zero delay: retries are due immediately
	})
}

// claimAndDie leaves a job as a killed worker would: running, lease expired.
func claimAndDie(t *testing.T, store job.Store, id string) {
	t.Helper()
	if _, err := store.Claim(t.Context(), id, -time.Second); err != nil {
		t.Fatalf("Claim: %v", err)
	}
}

func mustDequeue(t *testing.T, q *queue.Memory) string {
	t.Helper()
	id, err := q.Dequeue(t.Context())
	if err != nil {
		t.Fatalf("expected an ID on the queue: %v", err)
	}
	return id
}

func TestSweeperRecoversAbandonedJob(t *testing.T) {
	store, q := job.NewMemoryStore(), newQueue()
	s := newTestSweeper(store, q, time.Hour)
	j := mustCreate(t, store, "hash", json.RawMessage(`{"input":"x"}`))
	claimAndDie(t, store, j.ID)

	s.sweep(t.Context())

	if id := mustDequeue(t, q); id != j.ID {
		t.Errorf("enqueued %s, want %s", id, j.ID)
	}
	got, _ := store.Get(t.Context(), j.ID)
	if got.Status != job.StatusQueued || got.Attempts != 1 || !strings.Contains(got.Error, "abandoned") {
		t.Errorf("got status %s attempts %d error %q", got.Status, got.Attempts, got.Error)
	}

	// A live worker can now run it as attempt 2.
	w := New(store, nil, DefaultHandlers(), testConfig)
	w.process(t.Context(), j.ID)
	if got, _ := store.Get(t.Context(), j.ID); got.Status != job.StatusCompleted || got.Attempts != 2 {
		t.Errorf("after rerun: status %s attempts %d", got.Status, got.Attempts)
	}
}

// A "poison message": a job that kills its worker every time. Each abandoned
// attempt still counts, so it fails after max attempts instead of looping.
func TestSweeperFailsJobThatKeepsKillingWorkers(t *testing.T) {
	store, q := job.NewMemoryStore(), newQueue()
	s := newTestSweeper(store, q, time.Hour)
	j := mustCreate(t, store, "hash", json.RawMessage(`{"input":"x"}`))

	for attempt := 1; attempt <= job.DefaultMaxAttempts; attempt++ {
		claimAndDie(t, store, j.ID)
		s.sweep(t.Context())
		if attempt < job.DefaultMaxAttempts {
			mustDequeue(t, q)
		}
	}

	got, _ := store.Get(t.Context(), j.ID)
	if got.Status != job.StatusDeadLettered || got.Attempts != job.DefaultMaxAttempts {
		t.Fatalf("got status %s attempts %d; want dead_lettered after %d", got.Status, got.Attempts, job.DefaultMaxAttempts)
	}
	if !strings.Contains(got.Error, "abandoned") || !strings.Contains(got.Error, "gave up") {
		t.Errorf("error: %q", got.Error)
	}
}

func TestSweeperRequeuesJobWhoseIDWasLost(t *testing.T) {
	store, q := job.NewMemoryStore(), newQueue()
	// A negative age makes every queued job count as stale.
	s := newTestSweeper(store, q, -time.Second)
	j := mustCreate(t, store, "hash", nil) // stored, but its ID never reached the queue

	s.sweep(t.Context())

	if id := mustDequeue(t, q); id != j.ID {
		t.Errorf("enqueued %s, want %s", id, j.ID)
	}
}

// While the queue has a backlog, an old queued job is most likely still in
// line. Pushing it again would only add a duplicate to the backlog.
func TestSweeperDoesNotRequeueDuringBacklog(t *testing.T) {
	store, q := job.NewMemoryStore(), newQueue()
	s := newTestSweeper(store, q, -time.Second) // stale if the queue were empty
	waiting := mustCreate(t, store, "hash", nil)
	if err := q.Enqueue(t.Context(), "some-other-job"); err != nil {
		t.Fatal(err)
	}

	s.sweep(t.Context())

	if n, _ := q.Len(t.Context()); n != 1 {
		t.Fatalf("queue length %d, want 1: %s was pushed during a backlog", n, waiting.ID)
	}
}

// The backstop: a job queued longer than BacklogRequeueAfter is pushed
// even during a backlog, so a lost ID can't wait forever.
func TestSweeperBacklogBackstop(t *testing.T) {
	store, q := job.NewMemoryStore(), newQueue()
	s := NewSweeper(store, q, SweeperConfig{
		Interval:            time.Hour,
		RequeueAfter:        -time.Second,
		BacklogRequeueAfter: -time.Second,
	})
	lost := mustCreate(t, store, "hash", nil)
	if err := q.Enqueue(t.Context(), "some-other-job"); err != nil {
		t.Fatal(err)
	}

	s.sweep(t.Context())

	mustDequeue(t, q) // some-other-job
	if id := mustDequeue(t, q); id != lost.ID {
		t.Errorf("enqueued %s, want %s", id, lost.ID)
	}
}

func TestSweeperLeavesHealthyJobsAlone(t *testing.T) {
	store, q := job.NewMemoryStore(), newQueue()
	s := newTestSweeper(store, q, time.Hour)
	queued := mustCreate(t, store, "hash", nil)
	running := mustCreate(t, store, "hash", nil)
	store.Claim(t.Context(), running.ID, time.Hour)

	s.sweep(t.Context())

	if id, err := q.Dequeue(t.Context()); err == nil {
		t.Errorf("enqueued %s, want nothing", id)
	}
	if got, _ := store.Get(t.Context(), queued.ID); got.Status != job.StatusQueued {
		t.Errorf("queued job: got %s", got.Status)
	}
	if got, _ := store.Get(t.Context(), running.ID); got.Status != job.StatusRunning {
		t.Errorf("running job: got %s", got.Status)
	}
}

// Several sweepers can run at once (one per worker process). Recovering an
// abandoned job is two steps (find, then Retry/Fail), but Retry is
// conditional, so only one sweeper records the failed attempt.
func TestConcurrentSweepersRecoverAbandonedJobOnce(t *testing.T) {
	store, q := job.NewMemoryStore(), newQueue()
	j := mustCreate(t, store, "hash", json.RawMessage(`{"input":"x"}`))
	claimAndDie(t, store, j.ID)

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { newTestSweeper(store, q, time.Hour).sweep(t.Context()) })
	}
	wg.Wait()

	got, _ := store.Get(t.Context(), j.ID)
	if got.Attempts != 1 {
		t.Errorf("attempts: got %d, want 1 (the failed attempt must be recorded once)", got.Attempts)
	}
	if got.Status != job.StatusQueued && got.Status != job.StatusRetrying {
		t.Errorf("got status %s, want queued or retrying", got.Status)
	}
}
