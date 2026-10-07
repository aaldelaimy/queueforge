package job

import (
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"
)

// testStoreContract runs the same behavioral tests against any Store.
// newStore must return an empty store.
func testStoreContract(t *testing.T, newStore func(t *testing.T) Store) {
	tests := []struct {
		name string
		fn   func(t *testing.T, s Store)
	}{
		{"CreateAndGet", testCreateAndGet},
		{"CreateWithoutPayload", testCreateWithoutPayload},
		{"CreateStoresTraceParent", testCreateStoresTraceParent},
		{"GetUnknownJob", testGetUnknownJob},
		{"ListReturnsAllJobsInCreationOrder", testListReturnsAllJobsInCreationOrder},
		{"ClaimCountsAttemptAndSetsLease", testClaimCountsAttemptAndSetsLease},
		{"CompleteRecordsResult", testCompleteRecordsResult},
		{"FailRecordsError", testFailRecordsError},
		{"DeadLetterRecordsError", testDeadLetterRecordsError},
		{"ReopenResetsAttempts", testReopenResetsAttempts},
		{"ReopenOnlyFromFailedOrDeadLettered", testReopenOnlyFromFailedOrDeadLettered},
		{"ListFiltersByStatus", testListFiltersByStatus},
		{"RetrySchedulesNextAttempt", testRetrySchedulesNextAttempt},
		{"FullRetryCycle", testFullRetryCycle},
		{"InvalidTransitionLeavesJobUnchanged", testInvalidTransitionLeavesJobUnchanged},
		{"TerminalStatesAreFinal", testTerminalStatesAreFinal},
		{"UnknownJobTransitions", testUnknownJobTransitions},
		{"Delete", testDelete},
		{"ReturnedJobsAreCopies", testReturnedJobsAreCopies},
		{"PromoteDueOnlyPromotesDueJobs", testPromoteDueOnlyPromotesDueJobs},
		{"ExpiredLeases", testExpiredLeases},
		{"RequeueStale", testRequeueStale},
		{"ConcurrentCreate", testConcurrentCreate},
		{"IdempotentCreateReturnsSameJob", testIdempotentCreateReturnsSameJob},
		{"IdempotentCreateRejectsDifferentRequest", testIdempotentCreateRejectsDifferentRequest},
		{"CreateWithoutKeyAreIndependent", testCreateWithoutKeyAreIndependent},
		{"DeleteFreesIdempotencyKey", testDeleteFreesIdempotencyKey},
		{"ConcurrentIdempotentCreateOnlyOneJob", testConcurrentIdempotentCreateOnlyOneJob},
		{"ConcurrentClaimOnlyOneWins", testConcurrentClaimOnlyOneWins},
		{"ConcurrentPromoteReturnsEachJobOnce", testConcurrentPromoteReturnsEachJobOnce},
		{"ConcurrentReadWhileWriting", testConcurrentReadWhileWriting},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { tc.fn(t, newStore(t)) })
	}
}

func mustCreate(t *testing.T, s Store, jobType string, payload json.RawMessage) Job {
	t.Helper()
	j, created, err := s.Create(t.Context(), NewJob{Type: jobType, Payload: payload})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !created {
		t.Fatal("Create without a key must always create a job")
	}
	return j
}

func mustClaim(t *testing.T, s Store, id string) Job {
	t.Helper()
	j, err := s.Claim(t.Context(), id, time.Hour)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	return j
}

// jsonEqual compares JSON by meaning, not bytes: Postgres's jsonb type
// reformats what it stores (spacing, key order).
func jsonEqual(t *testing.T, got json.RawMessage, want string) bool {
	t.Helper()
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("invalid JSON %q: %v", got, err)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("invalid JSON %q: %v", want, err)
	}
	return reflect.DeepEqual(g, w)
}

func testCreateAndGet(t *testing.T, s Store) {
	created := mustCreate(t, s, "image-processing", json.RawMessage(`{"input":"example"}`))
	if created.ID == "" {
		t.Fatal("expected a non-empty ID")
	}
	if created.Status != StatusQueued || created.Attempts != 0 || created.MaxAttempts != DefaultMaxAttempts {
		t.Errorf("got status %q attempts %d max %d", created.Status, created.Attempts, created.MaxAttempts)
	}

	got, err := s.Get(t.Context(), created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Type != "image-processing" || !jsonEqual(t, got.Payload, `{"input":"example"}`) {
		t.Errorf("got %+v", got)
	}
	if !got.CreatedAt.Equal(created.CreatedAt) || got.CreatedAt.Location().String() != "UTC" {
		t.Errorf("CreatedAt: got %v, created %v", got.CreatedAt, created.CreatedAt)
	}
	if !got.RunAfter.IsZero() || !got.LeaseExpiresAt.IsZero() {
		t.Errorf("new job has run_after %v lease %v", got.RunAfter, got.LeaseExpiresAt)
	}
}

func testCreateWithoutPayload(t *testing.T, s Store) {
	j := mustCreate(t, s, "t", nil)

	got, _ := s.Get(t.Context(), j.ID)
	if got.Payload != nil || got.Result != nil || got.Error != "" {
		t.Errorf("expected empty payload, result, error; got %+v", got)
	}
}

func testGetUnknownJob(t *testing.T, s Store) {
	if _, err := s.Get(t.Context(), "does-not-exist"); !errors.Is(err, ErrNotFound) {
		t.Errorf("got %v, want ErrNotFound", err)
	}
}

func testListReturnsAllJobsInCreationOrder(t *testing.T, s Store) {
	empty, err := s.List(t.Context(), "")
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("empty store: got %v, %v; want a non-nil empty slice", empty, err)
	}

	want := map[string]bool{}
	for range 5 {
		want[mustCreate(t, s, "t", nil).ID] = true
	}

	jobs, err := s.List(t.Context(), "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(jobs) != len(want) {
		t.Fatalf("got %d jobs, want %d", len(jobs), len(want))
	}
	for i, j := range jobs {
		if !want[j.ID] {
			t.Errorf("unexpected job %s", j.ID)
		}
		if i > 0 && j.CreatedAt.Before(jobs[i-1].CreatedAt) {
			t.Errorf("jobs not sorted by CreatedAt at index %d", i)
		}
	}
}

func testClaimCountsAttemptAndSetsLease(t *testing.T, s Store) {
	j := mustCreate(t, s, "t", nil)

	running := mustClaim(t, s, j.ID)

	if running.Status != StatusRunning || running.Attempts != 1 {
		t.Errorf("got status %s attempts %d", running.Status, running.Attempts)
	}
	if running.LeaseExpiresAt.Before(running.UpdatedAt.Add(59 * time.Minute)) {
		t.Errorf("lease expires %v, want about an hour after %v", running.LeaseExpiresAt, running.UpdatedAt)
	}
}

func testCompleteRecordsResult(t *testing.T, s Store) {
	j := mustCreate(t, s, "t", nil)
	mustClaim(t, s, j.ID)

	done, err := s.Complete(t.Context(), j.ID, json.RawMessage(`{"ok":true}`))
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if done.Status != StatusCompleted || !jsonEqual(t, done.Result, `{"ok":true}`) {
		t.Errorf("got %+v", done)
	}
	if !done.LeaseExpiresAt.IsZero() {
		t.Errorf("completed job still has a lease: %v", done.LeaseExpiresAt)
	}
}

func testFailRecordsError(t *testing.T, s Store) {
	j := mustCreate(t, s, "t", nil)
	mustClaim(t, s, j.ID)

	failed, err := s.Fail(t.Context(), j.ID, "boom")
	if err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if failed.Status != StatusFailed || failed.Error != "boom" || failed.Result != nil {
		t.Errorf("got %+v", failed)
	}
}

func testDeadLetterRecordsError(t *testing.T, s Store) {
	j := mustCreate(t, s, "t", nil)
	mustClaim(t, s, j.ID)

	dl, err := s.DeadLetter(t.Context(), j.ID, "gave up")
	if err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}
	if dl.Status != StatusDeadLettered || dl.Error != "gave up" {
		t.Errorf("got %+v", dl)
	}
}

func testReopenResetsAttempts(t *testing.T, s Store) {
	j := mustCreate(t, s, "t", nil)
	mustClaim(t, s, j.ID)
	s.DeadLetter(t.Context(), j.ID, "gave up")

	reopened, err := s.Reopen(t.Context(), j.ID)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	if reopened.Status != StatusQueued || reopened.Attempts != 0 {
		t.Errorf("got status %s attempts %d", reopened.Status, reopened.Attempts)
	}
	if reopened.Error != "gave up" {
		t.Errorf("last error should remain until the job succeeds, got %q", reopened.Error)
	}

	second := mustClaim(t, s, j.ID)
	if second.Attempts != 1 {
		t.Errorf("attempts after reopen+claim: got %d, want 1", second.Attempts)
	}
}

func testReopenOnlyFromFailedOrDeadLettered(t *testing.T, s Store) {
	queued := mustCreate(t, s, "t", nil)
	if _, err := s.Reopen(t.Context(), queued.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("queued: got %v", err)
	}

	running := mustCreate(t, s, "t", nil)
	mustClaim(t, s, running.ID)
	if _, err := s.Reopen(t.Context(), running.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("running: got %v", err)
	}

	failed := mustCreate(t, s, "t", nil)
	mustClaim(t, s, failed.ID)
	s.Fail(t.Context(), failed.ID, "bad payload")
	if _, err := s.Reopen(t.Context(), failed.ID); err != nil {
		t.Errorf("failed should reopen: %v", err)
	}
}

func testListFiltersByStatus(t *testing.T, s Store) {
	q := mustCreate(t, s, "t", nil)
	r := mustCreate(t, s, "t", nil)
	mustClaim(t, s, r.ID)

	queued, err := s.List(t.Context(), StatusQueued)
	if err != nil || len(queued) != 1 || queued[0].ID != q.ID {
		t.Fatalf("queued: got %+v, %v", queued, err)
	}
	running, err := s.List(t.Context(), StatusRunning)
	if err != nil || len(running) != 1 || running[0].ID != r.ID {
		t.Fatalf("running: got %+v, %v", running, err)
	}
	if done, _ := s.List(t.Context(), StatusCompleted); len(done) != 0 {
		t.Errorf("completed: got %d, want 0", len(done))
	}
}

func testRetrySchedulesNextAttempt(t *testing.T, s Store) {
	j := mustCreate(t, s, "t", nil)
	mustClaim(t, s, j.ID)

	retrying, err := s.Retry(t.Context(), j.ID, "timeout", time.Hour)
	if err != nil {
		t.Fatalf("Retry: %v", err)
	}
	if retrying.Status != StatusRetrying || retrying.Error != "timeout" || retrying.Attempts != 1 {
		t.Errorf("got %+v", retrying)
	}
	if retrying.RunAfter.Before(retrying.UpdatedAt.Add(59 * time.Minute)) {
		t.Errorf("run_after %v, want about an hour after %v", retrying.RunAfter, retrying.UpdatedAt)
	}
	if !retrying.LeaseExpiresAt.IsZero() {
		t.Errorf("retrying job still has a lease: %v", retrying.LeaseExpiresAt)
	}
}

// queued -> running -> retrying -> queued -> running -> completed
func testFullRetryCycle(t *testing.T, s Store) {
	j := mustCreate(t, s, "t", nil)
	mustClaim(t, s, j.ID)
	if _, err := s.Retry(t.Context(), j.ID, "flaky", 0); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	if ids, err := s.PromoteDue(t.Context()); err != nil || !slices.Contains(ids, j.ID) {
		t.Fatalf("PromoteDue: got %v, %v", ids, err)
	}

	second := mustClaim(t, s, j.ID)
	if second.Attempts != 2 {
		t.Errorf("attempts on second claim: got %d, want 2", second.Attempts)
	}
	if second.Error != "flaky" {
		t.Errorf("error from the previous attempt should still be visible while running, got %q", second.Error)
	}

	done, err := s.Complete(t.Context(), j.ID, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if done.Error != "" || !done.RunAfter.IsZero() {
		t.Errorf("completed job: error %q run_after %v; want both cleared", done.Error, done.RunAfter)
	}
}

func testInvalidTransitionLeavesJobUnchanged(t *testing.T, s Store) {
	j := mustCreate(t, s, "t", nil)

	ops := map[string]func() error{
		"Complete": func() error { _, err := s.Complete(t.Context(), j.ID, json.RawMessage(`{}`)); return err },
		"Fail":     func() error { _, err := s.Fail(t.Context(), j.ID, "x"); return err },
		"Retry":    func() error { _, err := s.Retry(t.Context(), j.ID, "x", 0); return err },
	}
	for name, op := range ops {
		if err := op(); !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("%s on a queued job: got %v, want ErrInvalidTransition", name, err)
		}
	}

	got, _ := s.Get(t.Context(), j.ID)
	if got.Status != StatusQueued || !got.UpdatedAt.Equal(j.UpdatedAt) || got.Result != nil || got.Error != "" {
		t.Errorf("job changed after rejected transitions: %+v", got)
	}
}

func testTerminalStatesAreFinal(t *testing.T, s Store) {
	completed := mustCreate(t, s, "t", nil)
	mustClaim(t, s, completed.ID)
	s.Complete(t.Context(), completed.ID, json.RawMessage(`{}`))

	failed := mustCreate(t, s, "t", nil)
	mustClaim(t, s, failed.ID)
	s.Fail(t.Context(), failed.ID, "x")

	for _, id := range []string{completed.ID, failed.ID} {
		if _, err := s.Claim(t.Context(), id, time.Hour); !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("Claim: got %v, want ErrInvalidTransition", err)
		}
		if _, err := s.Retry(t.Context(), id, "x", 0); !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("Retry: got %v, want ErrInvalidTransition", err)
		}
		if _, err := s.Complete(t.Context(), id, json.RawMessage(`{}`)); !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("Complete: got %v, want ErrInvalidTransition", err)
		}
	}
	if _, err := s.Reopen(t.Context(), completed.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("Reopen completed: got %v, want ErrInvalidTransition", err)
	}
}

func testUnknownJobTransitions(t *testing.T, s Store) {
	if _, err := s.Claim(t.Context(), "does-not-exist", time.Hour); !errors.Is(err, ErrNotFound) {
		t.Errorf("Claim: got %v, want ErrNotFound", err)
	}
	if _, err := s.Retry(t.Context(), "does-not-exist", "x", 0); !errors.Is(err, ErrNotFound) {
		t.Errorf("Retry: got %v, want ErrNotFound", err)
	}
	if _, err := s.Reopen(t.Context(), "does-not-exist"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Reopen: got %v, want ErrNotFound", err)
	}
}

func testDelete(t *testing.T, s Store) {
	j := mustCreate(t, s, "t", nil)
	if err := s.Delete(t.Context(), j.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get(t.Context(), j.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("got %v, want ErrNotFound", err)
	}
}

func testReturnedJobsAreCopies(t *testing.T, s Store) {
	j := mustCreate(t, s, "t", json.RawMessage(`{"a":1}`))

	got, _ := s.Get(t.Context(), j.ID)
	got.Status = StatusCompleted
	got.Payload[0] = 'X'

	again, _ := s.Get(t.Context(), j.ID)
	if again.Status != StatusQueued || !jsonEqual(t, again.Payload, `{"a":1}`) {
		t.Errorf("mutating a returned job changed the store: %+v", again)
	}
}

func testPromoteDueOnlyPromotesDueJobs(t *testing.T, s Store) {
	due := mustCreate(t, s, "t", nil)
	mustClaim(t, s, due.ID)
	s.Retry(t.Context(), due.ID, "x", 0)

	later := mustCreate(t, s, "t", nil)
	mustClaim(t, s, later.ID)
	s.Retry(t.Context(), later.ID, "x", time.Hour)

	mustCreate(t, s, "t", nil) // plain queued job: not retrying, never promoted

	ids, err := s.PromoteDue(t.Context())
	if err != nil {
		t.Fatalf("PromoteDue: %v", err)
	}
	if len(ids) != 1 || ids[0] != due.ID {
		t.Fatalf("promoted %v, want only %s", ids, due.ID)
	}

	got, _ := s.Get(t.Context(), due.ID)
	if got.Status != StatusQueued || !got.RunAfter.IsZero() {
		t.Errorf("promoted job: status %s run_after %v", got.Status, got.RunAfter)
	}
	if got, _ := s.Get(t.Context(), later.ID); got.Status != StatusRetrying {
		t.Errorf("job not yet due: got %s, want retrying", got.Status)
	}
	if again, _ := s.PromoteDue(t.Context()); len(again) != 0 {
		t.Errorf("second PromoteDue returned %v, want nothing", again)
	}
}

func testExpiredLeases(t *testing.T, s Store) {
	expired := mustCreate(t, s, "t", nil)
	if _, err := s.Claim(t.Context(), expired.ID, -time.Second); err != nil { // already expired
		t.Fatalf("Claim: %v", err)
	}
	live := mustCreate(t, s, "t", nil)
	mustClaim(t, s, live.ID)

	jobs, err := s.ExpiredLeases(t.Context())
	if err != nil {
		t.Fatalf("ExpiredLeases: %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != expired.ID || jobs[0].Attempts != 1 {
		t.Errorf("got %+v, want only %s", jobs, expired.ID)
	}
}

func testRequeueStale(t *testing.T, s Store) {
	queued := mustCreate(t, s, "t", nil)
	running := mustCreate(t, s, "t", nil)
	mustClaim(t, s, running.ID)

	// A negative age makes every queued job count as stale.
	ids, err := s.RequeueStale(t.Context(), -time.Second)
	if err != nil {
		t.Fatalf("RequeueStale: %v", err)
	}
	if len(ids) != 1 || ids[0] != queued.ID {
		t.Fatalf("got %v, want only %s", ids, queued.ID)
	}

	// Its enqueue time was reset, so it isn't stale any more.
	if again, _ := s.RequeueStale(t.Context(), time.Minute); len(again) != 0 {
		t.Errorf("second RequeueStale returned %v, want nothing", again)
	}
}

func testConcurrentCreate(t *testing.T, s Store) {
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			if _, _, err := s.Create(t.Context(), NewJob{Type: "t", Payload: nil}); err != nil {
				t.Errorf("Create: %v", err)
			}
		})
	}
	wg.Wait()

	if jobs, _ := s.List(t.Context(), ""); len(jobs) != 100 {
		t.Errorf("got %d jobs, want 100", len(jobs))
	}
}

func testIdempotentCreateReturnsSameJob(t *testing.T, s Store) {
	payload := json.RawMessage(`{"input":"x"}`)
	first, created, err := s.Create(t.Context(), NewJob{Type: "hash", Payload: payload, IdempotencyKey: "key-1"})
	if err != nil || !created {
		t.Fatalf("first Create: created=%v err=%v", created, err)
	}

	second, created, err := s.Create(t.Context(), NewJob{Type: "hash", Payload: json.RawMessage(`{"input":"x"}`), IdempotencyKey: "key-1"})
	if err != nil || created {
		t.Fatalf("replay: created=%v err=%v; want the existing job", created, err)
	}
	if second.ID != first.ID {
		t.Errorf("replay ID %s, want %s", second.ID, first.ID)
	}

	jobs, _ := s.List(t.Context(), "")
	if len(jobs) != 1 {
		t.Errorf("store has %d jobs, want 1", len(jobs))
	}

	mustClaim(t, s, first.ID)
	s.Complete(t.Context(), first.ID, json.RawMessage(`{}`))
	replayed, created, err := s.Create(t.Context(), NewJob{Type: "hash", Payload: payload, IdempotencyKey: "key-1"})
	if err != nil || created || replayed.Status != StatusCompleted || replayed.ID != first.ID {
		t.Errorf("replay after complete: %+v created=%v err=%v", replayed, created, err)
	}
}

func testIdempotentCreateRejectsDifferentRequest(t *testing.T, s Store) {
	if _, _, err := s.Create(t.Context(), NewJob{Type: "hash", Payload: json.RawMessage(`{"input":"a"}`), IdempotencyKey: "key-1"}); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	_, _, err := s.Create(t.Context(), NewJob{Type: "hash", Payload: json.RawMessage(`{"input":"b"}`), IdempotencyKey: "key-1"})
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Errorf("different payload: got %v, want ErrIdempotencyConflict", err)
	}
	_, _, err = s.Create(t.Context(), NewJob{Type: "sleep", Payload: json.RawMessage(`{"input":"a"}`), IdempotencyKey: "key-1"})
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Errorf("different type: got %v, want ErrIdempotencyConflict", err)
	}

	if jobs, _ := s.List(t.Context(), ""); len(jobs) != 1 {
		t.Errorf("conflict must not create a second job; got %d", len(jobs))
	}
}

func testCreateWithoutKeyAreIndependent(t *testing.T, s Store) {
	a, _, _ := s.Create(t.Context(), NewJob{Type: "hash", Payload: nil})
	b, _, _ := s.Create(t.Context(), NewJob{Type: "hash", Payload: nil})
	if a.ID == b.ID {
		t.Error("two submissions without a key produced the same job")
	}
}

func testDeleteFreesIdempotencyKey(t *testing.T, s Store) {
	first, _, err := s.Create(t.Context(), NewJob{Type: "hash", Payload: nil, IdempotencyKey: "reusable"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	s.Delete(t.Context(), first.ID)

	second, created, err := s.Create(t.Context(), NewJob{Type: "hash", Payload: nil, IdempotencyKey: "reusable"})
	if err != nil || !created {
		t.Fatalf("after Delete: created=%v err=%v", created, err)
	}
	if second.ID == first.ID {
		t.Error("deleted job came back; the key should have been freed")
	}
}

func testConcurrentIdempotentCreateOnlyOneJob(t *testing.T, s Store) {
	const n = 50
	start := make(chan struct{})
	ids := make([]string, n)
	created := make([]bool, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			<-start
			j, ok, err := s.Create(t.Context(), NewJob{Type: "hash", Payload: json.RawMessage(`{"n":1}`), IdempotencyKey: "same-key"})
			if err != nil {
				t.Errorf("Create: %v", err)
				return
			}
			ids[i] = j.ID
			created[i] = ok
		})
	}
	close(start)
	wg.Wait()

	wins := 0
	for i := range n {
		if created[i] {
			wins++
		}
		if ids[i] == "" || ids[i] != ids[0] {
			t.Fatalf("caller %d got ID %q, want %q", i, ids[i], ids[0])
		}
	}
	if wins != 1 {
		t.Errorf("created=%d, want 1", wins)
	}
	if jobs, _ := s.List(t.Context(), ""); len(jobs) != 1 {
		t.Errorf("store has %d jobs, want 1", len(jobs))
	}
}

// Many goroutines claim the same queued job; exactly one must win. A single
// trial rarely exposes the race, so they start together and it's repeated.
func testConcurrentClaimOnlyOneWins(t *testing.T, s Store) {
	for trial := range 200 {
		j := mustCreate(t, s, "t", nil)

		start := make(chan struct{})
		var mu sync.Mutex
		wins := 0
		var wg sync.WaitGroup
		for range 50 {
			wg.Go(func() {
				<-start
				_, err := s.Claim(t.Context(), j.ID, time.Hour)
				if err != nil && !errors.Is(err, ErrInvalidTransition) {
					t.Errorf("unexpected error: %v", err)
				}
				if err == nil {
					mu.Lock()
					wins++
					mu.Unlock()
				}
			})
		}
		close(start)
		wg.Wait()

		if wins != 1 {
			t.Fatalf("trial %d: got %d successful claims, want 1", trial, wins)
		}
		if got, _ := s.Get(t.Context(), j.ID); got.Attempts != 1 {
			t.Fatalf("trial %d: attempts: got %d, want 1", trial, got.Attempts)
		}
	}
}

// Several worker processes each run a sweeper. A due job must be promoted,
// and so enqueued, by only one of them.
func testConcurrentPromoteReturnsEachJobOnce(t *testing.T, s Store) {
	const n = 20
	for range n {
		j := mustCreate(t, s, "t", nil)
		mustClaim(t, s, j.ID)
		s.Retry(t.Context(), j.ID, "x", 0)
	}

	start := make(chan struct{})
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			<-start
			ids, err := s.PromoteDue(t.Context())
			if err != nil {
				t.Errorf("PromoteDue: %v", err)
			}
			mu.Lock()
			for _, id := range ids {
				seen[id]++
			}
			mu.Unlock()
		})
	}
	close(start)
	wg.Wait()

	if len(seen) != n {
		t.Errorf("promoted %d distinct jobs, want %d", len(seen), n)
	}
	for id, c := range seen {
		if c != 1 {
			t.Errorf("job %s promoted %d times", id, c)
		}
	}
}

// Readers serialize jobs while writers change them. Run with -race: before
// MemoryStore returned copies, this pattern was a data race.
func testConcurrentReadWhileWriting(t *testing.T, s Store) {
	ids := make([]string, 20)
	for i := range ids {
		ids[i] = mustCreate(t, s, "t", json.RawMessage(`{}`)).ID
	}

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Go(func() {
			s.Claim(t.Context(), id, time.Hour)
			s.Complete(t.Context(), id, json.RawMessage(`{}`))
		})
		wg.Go(func() {
			j, _ := s.Get(t.Context(), id)
			json.Marshal(j)
			jobs, _ := s.List(t.Context(), "")
			json.Marshal(jobs)
		})
	}
	wg.Wait()

	jobs, _ := s.List(t.Context(), "")
	for _, j := range jobs {
		if j.Status != StatusCompleted {
			t.Errorf("job %s: got %s, want completed", j.ID, j.Status)
		}
	}
}

func testCreateStoresTraceParent(t *testing.T, s Store) {
	const tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	created, _, err := s.Create(t.Context(), NewJob{Type: "t", TraceParent: tp})
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(t.Context(), created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.TraceParent != tp {
		t.Errorf("TraceParent: got %q, want %q", got.TraceParent, tp)
	}
}
