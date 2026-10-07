package job

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var ErrNotFound = errors.New("job not found")

// DefaultMaxAttempts matches the max_attempts column default.
const DefaultMaxAttempts = 3

// NewJob is what a client asks for. Everything else on a Job is filled in
// by the store.
type NewJob struct {
	Type           string
	Payload        json.RawMessage
	IdempotencyKey string
	// TraceParent is the submitting request's W3C traceparent, or empty.
	TraceParent string
}

// Store persists jobs. Both implementations must pass contract_test.go.
//
// Status-changing methods return ErrInvalidTransition when the state machine
// forbids the change. Of several concurrent callers making the same change,
// exactly one succeeds. Durations use the store's clock.
type Store interface {
	// Create stores a new job in the queued state.
	//
	// If n.IdempotencyKey is empty, a new job is always created.
	// If it matches an existing job with the same type and payload, that job
	// is returned and created is false (do not enqueue again).
	// If it matches a job with a different type or payload, the error is
	// ErrIdempotencyConflict.
	Create(ctx context.Context, n NewJob) (j Job, created bool, err error)
	// Get returns ErrNotFound if no job has this ID.
	Get(ctx context.Context, id string) (Job, error)
	// List returns jobs ordered by creation time. An empty status means all.
	List(ctx context.Context, status Status) ([]Job, error)
	Delete(ctx context.Context, id string) error

	// Claim moves a queued job to running, counts the attempt, and leases the
	// job to the caller for the given duration.
	Claim(ctx context.Context, id string, lease time.Duration) (Job, error)
	// Complete moves a running job to completed and records its result.
	Complete(ctx context.Context, id string, result json.RawMessage) (Job, error)
	// Fail moves a running job to failed: a permanent error, not auto-retried.
	Fail(ctx context.Context, id string, reason string) (Job, error)
	// DeadLetter moves a running job to dead_lettered: retries are exhausted.
	DeadLetter(ctx context.Context, id string, reason string) (Job, error)
	// Retry moves a running job to retrying; it becomes due after delay.
	Retry(ctx context.Context, id string, reason string, delay time.Duration) (Job, error)
	// Reopen moves a failed or dead-lettered job back to queued and resets
	// attempts so it gets a full retry budget. The caller must enqueue the ID.
	Reopen(ctx context.Context, id string) (Job, error)

	// PromoteDue moves every retrying job whose delay has passed back to
	// queued and returns their IDs, which the caller must enqueue.
	PromoteDue(ctx context.Context) ([]string, error)
	// ExpiredLeases returns running jobs whose lease has passed.
	ExpiredLeases(ctx context.Context) ([]Job, error)
	// RequeueStale returns queued jobs last enqueued more than olderThan ago
	// and resets their enqueue time. The caller must enqueue them again.
	RequeueStale(ctx context.Context, olderThan time.Duration) ([]string, error)
}
