package job

import (
	"encoding/json"
	"errors"
	"reflect"
	"time"
)

// Status is the lifecycle state of a job.
type Status string

const (
	StatusQueued       Status = "queued"
	StatusRunning      Status = "running"
	StatusRetrying     Status = "retrying"
	StatusCompleted    Status = "completed"
	StatusFailed       Status = "failed"
	StatusDeadLettered Status = "dead_lettered"
)

// KnownStatuses is every status a client may filter on.
var KnownStatuses = []Status{
	StatusQueued, StatusRunning, StatusRetrying,
	StatusCompleted, StatusFailed, StatusDeadLettered,
}

// transitions lists, for each status, the statuses a job may move to next.
// completed is the only truly terminal status: failed and dead_lettered
// can be reopened by a human.
var transitions = map[Status][]Status{
	StatusQueued:       {StatusRunning},
	StatusRunning:      {StatusCompleted, StatusFailed, StatusRetrying, StatusDeadLettered},
	StatusRetrying:     {StatusQueued},
	StatusFailed:       {StatusQueued},
	StatusDeadLettered: {StatusQueued},
}

// ParseStatus accepts a client-supplied status string.
func ParseStatus(s string) (Status, bool) {
	st := Status(s)
	for _, known := range KnownStatuses {
		if st == known {
			return st, true
		}
	}
	return "", false
}

var (
	ErrInvalidTransition   = errors.New("invalid status transition")
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different request")
)

// CanTransitionTo reports whether a job in status s may move to next.
func (s Status) CanTransitionTo(next Status) bool {
	for _, allowed := range transitions[s] {
		if allowed == next {
			return true
		}
	}
	return false
}

// Job is the record for one unit of work.
type Job struct {
	ID          string          `json:"job_id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	Status      Status          `json:"status"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	Result      json.RawMessage `json:"result,omitempty"`
	// Error is the most recent failure. It is cleared when the job completes.
	Error string `json:"error,omitempty"`
	// RunAfter is when a retrying job becomes eligible to run again.
	RunAfter  time.Time `json:"run_after,omitzero"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

	// IdempotencyKey lets a client retry POST /jobs safely. Not returned.
	IdempotencyKey string `json:"-"`
	// TraceParent links worker spans to the submitting request's trace.
	TraceParent string `json:"-"`

	// Internal bookkeeping for the sweeper, not shown to clients.
	EnqueuedAt     time.Time `json:"-"`
	LeaseExpiresAt time.Time `json:"-"`
}

// sameRequest reports whether two submissions are the same work. jsonb can
// reformat JSON, so we compare meaning, not bytes.
func sameRequest(typeA string, payloadA json.RawMessage, typeB string, payloadB json.RawMessage) bool {
	if typeA != typeB {
		return false
	}
	if len(payloadA) == 0 && len(payloadB) == 0 {
		return true
	}
	var a, b any
	if json.Unmarshal(payloadA, &a) != nil || json.Unmarshal(payloadB, &b) != nil {
		return false
	}
	return reflect.DeepEqual(a, b)
}
