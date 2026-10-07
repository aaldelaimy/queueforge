package job

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
)

// MemoryStore is an in-memory Store for tests. Methods return copies.
type MemoryStore struct {
	mu   sync.RWMutex
	jobs map[string]*Job
	keys map[string]string // idempotency key -> job ID
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{jobs: make(map[string]*Job), keys: make(map[string]string)}
}

func (s *MemoryStore) Create(_ context.Context, n NewJob) (Job, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if n.IdempotencyKey != "" {
		if id, ok := s.keys[n.IdempotencyKey]; ok {
			j := s.jobs[id]
			if !sameRequest(j.Type, j.Payload, n.Type, n.Payload) {
				return Job{}, false, fmt.Errorf("%w: %q", ErrIdempotencyConflict, n.IdempotencyKey)
			}
			return j.snapshot(), false, nil
		}
	}

	now := time.Now().UTC()
	j := &Job{
		ID:             rand.Text(),
		Type:           n.Type,
		Payload:        slices.Clone(n.Payload),
		Status:         StatusQueued,
		MaxAttempts:    DefaultMaxAttempts,
		CreatedAt:      now,
		UpdatedAt:      now,
		EnqueuedAt:     now,
		IdempotencyKey: n.IdempotencyKey,
		TraceParent:    n.TraceParent,
	}
	s.jobs[j.ID] = j
	if n.IdempotencyKey != "" {
		s.keys[n.IdempotencyKey] = j.ID
	}
	return j.snapshot(), true, nil
}

func (s *MemoryStore) Get(_ context.Context, id string) (Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	j, ok := s.jobs[id]
	if !ok {
		return Job{}, ErrNotFound
	}
	return j.snapshot(), nil
}

func (s *MemoryStore) List(_ context.Context, status Status) ([]Job, error) {
	s.mu.RLock()
	out := make([]Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		if status != "" && j.Status != status {
			continue
		}
		out = append(out, j.snapshot())
	}
	s.mu.RUnlock()

	slices.SortFunc(out, func(a, b Job) int {
		if c := a.CreatedAt.Compare(b.CreatedAt); c != 0 {
			return c
		}
		return strings.Compare(a.ID, b.ID)
	})
	return out, nil
}

func (s *MemoryStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	if j, ok := s.jobs[id]; ok && j.IdempotencyKey != "" {
		delete(s.keys, j.IdempotencyKey)
	}
	delete(s.jobs, id)
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) Claim(_ context.Context, id string, lease time.Duration) (Job, error) {
	return s.transition(id, StatusRunning, func(j *Job, now time.Time) {
		j.Attempts++
		j.LeaseExpiresAt = now.Add(lease)
	})
}

func (s *MemoryStore) Complete(_ context.Context, id string, result json.RawMessage) (Job, error) {
	return s.transition(id, StatusCompleted, func(j *Job, _ time.Time) {
		j.Result = slices.Clone(result)
		j.Error = ""
		j.LeaseExpiresAt = time.Time{}
		j.RunAfter = time.Time{}
	})
}

func (s *MemoryStore) Fail(_ context.Context, id string, reason string) (Job, error) {
	return s.transition(id, StatusFailed, func(j *Job, _ time.Time) {
		j.Error = reason
		j.LeaseExpiresAt = time.Time{}
	})
}

func (s *MemoryStore) DeadLetter(_ context.Context, id string, reason string) (Job, error) {
	return s.transition(id, StatusDeadLettered, func(j *Job, _ time.Time) {
		j.Error = reason
		j.LeaseExpiresAt = time.Time{}
	})
}

func (s *MemoryStore) Reopen(_ context.Context, id string) (Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return Job{}, ErrNotFound
	}
	if j.Status != StatusFailed && j.Status != StatusDeadLettered {
		return Job{}, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, j.Status, StatusQueued)
	}
	now := time.Now().UTC()
	j.Status = StatusQueued
	j.Attempts = 0
	j.EnqueuedAt = now
	j.LeaseExpiresAt = time.Time{}
	j.RunAfter = time.Time{}
	j.UpdatedAt = now
	return j.snapshot(), nil
}

func (s *MemoryStore) Retry(_ context.Context, id string, reason string, delay time.Duration) (Job, error) {
	return s.transition(id, StatusRetrying, func(j *Job, now time.Time) {
		j.Error = reason
		j.LeaseExpiresAt = time.Time{}
		j.RunAfter = now.Add(delay)
	})
}

func (s *MemoryStore) PromoteDue(_ context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	var ids []string
	for _, j := range s.jobs {
		if j.Status == StatusRetrying && !j.RunAfter.After(now) {
			j.Status = StatusQueued
			j.RunAfter = time.Time{}
			j.EnqueuedAt = now
			j.UpdatedAt = now
			ids = append(ids, j.ID)
		}
	}
	return ids, nil
}

func (s *MemoryStore) ExpiredLeases(_ context.Context) ([]Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now().UTC()
	var out []Job
	for _, j := range s.jobs {
		if j.Status == StatusRunning && j.LeaseExpiresAt.Before(now) {
			out = append(out, j.snapshot())
		}
	}
	return out, nil
}

func (s *MemoryStore) RequeueStale(_ context.Context, olderThan time.Duration) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	var ids []string
	for _, j := range s.jobs {
		if j.Status == StatusQueued && j.EnqueuedAt.Before(now.Add(-olderThan)) {
			j.EnqueuedAt = now
			ids = append(ids, j.ID)
		}
	}
	return ids, nil
}

// transition checks and applies a status change under one lock.
func (s *MemoryStore) transition(id string, next Status, apply func(j *Job, now time.Time)) (Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return Job{}, ErrNotFound
	}
	if !j.Status.CanTransitionTo(next) {
		return Job{}, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, j.Status, next)
	}

	now := time.Now().UTC()
	j.Status = next
	apply(j, now)
	j.UpdatedAt = now
	return j.snapshot(), nil
}

// snapshot deep-copies j so callers don't share its Payload/Result slices.
func (j *Job) snapshot() Job {
	c := *j
	c.Payload = slices.Clone(j.Payload)
	c.Result = slices.Clone(j.Result)
	return c
}
