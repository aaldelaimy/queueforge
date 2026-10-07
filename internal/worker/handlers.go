package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aaldelaimy/queueforge/internal/job"
)

// Handler runs one attempt of a job. Wrap errors with Permanent if retrying
// can't help. It must return promptly once ctx is cancelled.
type Handler func(ctx context.Context, j job.Job) (json.RawMessage, error)

const maxSleep = 60 * time.Second

// DefaultHandlers returns every job type QueueForge knows how to run.
func DefaultHandlers() map[string]Handler {
	return map[string]Handler{
		"hash":  hashHandler,
		"sleep": sleepHandler,
		"fail":  failHandler,
		"flaky": flakyHandler,
	}
}

// hashHandler returns the SHA-256 of payload.input.
func hashHandler(_ context.Context, j job.Job) (json.RawMessage, error) {
	var p struct {
		Input *string `json:"input"`
	}
	if err := json.Unmarshal(j.Payload, &p); err != nil || p.Input == nil {
		return nil, Permanent(errors.New(`payload must be {"input": "<string>"}`))
	}
	sum := sha256.Sum256([]byte(*p.Input))
	return json.Marshal(map[string]string{"sha256": hex.EncodeToString(sum[:])})
}

// sleepHandler waits payload.duration_ms, honoring cancellation.
func sleepHandler(ctx context.Context, j job.Job) (json.RawMessage, error) {
	var p struct {
		DurationMS int `json:"duration_ms"`
	}
	if err := json.Unmarshal(j.Payload, &p); err != nil || p.DurationMS <= 0 {
		return nil, Permanent(errors.New(`payload must be {"duration_ms": <positive integer>}`))
	}
	d := time.Duration(p.DurationMS) * time.Millisecond
	if d > maxSleep {
		return nil, Permanent(fmt.Errorf("duration_ms must be at most %d", maxSleep.Milliseconds()))
	}

	select {
	case <-time.After(d):
		return json.Marshal(map[string]int{"slept_ms": p.DurationMS})
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// failHandler always fails transiently, so the job ends up dead-lettered.
func failHandler(_ context.Context, _ job.Job) (json.RawMessage, error) {
	return nil, errors.New("simulated transient failure")
}

// flakyHandler fails the first payload.fail_times attempts, then succeeds.
func flakyHandler(_ context.Context, j job.Job) (json.RawMessage, error) {
	var p struct {
		FailTimes *int `json:"fail_times"`
	}
	if err := json.Unmarshal(j.Payload, &p); err != nil || p.FailTimes == nil || *p.FailTimes < 0 {
		return nil, Permanent(errors.New(`payload must be {"fail_times": <non-negative integer>}`))
	}
	if j.Attempts <= *p.FailTimes {
		return nil, fmt.Errorf("simulated transient failure on attempt %d", j.Attempts)
	}
	return json.Marshal(map[string]int{"succeeded_on_attempt": j.Attempts})
}
