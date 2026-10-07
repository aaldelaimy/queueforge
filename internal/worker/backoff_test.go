package worker

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestBackoffDelayStaysWithinBounds(t *testing.T) {
	b := Backoff{Base: 100 * time.Millisecond, Max: time.Second}
	ceilings := map[int]time.Duration{
		1: 100 * time.Millisecond,
		2: 200 * time.Millisecond,
		3: 400 * time.Millisecond,
		4: 800 * time.Millisecond,
		5: time.Second, // 1.6s capped
		6: time.Second,
	}
	for attempt, ceiling := range ceilings {
		for range 1000 {
			d := b.Delay(attempt)
			if d < ceiling/2 || d > ceiling {
				t.Fatalf("attempt %d: delay %v outside [%v, %v]", attempt, d, ceiling/2, ceiling)
			}
		}
	}
}

func TestBackoffIsJittered(t *testing.T) {
	b := Backoff{Base: time.Second, Max: time.Minute}
	seen := map[time.Duration]bool{}
	for range 100 {
		seen[b.Delay(3)] = true
	}
	if len(seen) < 50 {
		t.Errorf("only %d distinct delays in 100 samples; retries would bunch together", len(seen))
	}
}

func TestBackoffHugeAttemptDoesNotOverflow(t *testing.T) {
	b := Backoff{Base: time.Second, Max: time.Hour}
	if d := b.Delay(10_000); d < 30*time.Minute || d > time.Hour {
		t.Errorf("got %v, want between 30m and 1h", d)
	}
}

func TestPermanentErrorsAreDetectedThroughWrapping(t *testing.T) {
	base := errors.New("bad payload")
	wrapped := fmt.Errorf("job abc: %w", Permanent(base))

	if !isPermanent(wrapped) {
		t.Error("wrapped permanent error not detected")
	}
	if !errors.Is(wrapped, base) {
		t.Error("Permanent hides the original error from errors.Is")
	}
	if isPermanent(base) {
		t.Error("plain error reported as permanent")
	}
}
