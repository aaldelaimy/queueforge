package worker

import (
	"errors"
	"math/rand/v2"
	"time"
)

// Backoff decides how long a failed job waits before its next attempt.
// The ceiling doubles with each attempt (Base, 2*Base, 4*Base, ...) up to Max.
type Backoff struct {
	Base time.Duration
	Max  time.Duration
}

func DefaultBackoff() Backoff {
	return Backoff{Base: time.Second, Max: 30 * time.Second}
}

// Delay returns the wait after attempt (1-based): random in [ceiling/2,
// ceiling] ("equal jitter"), so jobs that failed together spread out.
func (b Backoff) Delay(attempt int) time.Duration {
	ceiling := b.Base
	for i := 1; i < attempt && ceiling < b.Max; i++ {
		ceiling *= 2 // stops at Max, so it can't overflow
	}
	ceiling = min(ceiling, b.Max)

	half := ceiling / 2
	return half + rand.N(ceiling-half+1)
}

// Permanent marks err as not worth retrying. Unmarked errors are retried
// while attempts remain.
func Permanent(err error) error {
	return permanentError{err}
}

type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

func isPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}
