package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aaldelaimy/queueforge/internal/job"
)

func withPayload(payload string) job.Job {
	return job.Job{Payload: json.RawMessage(payload), Attempts: 1}
}

func TestHashHandler(t *testing.T) {
	got, err := hashHandler(context.Background(), withPayload(`{"input":"hello"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Known SHA-256 of "hello".
	want := `{"sha256":"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"}`
	if string(got) != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestBadPayloadsArePermanentFailures(t *testing.T) {
	cases := map[string]struct {
		h       Handler
		payload string
	}{
		"hash empty":         {hashHandler, ``},
		"hash no input":      {hashHandler, `{}`},
		"hash wrong type":    {hashHandler, `{"input":5}`},
		"hash array":         {hashHandler, `[1]`},
		"sleep empty":        {sleepHandler, ``},
		"sleep zero":         {sleepHandler, `{"duration_ms":0}`},
		"sleep negative":     {sleepHandler, `{"duration_ms":-5}`},
		"sleep too long":     {sleepHandler, `{"duration_ms":600000}`},
		"flaky no fail_time": {flakyHandler, `{}`},
		"flaky negative":     {flakyHandler, `{"fail_times":-1}`},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := tc.h(context.Background(), withPayload(tc.payload))
			if err == nil || !isPermanent(err) {
				t.Errorf("got %v, want a permanent error", err)
			}
		})
	}
}

func TestSleepHandler(t *testing.T) {
	start := time.Now()
	got, err := sleepHandler(context.Background(), withPayload(`{"duration_ms":20}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Errorf("returned after %v, want at least 20ms", elapsed)
	}
	if string(got) != `{"slept_ms":20}` {
		t.Errorf("got %s", got)
	}
}

func TestSleepHandlerStopsWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	_, err := sleepHandler(ctx, withPayload(`{"duration_ms":10000}`))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("took %v to notice cancellation", elapsed)
	}
}

func TestFailHandlerIsTransient(t *testing.T) {
	_, err := failHandler(context.Background(), job.Job{})
	if err == nil || isPermanent(err) {
		t.Errorf("got %v, want a transient error", err)
	}
}

func TestFlakyHandler(t *testing.T) {
	j := withPayload(`{"fail_times":2}`)
	for attempt := 1; attempt <= 3; attempt++ {
		j.Attempts = attempt
		got, err := flakyHandler(context.Background(), j)
		if attempt <= 2 {
			if err == nil || isPermanent(err) {
				t.Errorf("attempt %d: got %v, want a transient error", attempt, err)
			}
			continue
		}
		if err != nil || string(got) != `{"succeeded_on_attempt":3}` {
			t.Errorf("attempt %d: got %s, %v", attempt, got, err)
		}
	}
}
