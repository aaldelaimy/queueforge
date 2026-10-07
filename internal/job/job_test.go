package job

import (
	"encoding/json"
	"testing"
)

// The allowed set is written out again here on purpose: the test is the
// spec, and it should fail if someone edits the transitions map by accident.
func TestCanTransitionTo(t *testing.T) {
	all := []Status{StatusQueued, StatusRunning, StatusRetrying, StatusCompleted, StatusFailed, StatusDeadLettered, Status("bogus")}
	allowed := map[[2]Status]bool{
		{StatusQueued, StatusRunning}:       true,
		{StatusRunning, StatusCompleted}:    true,
		{StatusRunning, StatusFailed}:       true,
		{StatusRunning, StatusRetrying}:     true,
		{StatusRunning, StatusDeadLettered}: true,
		{StatusRetrying, StatusQueued}:      true,
		{StatusFailed, StatusQueued}:        true,
		{StatusDeadLettered, StatusQueued}:  true,
	}

	for _, from := range all {
		for _, to := range all {
			want := allowed[[2]Status{from, to}]
			if got := from.CanTransitionTo(to); got != want {
				t.Errorf("%s -> %s: got %v, want %v", from, to, got, want)
			}
		}
	}
}

func TestParseStatus(t *testing.T) {
	if _, ok := ParseStatus(""); ok {
		t.Error("empty string is not a status")
	}
	if _, ok := ParseStatus("bogus"); ok {
		t.Error("bogus accepted")
	}
	if got, ok := ParseStatus("dead_lettered"); !ok || got != StatusDeadLettered {
		t.Errorf("got %q, %v", got, ok)
	}
}

func TestSameRequestComparesJSONByMeaning(t *testing.T) {
	if !sameRequest("hash", json.RawMessage(`{"a":1,"b":2}`), "hash", json.RawMessage(`{"b":2,"a":1}`)) {
		t.Error("equivalent JSON should match")
	}
	if sameRequest("hash", json.RawMessage(`{"a":1}`), "hash", json.RawMessage(`{"a":2}`)) {
		t.Error("different payloads should not match")
	}
	if sameRequest("hash", nil, "sleep", nil) {
		t.Error("different types should not match")
	}
}
