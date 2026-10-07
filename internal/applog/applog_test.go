package applog

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestJSONLineHasRequiredFields(t *testing.T) {
	var buf bytes.Buffer
	log := New(&buf, "api")
	log.Info("hello", AttrEvent, "test.event", AttrJobID, "job-1")

	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, buf.String())
	}
	for _, key := range []string{"timestamp", "level", "msg", "service", "event", "job_id"} {
		if line[key] == nil || line[key] == "" {
			t.Errorf("missing %s in %v", key, line)
		}
	}
	if line["service"] != "api" {
		t.Errorf("service: %v", line["service"])
	}
	if line["event"] != "test.event" {
		t.Errorf("event: %v", line["event"])
	}
	if _, err := time.Parse(time.RFC3339Nano, line["timestamp"].(string)); err != nil {
		t.Errorf("timestamp %q: %v", line["timestamp"], err)
	}
}
