// Package applog builds the JSON logger shared by the API and worker.
package applog

import (
	"io"
	"log/slog"
	"os"
)

const (
	AttrService   = "service"
	AttrEvent     = "event"
	AttrRequestID = "request_id"
	AttrJobID     = "job_id"
	AttrError     = "error"
	AttrTraceID   = "trace_id"
)

// New returns a JSON logger that tags every line with service.
func New(w io.Writer, service string) *slog.Logger {
	if w == nil {
		w = os.Stdout
	}
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			// slog's default key is "time"; the rest of the project
			// calls it timestamp.
			if a.Key == slog.TimeKey {
				a.Key = "timestamp"
			}
			return a
		},
	})
	return slog.New(h).With(AttrService, service)
}
