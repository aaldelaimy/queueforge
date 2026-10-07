-- W3C traceparent of the request that submitted the job. The worker reads it
-- so its spans join the API request's trace. Empty when tracing is off.
ALTER TABLE jobs ADD COLUMN trace_parent text NOT NULL DEFAULT '';
