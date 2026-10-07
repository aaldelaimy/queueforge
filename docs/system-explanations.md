# QueueForge - System Explanations

## Part 0: Core Concepts

An asynchronous job system separates accepting work from doing it. The
client asks for something to be done and immediately gets back a receipt,
which is a job ID. The work happens later, in a different process. The client
uses the receipt to check the status and get the result.

Why not just do the work inside the HTTP request? Because slow work inside a
request causes four problems. The client and every proxy in between have to
wait, and may time out. A client that timed out doesn't know whether the work
happened, so it retries and the work runs twice. Every waiting request holds
a connection, so slow work makes every endpoint slow under load. And a crash
or deploy kills the work in progress with no record that it was ever started.
Splitting a fast API (accepts) from slow workers (executes) fixes all four,
and each side can be scaled on its own.

QueueForge has five runtime pieces, each with one job. The API validates a
request, records the job, hands off its ID, and answers 202. PostgreSQL is the
source of truth for every job: status, attempts, result, error. Redis is a
list of job IDs waiting to run, so workers find out about new work instantly.
Workers pop an ID, claim the job, run it, and record the outcome. The sweeper,
which runs inside every worker process, repairs whatever the fast path missed:
jobs whose worker died, retries that are due, and IDs that got lost.

The design rule that everything else follows: any state that matters lives in
Postgres, and anything the fast path loses can be rebuilt from Postgres. Redis
carries only IDs. If Redis lost all its data, nothing permanent would be lost,
because Postgres still knows which jobs are queued.

A queue's delivery guarantee depends on when a message is removed. If it's
removed before processing, a crash loses it: at-most-once. If it's removed
only after processing, a crash means it runs again: at-least-once.
Exactly-once is not achievable end to end, because a crash can always land
between "did the work" and "recorded that I did the work". Real systems use
at-least-once delivery plus idempotent processing, sometimes called
"effectively once". QueueForge does the same.

The jobs themselves are deliberately trivial: hash a string, sleep, fail on
purpose. This project runs locally with Docker Compose. It is not deployed and
serves no real traffic. The concepts are real; the scale is a laptop.

---

## Part 1: Go Code Conventions

The codebase uses standard Go throughout. These are the patterns that appear
in almost every package.

Code is organized in packages, one directory per package. Capitalized names
are exported (`job.Store`, `worker.New`); lowercase names are private to the
package (`transitions`, `recordFailure`). Everything lives under `internal/`,
which means only this module can import it.

Structs carry JSON tags in backticks. `json:"job_id"` renames the field in
JSON, `omitempty` drops empty values, and `json:"-"` never sends the field at
all. That's how the API hides internal bookkeeping like `IdempotencyKey` and
`TraceParent`. `json.RawMessage` holds raw, unparsed JSON bytes, which is how
payloads pass through the API without the API understanding them.

`Status` is a named type: a string underneath, but the compiler won't let you
pass any random string where a `Status` is expected. Methods have a receiver
in parentheses before the name. `func (s *PostgresStore) Claim(...)` is a
method on a pointer, so it works on the shared struct, not a copy.

Errors are ordinary return values. Nearly every function returns
`(value, error)` and `nil` means success. `fmt.Errorf("...: %w", err)` wraps an
error to add context while keeping the original in a chain. `errors.Is` asks
"is this specific error anywhere in the chain?" and is used for sentinel errors
like `job.ErrNotFound`. `errors.As` asks "is there an error of this type in the
chain?" and is used to detect permanent errors.

Interfaces are satisfied implicitly. There's no `implements` keyword: any type
with the right methods fits. The API only needs `Enqueue`, so it defines a tiny
`Enqueuer` interface, and both the real Redis queue and an in-memory queue
satisfy it. That's how the API is tested without Redis. Interfaces are defined
where they're used and kept small.

`context.Context` is the first parameter of almost every function that does
I/O. It carries a cancellation signal and a deadline. `context.WithTimeout`
gives a job attempt a deadline. `context.WithoutCancel` makes a context that
ignores cancellation, used when a result must be written even though shutdown
has started. `signal.NotifyContext` makes a context that's cancelled on SIGTERM.

Goroutines are lightweight threads. The worker's `main` starts several with
`wg.Go(...)` and waits for them with `wg.Wait()`. `select` waits for whichever
channel is ready first, which is how the sleep handler stops early when its
context is cancelled.

`defer` runs code when the function exits. `recover()` inside a deferred
function stops a panic from crashing the process. Because `Worker.run` uses a
named return value `err`, the deferred function can replace it with a
permanent error after a panic.

Database calls use pgx with `$1, $2` parameters. Values are sent separately
from the SQL text, so user input can never change the query. That's what
prevents SQL injection.

---

## Part 2: Job Domain and State Machine

A state machine is a fixed set of states plus a fixed list of allowed moves
between them. Anything not on the list is an error. The alternative is a
status string anyone can set to anything, which lets bugs and races put
records into states that make no sense.

QueueForge needs this because many actors change a job at the same time. Two
workers can receive the same ID. The sweeper scans for stuck jobs. A human
hits "retry". Without enforced transitions, a late duplicate could move a
completed job back to running and run it again. The state machine turns those
races into rejected operations.

There are six statuses. `queued` means ready to run now. `running` means a
worker holds a lease on it. `retrying` means it failed transiently and is
waiting for its backoff to pass. `completed` is done, and it's the only status
that can never change again. `failed` means a permanent error like a bad
payload. `dead_lettered` means it failed transiently three times and the
system gave up.

The allowed moves are one Go map in `internal/job/job.go`. `queued` can go to
`running`. `running` can go to `completed`, `failed`, `retrying`, or
`dead_lettered`. `retrying`, `failed`, and `dead_lettered` can go back to
`queued`. `completed` has no entry, so nothing can ever leave it.

The status only changes through store methods, never by writing the field
directly. Each method names one target status: `Claim` goes to running,
`Complete` to completed, `Retry` to retrying, and so on. The API calls
`Create` and `Reopen`. Workers call `Claim`, `Complete`, `Fail`, `DeadLetter`,
and `Retry`. The sweeper calls `PromoteDue` and, for abandoned jobs, `Retry`
or `DeadLetter`.

Why is `retrying` its own status instead of "queued with a run_after time"?
Because then every claim would have to check the clock. A separate state keeps
"ready now" and "waiting" apart. Why separate `failed` and `dead_lettered`?
Because they mean different things to an operator. `failed` means "this will
never work as is, fix the input or the code". `dead_lettered` means "we gave
up after trying, maybe the dependency is back now".

Even raw SQL can't store an impossible status: the `status` column has a
CHECK constraint listing the six values. That protects the data from every
client, not just the Go code.

---

## Part 3: PostgreSQL Store

A relational database stores data durably. A committed write survives a crash
because it's written to the write-ahead log on disk first. It also offers
atomic statements, transactions, and constraints that reject invalid data. A
connection pool reuses a few connections across many concurrent requests.

QueueForge has one table, `jobs`, built by five migration files. `db.Migrate`
runs at startup in both the API and the worker. It applies unapplied files in
order inside one transaction, after taking a Postgres advisory lock so two
processes starting at once don't both apply the same migration. If a migration
fails, the whole transaction rolls back and the process exits. Postgres
supports transactional schema changes, so nothing is ever half-applied.

The most important code in the project is `PostgresStore.transition`. Every
state change is a single conditional UPDATE:

```sql
UPDATE jobs SET status = $2, updated_at = now(), <extra columns>
WHERE id = $1 AND status = ANY($3)
RETURNING <all columns>
```

`$3` is the list of statuses allowed before the new one. It's computed by
`statusesBefore`, which reads the same Go `transitions` map. So the Go code
and the SQL can never disagree about the rules.

Why does this make exactly one of 50 concurrent claims win? Postgres locks
the row for the first UPDATE. The other 49 wait. When the first commits, each
waiting UPDATE re-checks its WHERE clause against the new row version, sees
`running` instead of `queued`, and matches nothing. The default isolation
level, READ COMMITTED, is enough because of this re-check.

The naive version, SELECT the status, check it in Go, then UPDATE, was
measured during development: 48 to 50 of 50 concurrent claims won against
Postgres. In memory the gap between check and write is nanoseconds, so it
almost never failed there. Over the network the gap is a full round trip, so
all 50 read "queued" before any write landed. One statement closes the gap.

If an UPDATE returns no row, a second query reads the current status only to
build the right error (`ErrNotFound` or `ErrInvalidTransition`). The decision
was already made atomically.

The database computes every timestamp with `now()`, including
`now() + lease`. That way the API, the workers, and the sweeper share one
clock and can't disagree because of clock skew.

There are no indexes beyond the primary key and the idempotency key. The
sweeper's queries scan the table every second per worker process. That was
fine at the measured scale (up to about 900k rows during load tests), but a
long-lived system would add partial indexes, for example on `enqueued_at`
where `status = 'queued'`.

`jsonb` validates JSON but reformats it (it reorders keys and removes
spacing). So when comparing payloads for idempotency, the code compares
meaning, not bytes.

---

## Part 4: API Request Flow

The API is the front door, and its whole job is: check the request, record it
durably, hand it off, answer. It must stay fast no matter how slow the jobs are.

| Route | Success | Errors |
|---|---|---|
| `POST /jobs` | 202 + `Location` | 400 bad input, 409 key conflict, 413 too large, 500 DB, 503 Redis |
| `GET /jobs/{id}` | 200 | 404, 500 |
| `GET /jobs?status=` | 200 `{"jobs":[...]}` | 400 unknown status, 500 |
| `POST /jobs/{id}/retry` | 202 | 404, 409 wrong state, 500 |
| `GET /health`, `GET /ready` | 200 | 503 (ready only) |
| `GET /metrics` | 200 | |

Every request first goes through `withRequestLog`, the middleware. It takes
the client's `X-Request-ID` or generates one, continues the caller's trace if
it sent a `traceparent` header, and puts a logger carrying the request ID and
trace ID into the request context. It wraps the `ResponseWriter` in a
`statusWriter`, because Go's `ResponseWriter` doesn't let you read back the
status code afterwards. When the handler returns, it records the HTTP latency
histogram labelled by route pattern (never the raw URL) and writes one
`http.request` log line.

`createJob` then validates. The body is capped at 1 MiB (413 if larger). The
JSON decoder rejects unknown fields. The type must be one of the worker's job
types. The idempotency key is trimmed and limited to 256 characters. Any
problem is a 400.

The payload is not validated in the API. Each worker handler owns its own
payload format, which keeps the API generic. The cost is that a bad payload
is accepted and then fails in the worker as `failed`, which the client can see.

Next, `store.Create` inserts the row as `queued`. Then `queue.Enqueue` pushes
the ID to Redis. The order matters: if the ID reached Redis first, a fast
worker could pop it and try to claim a row that doesn't exist yet.

If the push fails because Redis is down, the handler deletes the row it just
wrote and answers 503 with `Retry-After: 1`. The delete uses
`context.WithoutCancel` so it still runs if the client has gone. Why delete
instead of leaving the job queued? Because the client was told to retry, and
leaving the row would create a duplicate when it does. Deleting also frees the
idempotency key, so the retry creates the job properly. If even the delete
fails, the job sits `queued` with no ID in Redis, and the sweeper pushes it
later. Nothing is lost either way.

Finally the handler bumps the submitted counter, logs `job.created`, and
returns 202 with the job. 202 means "accepted for processing, not done", which
is exactly the async contract.

Error details are never sent to the client. A database error is logged and
the client gets `500 internal error`. "Job not found" (404) is kept separate
from "database down" (500) on purpose: a client that sees 404 might decide
the job doesn't exist and resubmit, which during an outage would create
duplicates.

`http.Server` is configured with `ReadHeaderTimeout: 5s`, which stops a slow
client from holding a connection open forever while trickling in headers
(the "slowloris" attack).

---

## Part 5: Redis Queue

The queue lets workers learn instantly that there's work, without polling the
database, and it lets the API answer without waiting for a worker.

It's one Redis list, `queueforge:jobs`. The API does `LPUSH`, which adds to
the head. Workers do `BRPOP` with a 1 second timeout, which blocks until
something is at the tail, then removes and returns it. Together that's FIFO.
The 1 second timeout bounds how long a worker takes to notice shutdown.

The list holds job IDs only. The worker always reads the current row from
Postgres before acting, so it can never act on a stale copy, and losing Redis
loses no information.

`BRPOP` removes the ID as it hands it over. So Redis on its own is
at-most-once: if a worker dies between the pop and the claim, the ID is gone.
The system is at-least-once anyway, because Postgres knows the job is still
`queued`, and the sweeper re-pushes IDs that went missing. Duplicates are
expected and handled by the atomic claim.

Several designs were considered. A list with `BLMOVE` into a per-worker
processing list would be at-least-once inside Redis, but needs a reaper for
dead workers' lists. Redis Streams with consumer groups have acks built in,
but bring more concepts. Postgres-only with `SELECT ... FOR UPDATE SKIP LOCKED`
works too, at the cost of workers polling the database.

The key insight is that no Redis design can find a job whose ID never reached
Redis, for example when the API dies between the INSERT and the LPUSH. Only a
scan of Postgres can find that. Once that scan exists, Redis-side acks add
little, so Redis is kept as simple as possible. At this scale a
Postgres-only queue would also work, with one less system to run.

Redis runs with `--appendonly yes`, which logs every write to disk, so a
restart keeps the list. That isn't needed for correctness. It just makes
recovery immediate instead of taking 30 seconds.

`Redis.Len` (`LLEN`) is used in two places: the queue-depth metric, and the
sweeper's backlog check. The `Memory` queue is a buffered channel with the
same behavior, used in unit tests.

---

## Part 6: Workers

A worker is a long-running process that pulls work and executes it outside
any request. Workers absorb the slow and failure-prone part of the system.
Because they're separate processes, they can be scaled, restarted, or crash
without touching the API.

`cmd/worker/main.go` starts one sweeper goroutine and `-concurrency`
goroutines (4 in Compose), each running a loop. Compose runs two worker
replicas, so 8 jobs can run at once. Each goroutine handles one job at a time:
pop an ID, claim it, run it, record the outcome, repeat. That gives natural
backpressure. A busy worker never pops more IDs than it can run.

The claim comes first. If it fails because the job is already running or
done, the worker logs `job.skipped` and does nothing else. That's the
duplicate guard.

After a successful claim, the worker starts a `job.process` span as a child
of the API's request span, then runs the handler. `Worker.run` wraps the
attempt in `context.WithTimeout` (15 s in Compose, 90 s default) and a deferred
`recover()` that turns a panic into a permanent error instead of crashing the
process. A timeout is wrapped as "timed out after 15s".

The outcome is recorded with `context.WithoutCancel`. If shutdown started
while the job was running, the result is still written. Otherwise a finished
job would sit `running` until its lease expired and then run again.

If `Dequeue` fails (Redis is down), the worker logs `queue.dequeue_failed`
and waits 1 second before trying again, so an outage doesn't become a tight
loop of failing calls and log spam.

There are four job types, all deliberately trivial. `hash` returns the
SHA-256 of its input and stands in for CPU work. `sleep` waits N milliseconds
(up to 60 s) while honoring cancellation and stands in for I/O. `fail` always
fails transiently. `flaky` fails N times, then succeeds.

A handler that ignores its context can't be stopped, because Go has no way to
kill a goroutine. It holds its slot until it returns and may outlive its
lease. That's why handlers must honor `ctx`.

How many goroutines? It depends on the job. With 100 ms jobs and 8 slots,
the maximum is 80 jobs per second. With ~1 ms jobs, a single goroutine was
measured at about 2,900 jobs per second, limited by network round trips
rather than CPU.

---

## Part 7: Retries and Backoff

A transient failure might succeed if tried again later: a timeout, a
dependency that's briefly down. A permanent failure fails the same way every
time: bad input, an unknown type, a code bug triggered by this input.
Retrying transient failures turns blips into successes. Not retrying
permanent ones avoids wasted work and gets an answer to the client sooner.

After a failed attempt, `recordFailure` in `internal/worker/worker.go`
decides in this order. If the error is permanent, the job becomes `failed`.
If the job has used all its attempts, it becomes `dead_lettered`. Otherwise
it becomes `retrying` with `run_after = now + delay`. Permanent is checked
first, because a permanent error should never be retried, no matter how many
attempts are left.

An error is permanent if it was wrapped with `Permanent(...)`: bad payloads,
unknown job types, and handler panics (the same input would most likely panic
again). `isPermanent` uses `errors.As`, so it still works after the error has
been wrapped further. Everything else is transient, including timeouts,
cancellations, and abandoned leases.

The delay comes from `Backoff.Delay`. Exponential backoff waits longer after
each failure (1 s, 2 s, 4 s, capped at 30 s) so a struggling dependency gets
breathing room. Jitter randomizes the wait so jobs that failed together don't
all retry at the same instant. QueueForge uses "equal jitter": a random wait
between half the ceiling and the full ceiling. Full jitter (anywhere from 0
to the ceiling) spreads load best, but can retry almost instantly. Equal
jitter guarantees at least half the ceiling.

Without backoff and jitter you get a retry storm: many jobs retrying
immediately multiply the load exactly when a dependency is weakest, which
can keep it down.

A `retrying` job doesn't sit in Redis. The sweeper's `PromoteDue` runs every
second, moves due jobs back to `queued`, and pushes their IDs. The next claim
makes it attempt 2.

The limit is 3 attempts, a column default. Delays are short (0.5 to 2
seconds) so tests and demos are quick. A real system would choose larger
delays and set limits per job type.

---

## Part 8: Idempotency

An operation is idempotent if doing it twice has the same effect as doing it
once. At-least-once delivery guarantees duplicates will sometimes happen, so
the system needs a defense for each kind.

Duplicate HTTP submissions happen when a client times out and retries, or a
user double-clicks. The defense is the `Idempotency-Key` header. `Create`
runs `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING RETURNING ...`. If
no row comes back, someone already stored this key, so the code loads that job
and compares type and payload by meaning (`sameRequest`). Same request: return
the original job and don't enqueue again. Different request: 409 Conflict.

The UNIQUE constraint is the lock. Doing it in Go (SELECT, then INSERT if
absent) would let two concurrent requests both see "absent" and both insert.
The contract test sends 50 concurrent creates with one key against real
Postgres and gets exactly one job.

The key column is nullable, and NULLs never collide, so requests without a
key are always independent jobs. Keys are never shown in responses.

Duplicate queue deliveries happen when the same ID is pushed twice, by the
sweeper or by hand. The defense is the atomic claim. The second delivery finds
the job `running` or `completed`, matches no row, and is skipped.

Duplicate executions are the case that isn't prevented. If a worker dies after
doing the work but before recording it, the lease expires and the job runs
again. The claim prevents concurrent duplicates, not re-execution after a
crash. The demo handlers are naturally idempotent (hashing the same input twice
is harmless). A real "send email" handler would need its own protection, for
example passing the job ID as an idempotency key to the email provider.

Why not exactly-once? The work and the record of the work happen in
different places. A crash between them forces a choice between "maybe twice"
and "maybe never". QueueForge chooses "maybe twice" plus idempotent handlers.

---

## Part 9: Dead-Letter Handling

A dead letter is a message the system has given up on after retrying.
Instead of retrying forever (wasting work and hiding the problem) or deleting
it (losing it), the system parks it where a human can inspect and re-drive it.

In QueueForge, `dead_lettered` is a status, not a separate queue. The
dead-letter "queue" is just a query: `GET /jobs?status=dead_lettered`.
Postgres already has the ID, type, payload, attempts, and last error.
Operators need to query and filter failed work ("all dead-lettered jobs of
type X since 3 pm"), not pop it off a list, so a Redis list would be the
wrong shape.

Re-driving is `POST /jobs/{id}/retry`, which calls `Reopen`. That's a
conditional UPDATE from `failed` or `dead_lettered` back to `queued`, with
`attempts` reset to 0 so the job gets a full new budget. Any other status
gets 409. The old `error` is kept until the job succeeds, as history. If the
enqueue after reopening fails, the job is already `queued` in Postgres, so the
sweeper pushes it later.

Humans can reopen `failed` jobs too, because after a code fix the same input
can succeed. Automatic retries never reopen `failed`.

An abandoned lease counts as an attempt. So a "poison" job that crashes every
worker that touches it is dead-lettered after 3 crashes instead of looping forever.

Reopening without fixing the cause just repeats the failure: the e2e test
reopens a `fail` job and watches it dead-letter again.

---

## Part 10: Leases, the Sweeper, and Graceful Shutdown

Processes die at the worst moments: between INSERT and LPUSH, between BRPOP
and Claim, halfway through a job. Deploys stop processes routinely. Every one
of those must end in a known state, not a job stuck forever.

A lease is a time-limited claim: "this job is mine until T". Claim sets
`lease_expires_at = now + 2 × job timeout`, which is 30 seconds in Compose.
Twice the timeout means a live worker always finishes or times out before the
lease ends. If the worker dies, the lease just runs out. Nothing has to detect
the death directly.

The sweeper is a reconciliation loop: it compares what Postgres says should be
happening with reality and repairs the difference. Kubernetes controllers work
the same way. One runs in every worker process, once a second, with three steps.

Step one, `ExpiredLeases`, finds `running` jobs past their lease. Their worker
is presumed dead. The sweeper records a failed attempt with the error
"abandoned: worker did not finish before its lease expired". Like any
transient failure, that leads to `retrying` or, on the last attempt,
`dead_lettered`.

Step two, `PromoteDue`, finds `retrying` jobs whose `run_after` has passed,
moves them to `queued`, and pushes their IDs.

Step three, `RequeueStale`, finds `queued` jobs whose ID was probably lost and
pushes them again. The rule is "queued for more than 30 seconds, but only if
the queue is empty". With nothing in Redis, an old queued job can't be
waiting in line, so its ID really is missing. If there's a backlog, the limit
becomes 10 minutes, as a backstop. That queue-empty condition was added after
a load test showed that age alone is a bad signal during a backlog (Part 18).

It's safe to run a sweeper in every worker because each step is a single
conditional UPDATE. Each row is returned to exactly one sweeper, and for
abandoned jobs only one sweeper's update wins. No leader election needed.

If the sweeper's push fails, the job stays `queued` in Postgres and a later
sweep tries again. Nothing is lost.

Graceful shutdown means: on SIGTERM, stop taking new work, finish what's in
progress within a limit, then exit. The worker uses two contexts. `stopCtx`
is cancelled by SIGTERM and ends the dequeue loops. `jobsCtx` is the parent of
every running attempt, and it's cancelled only after a 20 second grace period
(`time.AfterFunc(shutdownGrace, cancelJobs)`). With one context, SIGTERM would
cancel running jobs immediately. If the grace runs out, the job is cancelled
and recorded as a transient failure, so it's retried later. Docker's
`stop_grace_period` is 30 seconds, longer than the grace, so Docker never
SIGKILLs a worker mid-drain.

The API's shutdown first makes `/ready` return 503 so a load balancer stops
routing to it, waits `-drain-delay`, then calls `http.Server.Shutdown`, which
lets in-flight requests finish (up to 10 seconds), and finally flushes traces.

These were observed on the running stack. A worker killed with SIGKILL during
an 8 second job: the job stayed `running` for about 30 seconds, was logged as
`job.abandoned`, and completed on attempt 2 about 9 seconds later. Two workers
stopped with SIGTERM during 5 second jobs: each logged `worker.draining`, then
`job.completed`, then `worker.stopped`. The stop took 4 seconds, and both jobs
completed on attempt 1.

The tradeoff here is lease versus heartbeat. A heartbeat (the worker extends
its lease periodically) detects death faster and supports long jobs, but adds
a database write per job per interval. A fixed lease costs a recovery delay of
twice the timeout, and a handler that ignores cancellation past its lease can
overlap with its own retry.

---

## Part 11: Structured Logging

A log records an individual event. A structured log is machine-readable, here
one JSON object per line with stable field names, so you can filter on
`job_id = X` instead of grepping English sentences.

One job's life is spread across processes and time: an API request, a worker
attempt, a sweeper recovery, another attempt. Answering "what happened to job
X?" means finding every line about X across all of them.

`applog.New` returns a `log/slog` JSON logger that puts `service` on every
line and renames slog's `time` key to `timestamp`. The important fields are
`event` (a stable machine name like `job.created`, `job.completed`,
`job.skipped`, `job.abandoned`, `queue.dequeue_failed`), `request_id` (one
HTTP request), `job_id` (one job, across every process), `trace_id` (links
the line to its trace in Jaeger), and `error` (only when something failed).

Each line has both `event` and `msg`. You search on `event` and read `msg`.
Rewording a message doesn't break anything that searches for it.

In the API, the middleware builds a logger with the request ID and trace ID
and stores it in the request context. Handlers fetch it with `loggerFrom(r)`
and add `job_id`. The context key is an unexported type so no other package
can collide with it. In the worker, `process` uses a logger with `job_id` and
`trace_id`. Worker lines have no `request_id`, because the worker has no HTTP
request. The job ID and the trace ID connect them.

Payloads are never logged (they can be large or contain secrets), and neither
are `/metrics`, `/health`, or `/ready` requests, which are noise.

Logs go to stdout, and Docker collects them (`docker compose logs`). In a real
deployment stdout would go to a log collector and the app wouldn't change.

Logs answer "what happened to this job". They're the wrong tool for "how many
per minute", which is what metrics are for.

---

## Part 12: Metrics

Metrics are numbers aggregated over time: cheap to store and fast to query.
Prometheus scrapes (pulls) each process's `/metrics` page every 5 seconds.
There are three types. A counter only goes up (jobs completed). A gauge goes
up and down (queue depth). A histogram counts observations into buckets so
you can compute percentiles (request duration). Counters are graphed with
`rate(x[1m])`, per second, never as their raw value.

QueueForge exports counters for jobs submitted, started, completed, failed,
dead-lettered, retried, and stale re-pushes; a queue depth gauge; a histogram
of job attempt duration; and a histogram of HTTP request duration labelled by
method, route, and status code.

The queue depth gauge is exported only by the API, which polls `LLEN` once a
second. That was a real bug found during load testing: every process used to
export the gauge, so Prometheus showed three series, two of them always zero.
If several processes export the same Redis list's length, `sum()` counts it
several times. Now the gauge is only registered by the process that polls Redis.

HTTP latency is labelled with the route pattern (`GET /jobs/{id}`), never the
raw URL. A label with a job ID in it would create one time series per job and
eventually exhaust Prometheus's memory. That's called a cardinality
explosion. IDs belong in logs, not metric labels.

`Metrics` methods are nil-safe: calling them on a nil pointer does nothing.
That lets tests skip metrics. `New` registers on a dedicated registry, not the
global one, so tests don't share state.

The Grafana dashboard is organized around operational questions, one per
panel: Is the queue backing up? Can workers keep up? Are jobs failing, are
retries spiking? How long are jobs taking? Are requests getting slower? Is the
API returning errors?

The alert to add first: queue depth rising for 10 minutes, or the submitted
rate higher than the finished rate. Both mean workers can't keep up, and the
response is to add workers.

`failed` and `dead_lettered` have separate counters because they mean
different things and need different responses.

---

## Part 13: Tracing

A trace is the timeline of one piece of work across processes. It's a tree of
spans: named, timed operations with a parent. Context propagation passes the
trace ID and parent span ID between processes, normally in the W3C
`traceparent` header (`00-<trace id>-<parent span id>-<flags>`).

Logs say what happened, metrics say how much and how fast overall, and traces
say where the time went for this one request.

QueueForge uses the OpenTelemetry SDK and exports spans to Jaeger when
`OTEL_EXPORTER_OTLP_ENDPOINT` is set. Compose sets it. Otherwise tracing is a
no-op with almost no cost.

The API creates a `POST /jobs` span in the middleware (continuing the caller's
trace if it sent a `traceparent`), with children `store.create` and
`queue.enqueue`.

The interesting part is crossing the queue. Redis carries only an ID, so
there's no message header to put a `traceparent` in. Instead the API stores
the request span's traceparent on the job row (the `trace_parent` column).
After claiming, the worker rebuilds the context from it and starts
`job.process` as a child. Every attempt, including retries seconds later and
manual re-drives, appears as another `job.process` span in the same trace.

Why store it in Postgres rather than in the Redis message? A message format
would be needed instead of a bare ID, and IDs re-pushed by the sweeper would
lose it. The row always has it.

If Jaeger is down, the batch exporter drops spans and the app is unaffected.
That actually happened: during load tests Jaeger's unbounded in-memory store
was OOM-killed. Jobs kept flowing and only spans were lost. Compose now caps
Jaeger at 50,000 traces. At high volume you'd sample a fraction of traces
instead of tracing every job.

Every log line carries `trace_id`, so you can jump from a log line to its trace.

---

## Part 14: Health and Readiness

Orchestrators and load balancers ask two different questions. Liveness
(`/health`) asks "is this process broken beyond repair?" and a failure means
restart it. Readiness (`/ready`) asks "should it receive traffic right now?"
and a failure means stop routing to it, but don't restart it.

The difference matters during an outage. If liveness checked Postgres and
Postgres went down, the orchestrator would restart every API container. That
can't fix Postgres; it just adds a restart storm to the outage. Readiness
takes instances out of rotation instead, and they rejoin by themselves.

`/health` always returns 200: if the handler runs, the process works.
`/ready` pings Postgres and Redis, all under one 1 second timeout, so a hung
dependency fails the probe quickly instead of hanging it. It returns 200 with
each check's result, 503 if any check fails, or 503 `draining` during shutdown.

The API serves both on port 8080. The worker serves both, plus `/metrics`, on
port 9091. Workers pull work, so their readiness doesn't gate traffic, but it
still shows whether the worker can reach its dependencies.

`Drain()` is called first during API shutdown, so `/ready` fails before
connections stop.

Observed with Redis stopped: `/health` returned 200, `/ready` returned 503
with Redis failing, and after Redis came back `/ready` was 200 again with no
process restarted.

---

## Part 15: Docker and Compose

An image is a read-only package: a filesystem plus a start command. A
container is a running instance of an image. One worker image, two worker
containers. A Dockerfile is the recipe that builds an image, and each step is
a cached layer. Compose describes several containers and how they connect, so
`docker compose up` starts the whole system.

The Dockerfile is a multi-stage build. The build stage uses `golang:1.26`.
It runs `go mod download` before copying the source, so dependencies are
cached until `go.mod` or `go.sum` changes. Then it compiles both binaries with
`CGO_ENABLED=0`, producing static binaries that need no system libraries. The
final `api` and `worker` targets copy one binary each onto a distroless image:
no shell, no package manager, a non-root user, about 20 MB.

`docker-compose.yml` runs seven services: Postgres 17, Redis 7, the API, two
worker replicas, Prometheus, Grafana, and Jaeger. Compose puts them on one
network where they reach each other by service name, which is how
`DATABASE_URL` can say `postgres:5432`. Named volumes keep Postgres and Redis
data when containers are replaced. `docker compose down` keeps the volumes;
`docker compose down -v` deletes them and all jobs with them.

The API and workers start only after Postgres and Redis pass their
healthchecks (`depends_on: condition: service_healthy`). If a process fails
at startup anyway, `restart: unless-stopped` starts it again. The worker has no
`ports:` entry because nothing outside needs to reach it; Prometheus reaches
it on the internal network.

The worker's flags use `${VAR:-default}`, so experiments can change replicas,
concurrency, or the sweeper's backlog threshold from the shell without editing
the file.

Compose is right for one machine. Kubernetes solves scheduling across many
machines, which this project doesn't need. The credentials in the Compose file
are local-only throwaways.

---

## Part 16: Testing

Most of the interesting bugs in a system like this are concurrency and
failure bugs, which happy-path tests never trigger. So the test suite
deliberately creates races, crashes, duplicates, and outages.

Unit tests in every package use in-memory fakes (`job.MemoryStore` and
`queue.Memory`), so the API and worker tests run in milliseconds.

Contract tests keep those fakes honest. `testStoreContract` is a suite of 31
behaviors that runs against `MemoryStore` always and against `PostgresStore`
when `QUEUEFORGE_TEST_DATABASE_URL` is set. The queue has the same setup with
Redis. If the fake and the real store ever behave differently, a test fails.

Concurrency tests start 50 goroutines at once with a start barrier and repeat
for many trials. A concurrency test that never fails proves nothing. During
development, a single-trial claim test caught a deliberately broken
implementation 0 times out of 100 (12 out of 100 with `-race`). With a start
barrier and 200 trials, it caught it every time. Everything runs with `-race`
in CI.

Some tests prove behavior without relying on timing.
`TestWorkersRunJobsConcurrently` makes each job wait until all 4 are running,
so it can only pass if they really overlap.
`TestRequestReturnsBeforeWorkFinishes` proves POST returns while a job is
still running.

Failure tests use stub dependencies (`brokenQueue`, `brokenStore`) to cover
panicking handlers, queue errors, cancellation mid-job, abandoned leases, and
poison jobs.

End-to-end tests in `e2e/e2e_test.go` drive the running Compose stack over
HTTP. They're behind the `e2e` build tag so `go test ./...` stays fast and
needs no running services.

Integration tests truncate their tables, which is why they use a separate
`queueforge_test` database and Redis DB 15, never the live data.

---

## Part 17: CI

Continuous integration means every push is automatically built and tested.
Continuous delivery means every passing change can be released with one
action. Continuous deployment means every passing change is released
automatically. QueueForge has CI only. It's deliberately not deployed, to
avoid cloud costs, and an unused deploy pipeline would be code nobody runs.

`.github/workflows/ci.yml` runs on every push to `main` and every pull
request. The `test` job checks gofmt, runs `go vet`, and runs `go test -race`
with Postgres 17 and Redis 7 service containers, so the contract and queue
tests run against real backends every time. The `e2e` job then builds the
images, starts the Compose stack, waits for `/ready`, runs the e2e suite,
prints logs on failure, and tears everything down.

If it were deployed, the pipeline would build both images on merge and tag
them with the commit SHA (not `latest`, so you always know exactly what's
running and can roll back). An orchestrator would do a rolling deploy: start
new tasks, wait for `/ready`, send SIGTERM to the old ones (which drain
gracefully), and roll back if new tasks never become healthy. Migrations run
at startup, so they'd need to be backward compatible with the previous version
still running: add columns first, remove them in a later release.

---

## Part 18: Load Testing and Performance

Throughput is operations completed per second. Latency percentiles describe
how long requests took: p50 is the median, p95 means 95% were at least this
fast, p99 is the slowest 1%, the tail that unlucky users feel. Averages hide
the tail.

There are two ways to generate load. A closed model runs N users in a loop,
so it slows its sending when the server slows down, which hides problems. An
open model sends at a target arrival rate regardless, like real traffic. The
problem with closed models is called coordinated omission. QueueForge uses
k6's `ramping-arrival-rate` executor, which is an open model.

`loadtest/submit.js` ramps from peak/8 to peak/4, peak/2, peak, then holds
peak, about two minutes in all. Each iteration is a `POST /jobs` plus a
`GET /jobs/{id}`. `loadtest/run.sh` empties the jobs table and the queue,
runs k6 in Docker on the Compose network, waits for the backlog to drain, and
writes a report. It measures both sides of the queue: k6 for API latency,
Prometheus for worker throughput and queue depth, and Postgres for exact job
counts. The API can be fast while workers fall behind, and k6 alone would
never show that.

On an Apple M5 Pro laptop, with k6 on the same machine and the default 2
workers × 4 goroutines: at a 3,000 jobs/s peak, POST p50/p95/p99 was
1.00/1.67/2.44 ms with no backlog. At 6,000 jobs/s it was 1.16/2.10/3.22 ms,
0 errors, workers completing about 6,050 jobs/s, and a temporary backlog of
about 61,000 IDs that cleared 9 seconds after the load stopped.

The bottleneck analysis: workers were not CPU bound (each used about 55% of a
core). Each job costs a fixed set of network round trips: BRPOP, the Claim
UPDATE, and the Complete UPDATE. One goroutine alone did about 2,900 jobs/s.
Postgres was the busiest container, because every job is one INSERT and two
UPDATEs. At 10,000 jobs/s the whole laptop saturated, including k6 itself. One
of those runs stalled (a 2 minute test took 471 seconds) and was discarded.
Past that point the numbers describe the laptop, not QueueForge.

The load test found a real bug. During a 10,000 jobs/s run a backlog of about
210,000 IDs built up. The sweeper's lost-ID rule at the time was "queued for
more than 30 seconds means the ID was lost". During a backlog almost every job
waits longer than 30 seconds, so the sweeper re-pushed 151,286 IDs that weren't
lost, just waiting in line. Each became a duplicate delivery that failed its
claim and was skipped. Correctness held, because the claim meant no job ran
twice. But it wasted work at the worst moment and formed a feedback loop: a
longer backlog meant more "stale" jobs, which meant more duplicates, which
meant a longer backlog.

It was found by counting log events after the run. There was no metric for
re-pushes, so the dashboard couldn't have shown it.

The fix: the 30 second rule only applies when the queue is empty, with a
10-minute backstop during a backlog. A `stale_requeued_total` metric and
dashboard series were added so it's visible next time. A/B runs on the same
machine (the "before" run reproduces the old behavior with
`BACKLOG_REQUEUE_AFTER=30s`): with 100 ms jobs, duplicates went from 8,325 to 0
and peak queue depth from 9,516 to 5,800, while drain time stayed at 80 seconds
(skipping a duplicate costs ~1 ms against 100 ms of real work). With fast jobs
and one goroutine, duplicates went from 51,313 to 0 and drain time from 44 to
36 seconds. Those were one run each, so treat the drain difference as indicative.

The lessons: idempotency makes duplicates safe, not free. Recovery mechanisms
need their own load test. Every repair action should be a metric. The full
write-up is in the [postmortem](postmortems/2026-10-07-sweeper-duplicate-flood.md).

---

## Full System Summary

QueueForge is an asynchronous job-processing system built in Go. It has an
API, PostgreSQL as the source of truth, Redis as the queue, and worker
processes that each run several goroutines and a sweeper.

When a client submits a job, the API validates the request, inserts a row in
Postgres with status `queued`, pushes the job ID onto a Redis list, and
returns 202 with the ID. If the client sends an idempotency key, a unique
constraint makes sure a retried request returns the original job instead of
creating a second one. The row is always written before the ID is pushed, and
if the push fails the row is deleted and the client is told to retry.

Workers block on the Redis list and pop IDs. Before running anything, a
worker claims the job with a single conditional UPDATE that only succeeds if
the job is still `queued`. That's the guarantee that only one worker runs it,
even if the ID was delivered twice. The worker runs the handler with a
timeout, then records the outcome through a state machine enforced in SQL:
`completed`; `failed` for permanent errors; `retrying` with exponential
backoff and jitter for transient ones; `dead_lettered` after three attempts,
where a human can find it and re-drive it.

Redis pops are at-most-once, and writing to two stores can be interrupted
halfway. So a sweeper in every worker uses Postgres as the ledger. It retries
jobs whose lease expired because their worker died, promotes retries that are
due, and re-pushes IDs that got lost. That makes the whole system
at-least-once. A job can run twice if a worker dies after doing the work but
before recording it, so handlers must be idempotent. On a normal shutdown,
workers stop taking new jobs and finish the ones they're running.

Everything is observable. JSON logs carry request, job, and trace IDs.
Prometheus metrics and a Grafana dashboard show queue depth, throughput,
failures, and latency. A trace follows each job from the HTTP request through
every worker attempt, crossing the queue through a traceparent stored on the
job row. `/health` and `/ready` separate "restart me" from "don't send me
traffic".

The tests cover concurrency (50-goroutine race tests against real Postgres),
failures, and the full stack end to end, all in CI. Load testing on a laptop
showed POST p99 around 3 ms at 6,000 jobs/s and found a bug in the sweeper,
which was fixed and verified with A/B runs.

The limitations are that this runs locally with one Postgres and one Redis
and is not deployed. It's at-least-once, not exactly-once. There's no
authentication, pagination, key expiry, or per-query timeout, and leases
aren't extended for long jobs. These are deliberate tradeoffs for a system
built to demonstrate backend concepts rather than serve production traffic.
