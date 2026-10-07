# QueueForge - Design

## Overview

QueueForge is an asynchronous job-processing system that runs locally with
Docker Compose. It is not production software and it is not deployed. It
demonstrates backend concepts that appear in real job systems like Sidekiq,
Celery, and AWS SQS + Lambda: async APIs, at-least-once delivery, retries,
idempotency, dead-lettering, crash recovery, and observability.

The jobs are deliberately trivial (hash a string, sleep, fail on purpose).
The interesting part is the machinery around them.

```
client ──HTTP──▶ API ──INSERT──▶ PostgreSQL ◀──Claim/Complete/Retry── Workers (N × K goroutines)
                  └──LPUSH id──▶ Redis list ──BRPOP id──────────────▶   + sweeper (recovery)

Prometheus ◀─ scrape /metrics      Jaeger ◀─ OTLP spans      stdout ─ JSON logs
```

| Component | Does | Does NOT do |
|---|---|---|
| API (`cmd/api`) | Validate, insert the job row, push the ID, answer 202, serve job status | Run jobs |
| PostgreSQL | Source of truth for every job; enforces the state machine | Wake workers up |
| Redis | A list of job IDs so workers wake instantly | Hold anything that can't be rebuilt from Postgres |
| Worker (`cmd/worker`) | Claim a job, run it with a timeout, record the outcome | Accept HTTP submissions |
| Sweeper (inside each worker) | Every second: recover dead workers' jobs, promote due retries, re-push lost IDs | Run jobs |

## Submit Flow (`POST /jobs`)

1. Middleware assigns a request ID, starts a trace span, and builds a logger
2. Body is capped at 1 MiB and decoded strictly (unknown fields rejected)
3. Job type must be one the workers know; `Idempotency-Key` header ≤ 256 chars
4. INSERT the row as `queued`:
   - `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING`
   - If the key already exists with the same body: return the original job, don't enqueue again
   - If the key exists with a different body: 409
5. `LPUSH queueforge:jobs <id>` (only the ID goes to Redis)
   - If the push fails: delete the row and return 503 with `Retry-After: 1`
6. Return 202 with the job ID

The row is written before the ID is pushed. If it were the other way round,
a fast worker could pop an ID whose row doesn't exist yet.

The payload is not validated by the API. Each job handler owns its own
payload format, so a bad payload becomes a `failed` job in the worker.

## Processing Flow (worker)

1. `BRPOP queueforge:jobs` with a 1 s timeout
2. Claim the job in Postgres:
   - `UPDATE ... SET status='running', attempts=attempts+1, lease_expires_at=now()+2×timeout WHERE id=$1 AND status='queued'`
   - If no row matches (duplicate delivery, already done): log `job.skipped` and move on
3. Start a `job.process` span as a child of the API request's span
4. Run the handler with a timeout (15 s in Compose); a panic becomes a permanent error
5. Record the outcome (with a context that ignores shutdown):
   - Success → `completed` with the result
   - Permanent error → `failed`
   - Transient error, attempts used up → `dead_lettered`
   - Transient error, attempts left → `retrying` with `run_after = now + backoff`

## Job State Machine

```
            Claim                Complete
  queued ───────────▶ running ───────────▶ completed   (final)
    ▲                  │  │  │
    │   PromoteDue     │  │  │ Fail (permanent error)
  retrying ◀───────────┘  │  └──────────────▶ failed ──────────┐
            Retry          │                                     │ Reopen
            (transient,    │ DeadLetter (transient,              │ (POST /jobs/{id}/retry)
             attempts left)│  attempts exhausted)                │
                           └─────────────────▶ dead_lettered ───┤
                                                                 ▼
                                                              queued
```

| Status | Meaning |
|---|---|
| `queued` | Ready to run now; its ID should be in Redis |
| `running` | A worker holds a lease on it until `lease_expires_at` |
| `retrying` | Failed transiently; waiting for `run_after` |
| `completed` | Done. The only status that can never change again |
| `failed` | Permanent error (bad payload, unknown type, panic). Not retried automatically |
| `dead_lettered` | Used up `max_attempts` (3) on transient errors. Waiting for a human |

The allowed transitions are one Go map (`transitions` in `internal/job/job.go`).
In Postgres every transition is a single statement:

```sql
UPDATE jobs SET status = $2, ... WHERE id = $1 AND status = ANY($3) RETURNING ...
-- $3 = the statuses allowed before $2, derived from the same Go map
```

Because the check and the write are one statement, of many concurrent callers
making the same change exactly one succeeds. This is what makes duplicate
delivery harmless and lets every worker run a sweeper.

## Database Schema

One table, `jobs`, built by five migrations in `internal/db/migrations/`.
Migrations run at startup in one transaction under a Postgres advisory lock,
so an API and workers starting together don't apply them twice.

| Column | Purpose |
|---|---|
| `id` | 26-char random ID (`crypto/rand.Text`, 128 bits) |
| `type`, `payload` | What to run; payload is `jsonb` |
| `status` | CHECK-constrained to the six statuses |
| `attempts`, `max_attempts` | Attempt counter (incremented by Claim) and the limit (3) |
| `result`, `error` | Outcome; `error` is the most recent failure |
| `run_after` | When a `retrying` job becomes due |
| `lease_expires_at` | When a `running` job's worker is presumed dead |
| `enqueued_at` | When the ID was last pushed; used to find lost IDs |
| `idempotency_key` | UNIQUE, nullable (NULLs never collide) |
| `trace_parent` | W3C traceparent of the submitting request |
| `created_at`, `updated_at` | Set by the database clock |

All timestamps and durations are computed by the database (`now()`), so every
process uses one clock.

## Redis Queue and Delivery Semantics

- One Redis list, `queueforge:jobs`, holding job IDs only
- API does `LPUSH`, workers do `BRPOP` → FIFO
- `BRPOP` removes the ID as it hands it over, so Redis alone is **at-most-once**
- The system as a whole is **at-least-once** because Postgres knows every job's
  state and the sweeper rebuilds lost deliveries from it
- It is **not exactly-once**. A job can run twice if its worker dies after doing
  the work but before recording it. Handlers must be idempotent

Redis runs with `--appendonly yes`, so a restart normally keeps the list. That
isn't needed for correctness; it just makes recovery immediate.

## Retries

After every failed attempt `recordFailure` decides, in this order:

1. Error wrapped with `Permanent(...)` → `failed`
2. `attempts >= max_attempts` → `dead_lettered`
3. Otherwise → `retrying` with `run_after = now + Backoff.Delay(attempts)`

Backoff is exponential with "equal jitter":
ceiling = `min(1s × 2^(attempt−1), 30s)`, delay = random in `[ceiling/2, ceiling]`.

| After attempt | Ceiling | Actual wait |
|---|---|---|
| 1 | 1 s | 0.5–1 s |
| 2 | 2 s | 1–2 s |
| 3 | dead-lettered | |

Timeouts, cancellations, and abandoned leases count as transient.

## Idempotency

Duplicates come from three places, and each has its own defense:

| Duplicate | Cause | Defense |
|---|---|---|
| HTTP submission | client times out and retries | `Idempotency-Key` header + UNIQUE constraint |
| Queue delivery | same ID pushed twice | atomic Claim (`queued → running`) |
| Job execution | worker dies after the work, before recording it | not prevented: handlers must be idempotent |

## Recovery: Leases and the Sweeper

Claim gives the worker a **lease**: `lease_expires_at = now + 2 × job timeout`
(30 s in Compose). A live worker always finishes or times out before then. If
the worker dies, the lease simply runs out. Nothing has to detect the death.

The sweeper runs in every worker process, once a second:

| Step | Finds | Does |
|---|---|---|
| 1. `ExpiredLeases` | `running` jobs past their lease | Count it as a failed attempt → `retrying` or `dead_lettered` |
| 2. `PromoteDue` | `retrying` jobs whose `run_after` has passed | → `queued`, push ID |
| 3. `RequeueStale` | `queued` jobs older than 30 s, **only if the queue is empty** (10 min if there's a backlog) | Push the ID again |

All three are safe with many sweepers running at once because each is a
single conditional UPDATE.

| Crash point | Left behind | Repaired by |
|---|---|---|
| API after INSERT, before LPUSH | `queued`, no ID in Redis | step 3 |
| Worker after BRPOP, before Claim | `queued`, no ID in Redis | step 3 |
| Worker after Claim | `running`, lease ticking | step 1 → retry |
| Worker after the work, before Complete | `running` | step 1 → **runs again** |
| Redis loses its data | `queued`, no IDs | step 3 |

## Graceful Shutdown

- **Worker:** two contexts. `stopCtx` is cancelled by SIGTERM and stops taking
  new jobs. `jobsCtx` (the parent of every running attempt) is cancelled only
  after a 20 s grace. Running jobs normally finish and are recorded. Docker's
  `stop_grace_period: 30s` is longer than the grace so Docker never SIGKILLs mid-drain.
- **API:** `/ready` starts returning 503 so a load balancer stops routing,
  then `http.Server.Shutdown` lets in-flight requests finish (up to 10 s).

## Failure Handling

All observed on the running stack on 2026-10-07.

| Failure | Behavior |
|---|---|
| Worker killed (`SIGKILL`) | Job stays `running` until its lease expires (~30 s), then is retried and completes on attempt 2 |
| Worker stopped (`SIGTERM`) | Finishes running jobs, takes no new ones, exits (took 4 s, not 30) |
| API killed | Accepted jobs are already in Postgres + Redis and keep processing |
| Redis down | POST → 503 + `Retry-After: 1` (row deleted); `/ready` 503; `/health` 200; workers retry every 1 s; full recovery with no restarts |
| Postgres down | Job endpoints → 500; `/ready` 503; sweeper logs errors; pool reconnects on its own |
| Transient job error | `retrying` with backoff, up to 3 attempts, then `dead_lettered` |
| Permanent job error | `failed` after one attempt |
| Duplicate request | Same idempotency key → same job; different body → 409 |
| Duplicate delivery | Claim fails → `job.skipped` |
| Redis loses its data | Sweeper re-pushes the IDs from Postgres after 30 s |
| Slow job | Cancelled at the 15 s timeout, retried, dead-lettered after 3 timeouts |

## Observability

| Signal | What | Where |
|---|---|---|
| Logs | JSON lines with `service`, `event`, `request_id`, `job_id`, `trace_id`, `error` | stdout (`docker compose logs`) |
| Metrics | job counters (submitted, started, completed, failed, dead_lettered, retries, stale_requeued), queue depth, job duration, HTTP duration per route | `/metrics` → Prometheus → Grafana |
| Traces | `POST /jobs` → `store.create`, `queue.enqueue` → `job.process` per attempt | Jaeger |
| Health | `/health` = process alive; `/ready` = Postgres and Redis reachable and not draining | API :8080, worker :9091 |

Redis only carries an ID, so the trace crosses the queue through the database:
the API saves its span's `traceparent` on the job row, and the worker continues
the trace from it after Claim.

## Performance

Measured on 2026-10-07 on an Apple M5 Pro laptop, with everything (including
the k6 load generator) on the same machine through Docker Desktop. Default
2 workers × 4 goroutines. These are not production numbers.

| Peak arrival | POST p50 | POST p95 | POST p99 | Errors | Peak completed/s | Peak queue depth |
|---|---|---|---|---|---|---|
| 3,000 jobs/s | 1.00 ms | 1.67 ms | 2.44 ms | 0% | 3,000 | 0 |
| 6,000 jobs/s | 1.16 ms | 2.10 ms | 3.22 ms | 0% | 6,058 | 60,735 (drained 9 s after load) |

- Workers are round-trip bound, not CPU bound (~55% CPU each). One goroutine did ~2,900 jobs/s
- Postgres is the hottest container (one INSERT and two UPDATEs per job)
- The knee is between 6,000 and 10,000 jobs/s, where the whole laptop saturated

Load testing found a real bug: during a backlog the sweeper re-pushed jobs
that were only waiting, 151,286 duplicates in one run. Fixed by using the 30 s
rule only when the queue is empty. A/B results (one run each):

| Scenario | Duplicates before → after | Other effect |
|---|---|---|
| 100 ms jobs, 200/s | 8,325 → 0 | peak depth 9,516 → 5,800; drain unchanged (80 s) |
| fast jobs, 1 goroutine, 4,000/s | 51,313 → 0 | drain 44 s → 36 s |

Full write-up: [postmortem](postmortems/2026-10-07-sweeper-duplicate-flood.md).
Raw data: `loadtest/results/`.

## What This Project Simulates vs Real Systems

Real concepts implemented here:
- Async request/response with 202 and polling
- A state machine enforced atomically in SQL
- At-least-once delivery on top of an at-most-once queue
- Idempotency keys (same idea as Stripe's API)
- Exponential backoff with jitter, dead-letter queues
- Leases and a reconciliation loop (same idea as Kubernetes controllers)
- Graceful shutdown, liveness vs readiness
- Logs, metrics, and traces correlated by ID

What is simulated locally:
- Everything runs on one machine with Docker Compose
- Single Postgres and single Redis, no replication
- No real network partitions
- No authentication
- Load tests run the load generator on the same laptop
