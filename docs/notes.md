# QueueForge - Notes

## Part 0: Core Concepts

- Async job system: accept work now (return a job ID), do it later in another process.
- Slow work inside an HTTP request: timeouts, client retries → duplicates,
  connections held open, work lost on crash. Async fixes all four.
- Five pieces: API (accept), Postgres (truth), Redis (list of IDs), workers (run), sweeper (repair).
- Rule: anything that matters lives in Postgres. Anything lost can be rebuilt from Postgres.
- Redis holds only IDs. Losing Redis loses nothing permanent.
- At-most-once: removed before processing. Crash = lost, never duplicated.
- At-least-once: removed after processing. Crash = runs twice, never lost.
- Exactly-once is impossible end to end. Real systems: at-least-once + idempotent handlers.
- Runs locally only. Not deployed. Concepts are real; scale is a laptop.

## Part 1: Go Code Conventions

- One directory = one package. Capitalized = exported. `internal/` = only this module can import.
- JSON tags: `json:"job_id"` renames, `omitempty` drops empty, `json:"-"` hides the field.
- `json.RawMessage` = raw JSON bytes. The API passes payloads through without parsing them.
- `type Status string` = named type. Compiler won't accept a random string.
- `func (s *PostgresStore) Claim(...)`: pointer receiver, works on the shared struct.
- Functions return `(value, error)`. `nil` error = success.
- `%w` in `fmt.Errorf` wraps an error and keeps the chain.
- `errors.Is` = this exact error in the chain? `errors.As` = an error of this type in the chain?
- Interfaces are implicit. Defined where used, kept tiny (`Enqueuer` only has `Enqueue`).
- `context.Context`: cancellation + deadline. First parameter of anything doing I/O.
- `context.WithTimeout` = deadline per attempt. `context.WithoutCancel` = write the result even during shutdown.
- `wg.Go(...)` starts a tracked goroutine; `wg.Wait()` waits for all of them.
- `select` waits for whichever channel is ready first.
- `defer` runs on function exit. `recover()` in a defer stops a panic from crashing the process.
- `$1, $2` SQL parameters: values sent separately from the query, so no SQL injection.

## Part 2: Job Domain and State Machine

- Six statuses: queued, running, retrying, completed, failed, dead_lettered.
- Allowed moves are one Go map, `transitions`, in `internal/job/job.go`.
- queued → running → completed / failed / retrying / dead_lettered.
- retrying → queued (sweeper). failed / dead_lettered → queued (human).
- completed has no entry. Nothing can ever leave it.
- Status only changes through store methods: Create, Claim, Complete, Fail, DeadLetter, Retry, PromoteDue, Reopen.
- Without a state machine, a late duplicate could move completed back to running.
- `retrying` is separate so Claim never has to check the clock.
- failed = will never work as is. dead_lettered = gave up after trying. Different operator actions.
- CHECK constraint on `status`: even raw SQL can't store an impossible status.

## Part 3: PostgreSQL Store

- One table, `jobs`, five migrations.
- `Migrate` runs at startup in one transaction under an advisory lock. No double-apply, no half-apply.
- Every transition is `UPDATE ... WHERE id=$1 AND status = ANY($3) RETURNING ...`.
- `$3` comes from `statusesBefore`, derived from the same Go map. Go and SQL can't disagree.
- Check and write in one statement → exactly one concurrent caller wins.
- Why: row lock; waiting UPDATEs re-check WHERE against the committed row and match nothing.
- SELECT-then-UPDATE let 48–50 of 50 concurrent claims win against Postgres. The gap is a network round trip.
- No row returned → a second query only builds the error message (ErrNotFound vs ErrInvalidTransition).
- All timestamps from `now()` in the database. One clock for every process.
- No indexes beyond PK + idempotency key. A long-lived system would add partial indexes for the sweeper.
- `jsonb` reorders keys, so payloads are compared by meaning, not bytes.

## Part 4: API Request Flow

- POST /jobs → 202. GET /jobs/{id}. GET /jobs?status=. POST /jobs/{id}/retry. /health, /ready, /metrics.
- Middleware `withRequestLog`: request ID, trace span, logger in context, HTTP histogram, access log.
- `statusWriter` exists because `ResponseWriter` can't tell you the status code afterwards.
- Validation: 1 MiB cap (413), unknown fields rejected (400), known type (400), key ≤ 256 chars (400).
- Payload is NOT validated by the API. Each handler owns its format. Bad payload → `failed` in the worker.
- Insert row first, then push ID. Otherwise a worker could pop an ID with no row.
- Push fails → delete the row, return 503 + `Retry-After: 1`. Frees the idempotency key for the retry.
- If the delete also fails, the sweeper pushes the job later. Nothing lost.
- 202 = accepted, not done.
- Errors never leaked. DB error → 500 `internal error`, details in logs.
- 404 vs 500 kept separate so an outage doesn't look like "your job doesn't exist".
- `ReadHeaderTimeout: 5s` protects against slowloris.

## Part 5: Redis Queue

- One list, `queueforge:jobs`. API `LPUSH`, workers `BRPOP` (1 s timeout). FIFO.
- IDs only. Worker always reads the current row from Postgres.
- BRPOP removes on pop → Redis alone is at-most-once.
- System is at-least-once because the sweeper re-pushes lost IDs from Postgres.
- No Redis feature can find a job whose ID never reached Redis. Only scanning Postgres can.
- So Redis is kept simple: no processing lists, no Streams.
- Alternatives: BLMOVE processing list, Redis Streams, Postgres `SKIP LOCKED`. All valid.
- `--appendonly yes`: restart keeps the list. Not needed for correctness; makes recovery instant.
- `LLEN` used for the queue-depth metric and the sweeper's backlog check.

## Part 6: Workers

- Each worker process: 1 sweeper + `-concurrency` goroutines (4). Compose runs 2 replicas → 8 slots.
- One job per goroutine at a time = natural backpressure.
- Loop: BRPOP → Claim → span → run with timeout → record outcome.
- Claim first. Fails → `job.skipped`, nothing else happens.
- Timeout 15 s in Compose (90 s default). Panic → recovered → permanent error.
- Outcome recorded with `context.WithoutCancel` so shutdown never throws away a result.
- Dequeue error → log `queue.dequeue_failed`, wait 1 s. No tight loop.
- Job types: hash (CPU), sleep (I/O, honors cancellation), fail (always transient), flaky (fails N times).
- A handler that ignores `ctx` can't be stopped. Go can't kill a goroutine.
- 100 ms jobs × 8 slots = 80 jobs/s max. ~1 ms jobs: ~2,900 jobs/s per goroutine (round-trip bound).

## Part 7: Retries and Backoff

- Transient = might work later (timeout, dependency down). Permanent = will always fail (bad input).
- `recordFailure` order: permanent → failed; attempts used up → dead_lettered; else → retrying.
- Permanent = wrapped with `Permanent(...)`: bad payload, unknown type, panic.
- `isPermanent` uses `errors.As`, so it works through extra wrapping.
- Everything else is transient, including timeouts and abandoned leases.
- Exponential backoff: ceiling 1 s, 2 s, 4 s ... capped at 30 s.
- Equal jitter: random in [ceiling/2, ceiling]. Full jitter [0, ceiling] can retry almost instantly.
- No jitter = jobs that failed together retry together = retry storm.
- Sweeper's `PromoteDue` moves due retries to queued and pushes them, every second.
- Max 3 attempts (column default). Short delays so demos are fast.

## Part 8: Idempotency

- Idempotent = doing it twice has the same effect as once.
- Three duplicates, three defenses:
  - HTTP submission → `Idempotency-Key` + UNIQUE constraint.
  - Queue delivery → atomic Claim.
  - Execution after a crash → not prevented. Handlers must be idempotent.
- `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING RETURNING ...`.
- Same key + same body → original job, no second enqueue. Same key + different body → 409.
- The UNIQUE constraint is the lock. Go-side SELECT-then-INSERT would race.
- 50 concurrent creates with one key → exactly one job (tested against real Postgres).
- Nullable key; NULLs never collide. No key = always a new job.
- Replays log `job.replayed` and don't count as submitted.
- "Send email" handler would need its own key (e.g. job ID passed to the email provider).

## Part 9: Dead-Letter Handling

- Dead letter = given up after retries. Park it for a human instead of retrying forever or deleting.
- `dead_lettered` is a status, not a separate queue. The DLQ is `GET /jobs?status=dead_lettered`.
- Postgres already stores everything an operator needs, and supports filtering.
- Re-drive: `POST /jobs/{id}/retry` → `Reopen`: failed/dead_lettered → queued, attempts reset to 0.
- Any other status → 409.
- Old `error` kept until the job succeeds.
- Abandoned lease counts as an attempt → poison jobs terminate after 3 crashes.
- Reopening without fixing the cause just dead-letters again.

## Part 10: Leases, the Sweeper, and Graceful Shutdown

- Lease = "mine until T". `lease_expires_at = now + 2 × job timeout` (30 s in Compose).
- 2× so a live worker always finishes or times out before the lease ends.
- Worker dies → lease runs out. Nothing detects the death directly.
- Sweeper = reconciliation loop, in every worker, every second:
  1. `ExpiredLeases`: running past lease → failed attempt ("abandoned") → retrying / dead_lettered.
  2. `PromoteDue`: retrying and due → queued, push ID.
  3. `RequeueStale`: queued > 30 s **and queue empty** → push ID again. 10 min backstop during a backlog.
- Safe with many sweepers: each step is one conditional UPDATE. No leader election.
- Failed push → job stays queued, next sweep retries.
- Worker shutdown: `stopCtx` (SIGTERM) stops new work; `jobsCtx` cancelled after 20 s grace.
- Grace runs out → job cancelled → transient failure → retried.
- `stop_grace_period: 30s` > 20 s grace, so Docker never SIGKILLs mid-drain.
- API shutdown: `/ready` → 503, wait, `http.Server.Shutdown` (≤ 10 s), flush traces.
- Observed: SIGKILL → running ~30 s → completed on attempt 2. SIGTERM → stop took 4 s, completed on attempt 1.
- Lease vs heartbeat: heartbeat is faster and supports long jobs, but costs a write per job per interval.

## Part 11: Structured Logging

- One JSON object per line, stable field names.
- Fields: timestamp, level, service, event, msg, request_id, job_id, trace_id, error.
- `event` is for searching, `msg` is for reading. Rewording msg breaks nothing.
- API middleware puts a logger with request_id + trace_id in the request context.
- Context key is an unexported type so no package can collide.
- Worker lines have job_id + trace_id but no request_id (no HTTP request in the worker).
- Payloads never logged. Probe endpoints never logged.
- stdout → Docker → `docker compose logs`.
- Logs = what happened to this job. Metrics = how many per minute.

## Part 12: Metrics

- Counter only goes up. Gauge goes up and down. Histogram → percentiles.
- Graph counters with `rate(x[1m])`.
- Prometheus scrapes `/metrics` every 5 s, API and each worker replica.
- Counters: submitted, started, completed, failed, dead_lettered, retries, stale_requeued.
- Gauge: queue_depth. Histograms: job duration, HTTP duration by route.
- Queue depth exported only by the API. Real bug: workers exported a fake 0 → 3 series.
- HTTP label = route pattern (`GET /jobs/{id}`), never the URL.
- Job ID as a label = one series per job = cardinality explosion. IDs go in logs.
- Nil-safe methods; dedicated registry so tests don't share state.
- Dashboard = one operational question per panel.
- First alert: queue depth rising 10 min, or submitted > finished. Fix: add workers.

## Part 13: Tracing

- Trace = tree of spans for one piece of work across processes.
- W3C `traceparent`: `00-<trace id>-<parent span id>-<flags>`.
- OpenTelemetry → OTLP → Jaeger. No-op if no endpoint configured.
- API spans: `POST /jobs` → `store.create`, `queue.enqueue`.
- Redis has no headers, so the API stores the traceparent on the job row.
- Worker rebuilds it after Claim and starts `job.process` as a child.
- Every attempt, including retries and re-drives, is in the same trace.
- Not in the Redis message: re-pushed IDs would lose it. The row always has it.
- Jaeger down → spans dropped, app unaffected. Observed: Jaeger OOM-killed under load; jobs kept flowing.
- Jaeger now capped at 50,000 traces. Production would sample.

## Part 14: Health and Readiness

- `/health` = liveness. Fail → restart.
- `/ready` = readiness. Fail → stop routing, don't restart.
- Liveness must not check Postgres, or an outage becomes a restart storm.
- `/ready` pings Postgres + Redis under one 1 s timeout. Hung dependency fails fast.
- `/ready` returns 503 `draining` during shutdown.
- API on :8080, worker on :9091.
- Workers pull work, so readiness doesn't gate traffic, but still shows dependency health.
- Observed: Redis down → health 200, ready 503 → Redis back → ready 200. No restarts.

## Part 15: Docker and Compose

- Image = package. Container = running instance. One worker image, two worker containers.
- Multi-stage: build on `golang:1.26`, copy binary onto distroless (~20 MB, no shell, non-root).
- `go mod download` before copying source → dependency layer cached.
- `CGO_ENABLED=0` → static binary.
- Seven services: postgres, redis, api, worker ×2, prometheus, grafana, jaeger.
- Services reach each other by name (`postgres:5432`).
- Named volumes keep data. `down` keeps them, `down -v` deletes them.
- `depends_on: service_healthy` gates startup. `restart: unless-stopped` retries failed starts.
- Worker has no `ports:`. Prometheus reaches it inside the network.
- `${VAR:-default}` lets experiments change replicas/concurrency from the shell.

## Part 16: Testing

- Unit tests with in-memory fakes (`MemoryStore`, `queue.Memory`). Milliseconds.
- Contract tests: 31 behaviors run against MemoryStore and PostgresStore. Keeps the fake honest.
- Concurrency: 50 goroutines + start barrier + many trials, under `-race`.
- A concurrency test that never fails proves nothing: single trial caught a broken claim 0/100 times.
- `TestWorkersRunJobsConcurrently`: each job waits for all 4 → only passes if they overlap.
- Failure tests with stubs: `brokenQueue`, `brokenStore`.
- E2E behind the `e2e` build tag, against the running stack.
- Integration tests use `queueforge_test` DB and Redis DB 15 (they truncate).

## Part 17: CI

- CI = every push tested. Continuous delivery = always releasable. Continuous deployment = auto release.
- QueueForge has CI only. Not deployed, by choice.
- `test` job: gofmt, vet, `go test -race` with Postgres + Redis service containers.
- `e2e` job: build images, Compose up, wait for `/ready`, e2e suite, logs on failure, tear down.
- If deployed: SHA-tagged images, rolling deploy gated on `/ready`, rollback, backward-compatible migrations.

## Part 18: Load Testing and Performance

- p50 = median, p95/p99 = tail. Averages hide the tail.
- Open model (arrival rate) vs closed model (N users). Closed hides slowness: coordinated omission.
- k6 `ramping-arrival-rate`: peak/8 → peak/4 → peak/2 → peak → hold.
- `run.sh` measures both sides: k6 (API latency), Prometheus (throughput, depth), Postgres (exact counts).
- 3,000/s: POST p99 2.44 ms, no backlog.
- 6,000/s: POST p50/p95/p99 1.16/2.10/3.22 ms, 0 errors, ~6,050 completions/s, backlog cleared 9 s after load.
- Laptop, k6 on the same machine. Not production numbers.
- Workers round-trip bound, ~55% CPU. One goroutine ~2,900 jobs/s.
- Postgres hottest container (1 INSERT + 2 UPDATEs per job).
- 10,000/s saturated the whole laptop. One stalled run discarded.
- Bug found: sweeper treated "queued > 30 s" as lost during a backlog → 151,286 duplicates. Feedback loop.
- Found by counting logs. No metric existed.
- Fix: 30 s rule only when the queue is empty, 10 min backstop. Added `stale_requeued_total`.
- A/B: 8,325 → 0 duplicates (100 ms jobs); 51,313 → 0 and drain 44 s → 36 s (fast jobs). One run each.
- Idempotency makes duplicates safe, not free.
