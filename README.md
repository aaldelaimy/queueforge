# QueueForge

An asynchronous job-processing system built in Go. Clients submit jobs over a
REST API and get an ID back immediately; workers run the jobs in the
background. Demonstrates backend systems concepts including at-least-once
delivery, retries with backoff, idempotency, dead-letter handling, crash
recovery, and observability.

## Architecture

The system has four components:

**API** - validates requests, writes the job row to Postgres, pushes the job
ID to Redis, and answers `202` with the ID. Never runs jobs.

**PostgreSQL** - the source of truth for every job: status, attempts, result,
error. Enforces the job state machine with atomic conditional UPDATEs.

**Redis** - a list of job IDs so workers wake instantly. Holds nothing that
can't be rebuilt from Postgres.

**Workers** - claim a job (exactly one worker wins), run it with a timeout,
and record the outcome. A sweeper inside each worker recovers jobs from dead
workers, promotes due retries, and re-pushes lost IDs.

```
client ──HTTP──▶ API ──INSERT──▶ PostgreSQL ◀──Claim/Complete/Retry── Workers (N × K goroutines)
                  └──LPUSH id──▶ Redis list ──BRPOP id──────────────▶   + sweeper (recovery)

Prometheus ◀─ scrape /metrics      Jaeger ◀─ OTLP spans      stdout ─ JSON logs
```

## Features

- Job state machine: `queued → running → completed | retrying | failed | dead_lettered`, enforced in SQL
- Retries with exponential backoff and jitter; permanent vs transient errors; max 3 attempts
- Dead-lettering with list and re-drive (`POST /jobs/{id}/retry`)
- Idempotency keys (`Idempotency-Key` header, UNIQUE constraint, 409 on conflicting reuse)
- Duplicate-delivery protection (atomic claim)
- Crash recovery with leases; lost-ID recovery; graceful shutdown that finishes in-flight jobs
- Structured JSON logs with `request_id`, `job_id`, `trace_id`
- Prometheus metrics and a provisioned Grafana dashboard
- OpenTelemetry tracing from the HTTP request through every worker attempt, across the queue
- `/health` (liveness) and `/ready` (readiness) on API and workers
- Unit, contract, integration, and end-to-end tests; CI on GitHub Actions
- Load tested with k6

## Project Structure

```
queueforge/
  cmd/
    api/               # API entry point
    worker/            # worker entry point
  internal/
    job/               # job model, state machine, Postgres + in-memory stores
    api/               # HTTP handlers and middleware
    worker/            # worker loop, retries/backoff, sweeper, job types
    queue/             # Redis queue
    db/                # connection + embedded migrations
    applog/            # structured logging
    metrics/           # Prometheus metrics
    tracing/           # OpenTelemetry tracing
    health/            # liveness and readiness
  e2e/                 # end-to-end tests
  loadtest/            # k6 script, runner, results
  deploy/              # Prometheus, Grafana, Postgres init config
  docs/
    design.md              # system architecture and tradeoffs
    notes.md               # part-by-part notes
    system-explanations.md # plain-English system explanation
    postmortems/           # real incidents
```

## More Details

- [Design](docs/design.md) - architecture, flows, schema, failure handling, and performance
- [Notes](docs/notes.md) - part-by-part notes
- [System Explanation](docs/system-explanations.md) - plain-English system explanation
- [Postmortem](docs/postmortems/2026-10-07-sweeper-duplicate-flood.md) - a bug found by load testing

## Setup

Requirements: Docker. Go 1.26+ only for running tests.

```bash
git clone https://github.com/aaldelaimy/queueforge.git
cd queueforge
go mod download
```

## Running the System

```bash
docker compose up -d --build
```

Once it's running, these are available on your machine:

| Service | URL |
|---|---|
| API | `http://localhost:8080` |
| Grafana dashboard | `http://localhost:3000/d/queueforge` |
| Prometheus | `http://localhost:9090` |
| Jaeger (traces) | `http://localhost:16686` |

```bash
# scale workers
WORKER_REPLICAS=4 WORKER_CONCURRENCY=8 docker compose up -d worker

# stop (add -v to delete all data)
docker compose down
```

## Commands

```bash
# submit a job (the key is optional; reuse it to retry safely)
curl -s -X POST localhost:8080/jobs -H 'Idempotency-Key: order-42' \
  -d '{"type":"hash","payload":{"input":"hello"}}'

# check a job
curl -s localhost:8080/jobs/<job_id>

# list jobs, filtered by status
curl -s 'localhost:8080/jobs?status=dead_lettered'

# re-drive a failed or dead-lettered job
curl -s -X POST localhost:8080/jobs/<job_id>/retry

# follow one job through the logs
docker compose logs --no-log-prefix api worker | grep <job_id>
```

| Job type | Payload | Behavior |
|---|---|---|
| `hash` | `{"input":"..."}` | SHA-256 of the input |
| `sleep` | `{"duration_ms":N}` (≤ 60000) | waits N ms |
| `fail` | — | always fails transiently → dead-lettered after 3 attempts |
| `flaky` | `{"fail_times":N}` | fails N times, then succeeds |

Status codes: `202` accepted · `400` invalid · `404` unknown job · `409`
idempotency conflict or job not retryable · `413` body > 1 MiB · `500` database
error · `503` queue unavailable (with `Retry-After`).

## Demo

**Submit a job and get the result:**
```
$ curl -s -X POST localhost:8080/jobs -d '{"type":"hash","payload":{"input":"hello"}}'
{"job_id":"3JBM...","type":"hash","status":"queued","attempts":0,"max_attempts":3,...}

$ curl -s localhost:8080/jobs/3JBM...
{"job_id":"3JBM...","status":"completed","attempts":1,"result":{"sha256":"2cf24dba..."},...}
```

**Same idempotency key again (no duplicate job):**
```
$ curl -s -X POST localhost:8080/jobs -H 'Idempotency-Key: demo-1' -d '{"type":"hash","payload":{"input":"pay"}}'
# → same job_id as the first request, attempts still 1
$ curl -s -X POST localhost:8080/jobs -H 'Idempotency-Key: demo-1' -d '{"type":"hash","payload":{"input":"DIFFERENT"}}'
# → 409
```

**Transient failures are retried:**
```
$ curl -s -X POST localhost:8080/jobs -d '{"type":"flaky","payload":{"fail_times":2}}'
# a few seconds later:
{"status":"completed","attempts":3,"result":{"succeeded_on_attempt":3},...}
```

**Retries run out (dead-letter):**
```
$ curl -s -X POST localhost:8080/jobs -d '{"type":"fail"}'
# ~6 seconds later:
{"status":"dead_lettered","attempts":3,"error":"... (gave up after 3 attempts)",...}
```

**Worker crash recovery:**
```
# submit an 8 s sleep job, then:
$ docker compose kill -s SIGKILL worker; docker compose start worker
# job stays "running" ~30 s (its lease), then is retried:
{"status":"completed","attempts":2,...}
```

**Redis down:**
```
$ docker compose stop redis
$ curl -s -i -X POST localhost:8080/jobs -d '{"type":"hash","payload":{"input":"x"}}'
HTTP/1.1 503 Service Unavailable
Retry-After: 1
{"error":"queue unavailable, try again later"}

$ curl -s localhost:8080/ready
{"checks":{"postgres":"ok","redis":"context deadline exceeded"},"status":"unavailable"}
# /health still returns 200; after `docker compose start redis` everything recovers with no restarts
```

## Running Tests

```bash
# unit + contract tests (in-memory, no services needed)
go test -race ./...

# + real Postgres and Redis (stack must be running)
QUEUEFORGE_TEST_DATABASE_URL='postgres://queueforge:queueforge@localhost:5432/queueforge_test?sslmode=disable' \
QUEUEFORGE_TEST_REDIS_URL='redis://localhost:6379/15' \
go test -race -count=1 ./...

# end-to-end against the running stack
go test -tags e2e -count=1 -v ./e2e/
```

The same 31-test store contract runs against the in-memory store and real
Postgres, including 50-goroutine race tests. CI (`.github/workflows/ci.yml`)
runs gofmt, vet, the race-detected tests with Postgres and Redis service
containers, and the e2e suite against Docker Compose.

## Load Testing

```bash
loadtest/run.sh 3000 my-run          # resets the jobs table, runs k6 in Docker, waits for the drain, reports
loadtest/run.sh 200 slow-run sleep   # 100 ms jobs: makes the workers the bottleneck
```

Measured on an Apple M5 Pro laptop, everything (including k6) on one machine,
default 2 workers × 4 goroutines. Not production numbers.

| Peak arrival | POST p50 | POST p95 | POST p99 | Errors | Workers' peak completions |
|---|---|---|---|---|---|
| 3,000 jobs/s | 1.00 ms | 1.67 ms | 2.44 ms | 0% | 3,000/s |
| 6,000 jobs/s | 1.16 ms | 2.10 ms | 3.22 ms | 0% | ~6,050/s |

Load testing found a real bug: during a backlog, the recovery sweeper mistook
jobs waiting longer than 30 s for lost ones and re-pushed them (151,286
duplicates in one run). After the fix, A/B runs showed duplicates going from
8,325 → 0 (slow jobs) and 51,313 → 0 (fast jobs, with drain time 44 s → 36 s).
See the [postmortem](docs/postmortems/2026-10-07-sweeper-duplicate-flood.md).

## Design Tradeoffs

**Postgres + Redis vs Postgres only**
Postgres is durable and queryable; Redis lets workers wait for work without
polling the database. A Postgres-only queue (`SELECT ... FOR UPDATE SKIP LOCKED`)
would also work at this scale, with one less system to run.

**At-least-once vs exactly-once**
Exactly-once isn't achievable end to end. We use at-least-once delivery plus
idempotent handling: a job can run twice if a worker dies after doing the work
but before recording it.

**Leases vs heartbeats**
A fixed lease (2 × job timeout) is simple but delays crash recovery by ~30 s.
Heartbeats would detect death faster and support long jobs, at the cost of a
write per job per interval.

**Redis list vs Redis Streams / a broker**
No Redis feature can find a job whose ID never reached Redis, so a Postgres
sweeper is needed anyway. Once it exists, Redis-side acks add little, so the
queue is a plain list.

**State machine in SQL vs status strings**
Every transition is one conditional UPDATE, so concurrent races become
rejected operations instead of silent corruption.

## Limitations

- Runs locally with Docker Compose; not deployed
- Single Postgres and Redis instances, no replication
- No authentication, pagination, idempotency key expiry, or job archival
- No per-query timeouts; a hung (rather than down) dependency isn't bounded
- Leases aren't extended; very long jobs need heartbeats
- The sweeper's queries scan the `jobs` table; a long-lived system would add partial indexes

## Potential Improvements

- Lease heartbeats for long-running jobs
- Partial indexes for the sweeper queries
- Statement timeouts on database queries
- Authentication and per-tenant idempotency keys
- Worker autoscaling on queue depth, plus alerting rules
- Per-job-type retry policies
- A time-in-queue histogram
- Cloud deployment (ECS/Fargate, RDS, ElastiCache) with a CD pipeline
