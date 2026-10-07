# Postmortem: The sweeper flooded the queue with duplicates during a backlog

**Date:** 2026-10-07 · **Environment:** local Docker Compose stack, load test
· **Severity:** no data loss, no wrong results; wasted work and a longer
backlog under overload · **Status:** fixed and verified

## Summary

During a k6 load test at a 10,000 jobs/s peak, the workers fell behind and a
backlog of about 210,000 IDs built up in Redis. The sweeper's lost-ID check
treats any job that has been `queued` for more than 30 s as "probably lost"
and pushes its ID again. During a backlog, almost every job waits longer than
30 s, so the sweeper pushed **151,286** IDs that were not lost, just waiting
in line. Every one of them later reached a worker as a duplicate, failed its
`Claim`, and was logged as `job.skipped`: 151,286 skips, about 17% extra
deliveries on top of the ~885,000 real jobs, all arriving while the system
was already overloaded.

Correctness held: the conditional `Claim` meant no job ran twice. The cost was
throughput and queue length at the worst possible moment, plus a positive
feedback loop (a longer backlog → more "stale" jobs → more duplicates → a
longer backlog).

## Impact

- 151,286 needless deliveries in one run (17% of real jobs). Each cost one
  Redis pop and one Postgres UPDATE that matched nothing.
- In controlled reproductions (below), duplicates were 60% of jobs with
  100 ms jobs, and 18.5% with fast jobs, where they made the backlog take
  longer to drain.
- No job ran twice, none was lost, and no client saw an error.

## Timeline (UTC)

| Time | Event |
|---|---|
| ~18:37 | Load test starts, stepping from 1,250 to 10,000 req/s |
| 18:37:39 | Queue depth leaves zero: workers peak at ~5,250 completions/s |
| 18:38:34 | Queue depth peaks at 210,256 |
| ~18:38–18:39 | Jobs older than 30 s begin to be re-pushed by every sweeper |
| 18:39:14 | Backlog finally drains |
| shortly after | While checking the run, logs show `job.stale_requeued` 151,286 times and `job.skipped` 151,286 times |
| same day | Fix written and unit-tested; A/B experiments run (below) |

## Detection

Not by an alert. It was found by counting log events after the run
(`docker compose logs worker | grep -c job.stale_requeued`). The matching
counts of `stale_requeued` and `skipped` pointed straight at the sweeper.

At that point there was **no metric** for stale re-pushes, so the Grafana
dashboard could not have shown it. Only queue depth looked "bad", and that
was expected under overload.

## Root cause

`Sweeper.sweep` called `store.RequeueStale(ctx, 30s)`, which returns every job
that has been `queued` longer than 30 s. That rule rests on an assumption:
*a job doesn't normally wait in Redis for 30 s, so a 30-second-old queued job
must have lost its ID.* The assumption holds when workers keep up. It fails
during any backlog longer than 30 s, which is exactly when extra deliveries
hurt most.

The code comment even said the duplicate was "harmless: only one worker can
claim it". That was true for correctness but not for cost. Each sweeper ran
this every second, and every worker process has a sweeper.

## Resolution

The sweeper now checks the queue length first (`internal/worker/sweeper.go`):

- **Queue empty** → use the 30 s limit (`-requeue-after`). With nothing in Redis,
  an old `queued` job cannot be waiting in line: its ID really is missing (or
  being popped this instant, which is harmless).
- **Queue has a backlog** (or Redis can't be reached) → use a 10-minute backstop
  (`-backlog-requeue-after`), so a lost ID still can't wait forever if the
  backlog never drains.

Also added:
- `queueforge_jobs_stale_requeued_total` counter and a Grafana series, so this
  is visible on the dashboard next time.
- Tests: `TestSweeperDoesNotRequeueDuringBacklog`, `TestSweeperBacklogBackstop`,
  `TestStaleRequeueIsCounted`.
- `NewSweeper` treats a zero `BacklogRequeueAfter` as the default instead of
  "everything is stale".

## Verification (A/B, same machine, same script)

`loadtest/run.sh` resets the table, runs k6, waits for the drain, and reads
Prometheus and Postgres. "Before" is the old behavior, reproduced exactly with
`BACKLOG_REQUEUE_AFTER=30s`; "after" is the default. One run each.

**100 ms jobs (`sleep`), 8 worker slots (~80 jobs/s capacity), 200 req/s peak:**

| | Before | After |
|---|---|---|
| Jobs | 13,874 | 13,875 |
| Stale re-pushes | **8,325** | **0** |
| Peak queue depth | 9,516 | 5,800 |
| Drain after load | 80 s | 80 s |

Drain time is unchanged: a skipped duplicate costs ~1 ms against a 100 ms job.

**Fast jobs (`hash`), 1 worker goroutine, 4,000 req/s peak:**

| | Before | After |
|---|---|---|
| Jobs | 277,423 | 277,452 |
| Stale re-pushes | **51,313** | **0** |
| Peak completions/s | 2,873 | 2,907 |
| Drain after load | 44 s | 36 s |

With fast jobs a duplicate costs about as much as a real job, so removing them
shortened the drain by 8 s (18%). Single runs: treat the drain difference as
indicative, not precise.

Raw data: `loadtest/results/backlog-*`.

## Prevention / lessons

- **"Harmless" duplicates still cost something.** Idempotency makes duplicates
  safe, not free. Check what they cost under load.
- **Recovery mechanisms need their own load test.** The sweeper was correct
  in every unit and failure test. It only misbehaved under sustained overload,
  which only a load test creates.
- **Every repair action should be a metric.** Without
  `stale_requeued_total` the behavior was invisible on the dashboard.
- **A time-based heuristic (age > 30 s) should be checked against the
  assumption behind it** (normal queue wait < 30 s), and that assumption can be
  measured: queue depth.

## Remaining risk

During a backlog longer than 10 minutes, the backstop will still re-push jobs
older than 10 minutes. By then the system is badly overloaded and the right
response is more workers, which the queue depth panel shows. A precise
alternative (tracking which IDs are actually in Redis, e.g. with a set
alongside the list) was rejected as more moving parts than the problem warrants.
