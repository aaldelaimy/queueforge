#!/usr/bin/env bash
# Runs one k6 load test against a clean stack and reports what happened on
# both sides of the queue: API latency from k6, worker side from Prometheus.
#
#   loadtest/run.sh <peak_rps> <label> [job_type]
#
# It empties the jobs table and the Redis queue first, so runs are
# comparable. Do not point it at data you want to keep.
set -euo pipefail
cd "$(dirname "$0")/.."

PEAK=${1:?peak rps}
LABEL=${2:?label}
JOB_TYPE=${3:-hash}
OUT=loadtest/results/$LABEL
mkdir -p "$OUT"

echo "== resetting jobs table and queue"
docker compose exec -T postgres psql -qU queueforge -c 'TRUNCATE jobs' >/dev/null
docker compose exec -T redis redis-cli DEL queueforge:jobs >/dev/null
sleep 5 # let rates from earlier runs age out of Prometheus' window

START=$(date +%s)
echo "== k6: peak $PEAK req/s, job type $JOB_TYPE"
docker run --rm -i --network queueforge_default \
  -e BASE_URL=http://api:8080 -e PEAK_RPS="$PEAK" -e JOB_TYPE="$JOB_TYPE" \
  -v "$PWD/$OUT:/results" \
  grafana/k6 run --quiet --summary-export=/results/k6-summary.json - < loadtest/submit.js \
  > "$OUT/k6.txt" 2>&1 || true # a failed threshold is a result, not an error
LOAD_END=$(date +%s)

echo "== waiting for the backlog to drain"
while :; do
  depth=$(docker compose exec -T redis redis-cli LLEN queueforge:jobs | tr -d '\r')
  queued=$(docker compose exec -T postgres psql -tAU queueforge -c "select count(*) from jobs where status in ('queued','running','retrying')" | tr -d '\r')
  [ "$depth" = 0 ] && [ "$queued" = 0 ] && break
  sleep 2
done
END=$(($(date +%s) + 10)) # one more scrape after the drain
sleep 10

# Exact counts come from Postgres; Prometheus increase() extrapolates.
DB_COUNTS=$(docker compose exec -T postgres psql -tAU queueforge -c "select count(*), count(*) filter (where status='completed'), coalesce(sum(attempts),0) from jobs" | tr -d '\r')

python3 - "$OUT" "$START" "$LOAD_END" "$END" "$DB_COUNTS" <<'PY'
import json, sys, urllib.parse, urllib.request
out, start, load_end, end = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
db_jobs, db_completed, db_attempts = (int(x) for x in sys.argv[5].split("|"))
rng = f"{end-start}s"

def q(expr):
    url = "http://localhost:9090/api/v1/query?" + urllib.parse.urlencode({"query": expr, "time": end})
    r = json.load(urllib.request.urlopen(url))["data"]["result"]
    return float(r[0]["value"][1]) if r else 0.0

def inc(name):
    return q(f"sum(increase({name}[{rng}]))")

k6 = json.load(open(f"{out}/k6-summary.json"))["metrics"]
post = k6["http_req_duration{name:POST /jobs}"]
report = {
    "post_jobs_ms": {k: round(post[k], 2) for k in ("med", "p(95)", "p(99)", "max")},
    "http_reqs_per_s": round(k6["http_reqs"]["rate"], 1),
    "http_error_rate": k6["http_req_failed"]["value"],
    "k6_dropped_iterations": k6.get("dropped_iterations", {}).get("count", 0),
    "jobs_submitted": db_jobs,
    "jobs_completed": db_completed,
    "attempts": db_attempts,
    "stale_requeued": round(inc("queueforge_jobs_stale_requeued_total")),
    "peak_completed_per_s": round(q(f"max_over_time(sum(rate(queueforge_jobs_completed_total[15s]))[{rng}:5s])")),
    "peak_queue_depth": round(q(f'max_over_time(queueforge_queue_depth{{job="api"}}[{rng}])')),
    "job_attempt_p99_ms": round(1000 * q(f"histogram_quantile(0.99, sum by (le) (increase(queueforge_job_duration_seconds_bucket[{rng}])))"), 2),
    "load_seconds": load_end - start,
    "drain_after_load_seconds": end - 10 - load_end,
}
json.dump(report, open(f"{out}/report.json", "w"), indent=2)
print(json.dumps(report, indent=2))
PY
