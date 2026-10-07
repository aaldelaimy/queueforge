// k6 load test: POST /jobs, then GET /jobs/{id}, at a stepped arrival rate.
// An arrival-rate executor so a slow API shows up as latency instead of
// silently lowering the load.
//
// Run against the Compose stack without installing k6:
//   docker run --rm -i --network queueforge_default -e BASE_URL=http://api:8080 \
//     grafana/k6 run - < loadtest/submit.js
//
// Env:
//   BASE_URL   default http://localhost:8080
//   JOB_TYPE   hash (default, ~instant) or sleep (I/O-bound, 100 ms each)
//   PEAK_RPS   highest arrival rate, default 400
import http from "k6/http";
import { check } from "k6";
import { Counter } from "k6/metrics";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
const JOB_TYPE = __ENV.JOB_TYPE || "hash";
const PEAK = parseInt(__ENV.PEAK_RPS || "400", 10);

const rejected = new Counter("jobs_rejected");

export const options = {
  scenarios: {
    submit: {
      executor: "ramping-arrival-rate",
      startRate: Math.max(1, Math.floor(PEAK / 8)),
      timeUnit: "1s",
      preAllocatedVUs: 50,
      maxVUs: 500,
      stages: [
        { target: Math.floor(PEAK / 4), duration: "30s" },
        { target: Math.floor(PEAK / 2), duration: "30s" },
        { target: PEAK, duration: "30s" },
        { target: PEAK, duration: "30s" },
      ],
    },
  },
  // Report p50 as well as k6's default percentiles.
  summaryTrendStats: ["avg", "min", "med", "p(90)", "p(95)", "p(99)", "max"],
  thresholds: {
    // Fail the run if submission gets slow or starts erroring.
    "http_req_duration{name:POST /jobs}": ["p(95)<250"],
    http_req_failed: ["rate<0.01"],
  },
};

function payload() {
  if (JOB_TYPE === "sleep") {
    return { type: "sleep", payload: { duration_ms: 100 } };
  }
  return { type: "hash", payload: { input: `k6-${__VU}-${__ITER}` } };
}

export default function () {
  const res = http.post(`${BASE_URL}/jobs`, JSON.stringify(payload()), {
    headers: { "Content-Type": "application/json" },
    tags: { name: "POST /jobs" },
  });
  const ok = check(res, { "POST 202": (r) => r.status === 202 });
  if (!ok) {
    rejected.add(1);
    return;
  }

  const id = res.json("job_id");
  const got = http.get(`${BASE_URL}/jobs/${id}`, { tags: { name: "GET /jobs/{id}" } });
  check(got, { "GET 200": (r) => r.status === 200 });
}
