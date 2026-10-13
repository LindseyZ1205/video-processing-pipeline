// One k6 run at a fixed arrival rate. Every iteration is a whole upload: request a presigned URL, PUT the file to S3,
// then wait for the worker to store the transcript. Run by .github/workflows/load-test.yml against the docker compose
// stack; RATE and DURATION come from the environment.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

const API = __ENV.API_URL || 'http://localhost:8080';
const RATE = Number(__ENV.RATE || 10);
const DURATION = __ENV.DURATION || '60s';
const FILE = 'load test file\n';

// From the moment S3 has the file until the transcript is stored: the asynchronous pipeline itself.
const pipelineLatency = new Trend('pipeline_latency', true);
const completed = new Rate('upload_completed');

export const options = {
  scenarios: {
    uploads: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: Math.min(RATE * 3, 300),
      maxVUs: 300,
      // Uploads still in flight when the rate stops get time to finish, so every started upload is counted.
      gracefulStop: '120s',
    },
  },
  summaryTrendStats: ['med', 'p(95)', 'p(99)', 'max'],
};

export default function () {
  const created = http.post(`${API}/api/uploads`, JSON.stringify({
    userId: 'load-test', fileName: 'clip.mp4', contentType: 'video/mp4', sizeBytes: FILE.length,
  }), { headers: { 'Content-Type': 'application/json' }, tags: { name: 'create upload' } });
  if (!check(created, { 'upload created': (r) => r.status === 201 })) {
    completed.add(false);
    return;
  }
  const upload = created.json();

  const put = http.put(upload.uploadUrl, FILE, { headers: upload.headers, tags: { name: 'put file' } });
  if (!check(put, { 'file stored in S3': (r) => r.status === 200 })) {
    completed.add(false);
    return;
  }
  const uploadedAt = Date.now();

  // Poll gently, and take the completion time from the job itself, so the polling interval doesn't skew the latency.
  while (Date.now() - uploadedAt < 120000) {
    sleep(0.5);
    const job = http.get(`${API}/api/uploads/${upload.uploadId}`, { tags: { name: 'poll job' } });
    if (job.status === 200 && job.json('status') === 'COMPLETED') {
      pipelineLatency.add(Math.max(0, Date.parse(job.json('updatedAt')) - uploadedAt));
      completed.add(true);
      return;
    }
  }
  completed.add(false);
}

export function handleSummary(data) {
  const metric = (name) => (data.metrics[name] ? data.metrics[name].values : {});
  const latency = metric('pipeline_latency');
  const result = {
    rate: RATE,
    duration: DURATION,
    started: metric('iterations').count || 0,
    completed: metric('upload_completed').passes || 0,
    failed: metric('upload_completed').fails || 0,
    dropped: metric('dropped_iterations').count || 0,
    latencyMs: { p50: latency.med, p95: latency['p(95)'], p99: latency['p(99)'], max: latency.max },
  };
  const ms = (value) => (value === undefined ? '-' : `${Math.round(value)} ms`);
  return {
    [`/results/summary-${RATE}.json`]: JSON.stringify(result, null, 2),
    stdout: `${RATE} uploads/s for ${DURATION}: ${result.completed} of ${result.started} completed, `
      + `${result.dropped} dropped; pipeline latency p50 ${ms(result.latencyMs.p50)}, `
      + `p95 ${ms(result.latencyMs.p95)}, max ${ms(result.latencyMs.max)}\n`,
  };
}
