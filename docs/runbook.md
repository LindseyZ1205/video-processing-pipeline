# Runbook

What to do when an alert from [`monitoring/alerts.yml`](../monitoring/alerts.yml) fires. Every alert links to its
section here through a `runbook_url` annotation.

The commands use the AWS CLI v2. Against LocalStack, set `AWS_ENDPOINT_URL=http://localhost:4566` and any credentials
(`test`/`test`). The queue names are the service's defaults. With Terraform, take them from
`terraform output -json service_environment`.

## UploadEventsInDeadLetterQueue

**Page.** Upload events failed three deliveries in a row, and SQS moved them to the dead-letter queue. Those uploads
won't be processed until someone redrives them. The dead-letter queue Terraform creates keeps messages for 14 days.

1. **Find the uploads.** Look at the messages without consuming them. A visibility timeout of 0 leaves them visible:

   ```bash
   aws sqs receive-message \
     --queue-url "$(aws sqs get-queue-url --queue-name video-upload-events-dlq --query QueueUrl --output text)" \
     --max-number-of-messages 10 --visibility-timeout 0 --query 'Messages[].Body' --output text
   ```

   Each body is an S3 event. Its object key, `uploads/{userId}/{uploadId}/{fileName}`, contains the upload ID.
2. **Read why they failed.** `GET /api/uploads/{uploadId}` returns the job's `attempts` and its last `error`.
   - `401` or `403` from the provider: the API key is missing or was revoked. Fix the configuration.
   - `429`, `5xx` or timeouts: the provider was degraded for longer than the retries' backoff covered. A
     `RetryLaterException` also says how long the provider asked to wait.
   - No job, or no error on it: the handler threw before recording anything. DynamoDB was unreachable, or the message
     isn't a readable S3 event. These count as `outcome="error"` in `pipeline_events_total`, and the worker log has
     the exception next to `was not processed`.
3. **Fix the cause first.** Otherwise the redriven messages fail again and are back in the dead-letter queue three
   deliveries later.
4. **Redrive:**

   ```bash
   scripts/redrive-dlq.sh
   ```

   It starts an SQS message move task from the dead-letter queue back to the event queue, and waits until the task
   completes. A job that already failed three times is simply claimed again, as its next attempt.
5. **Check.** `pipeline_queue_messages{queue="dead-letter",state="visible"}` drops to 0 at the next gauge refresh
   (30 s), and the jobs reach `COMPLETED`.

## UploadEventsFailing

**Warn.** Over 10 minutes, more than 10% of upload events failed (`outcome="failed"`) or errored
(`outcome="error"`). Failed attempts are retried with a backoff, so many of these uploads still complete.

- `failed` comes from the attempt itself: the transcription provider (rate limits, outages, timeouts) or S3. The worker
  log has a `failed on attempt` warning with the `jobId`.
- `error` means the handler threw: DynamoDB is unreachable, or messages can't be read. The worker log has
  `was not processed`.
- A degraded provider usually recovers on its own, and the backoff spreads the retries out. If it lasts,
  `UploadEventsInDeadLetterQueue` follows.

## SlowUploadPickup

**Warn.** For 10 minutes, the p95 time from an upload finishing to a worker starting on it has been over two
minutes.

- A growing backlog in `pipeline_queue_messages{queue="events",state="visible"}` means the workers can't keep up. Run
  more instances, or raise `pipeline.worker.concurrency`.
- Slow pickups without a backlog usually mean retries. The lag includes backoff delays, so check
  `UploadEventsFailing` first.
- Long transcriptions (`pipeline_transcription_seconds`) occupy the pollers, and each poller handles one message at a
  time.

## WorkerCannotReachSqs

**Page.** Polling SQS has been failing for 10 minutes, so no uploads are being processed.

- The worker log says `Receiving from SQS failed N time(s) in a row` and shows the cause.
- Credentials and IAM: the service's role needs `sqs:ReceiveMessage` on the event queue (`infra/terraform/iam.tf`).
- Configuration: the queue name (`PIPELINE_QUEUE_NAME`), or on LocalStack the endpoint (`PIPELINE_AWS_ENDPOINT`).
- Nothing is lost meanwhile. Events wait in the queue, which keeps them for 4 days, and they're processed once polling
  recovers.

## JobLeasesKeepGettingLost

**Warn.** Workers keep finding that another worker took over their job midway, so those files get transcribed
twice.

- A worker loses its lease when it fails to renew it for a whole visibility timeout. Look for long GC pauses or CPU
  starvation on the hosts, and for DynamoDB errors in `pipeline_lease_extensions_total{outcome="failed"}`.
- `pipeline.worker.heartbeat-interval` must stay well below `pipeline.worker.visibility-timeout`: 20 s against 60 s by
  default.
- Results stay correct, because the lease token fences off the stale worker. But every lost lease is a paid
  transcription thrown away.
