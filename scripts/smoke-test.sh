#!/usr/bin/env bash
# End-to-end check against a running service: ask for an upload URL, PUT a file straight to S3,
# then wait for the worker to finish the job. Needs curl and jq.
set -euo pipefail

api="${API_URL:-http://localhost:8080}"
file="$(mktemp)"
trap 'rm -f "$file"' EXIT
printf 'smoke test %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$file"
size="$(wc -c < "$file" | tr -d ' ')"

upload="$(curl -fsS -X POST "$api/api/uploads" \
  -H 'Content-Type: application/json' \
  -d "{\"userId\":\"smoke-test\",\"fileName\":\"smoke.mp4\",\"contentType\":\"video/mp4\",\"sizeBytes\":$size}")"
upload_id="$(jq -r .uploadId <<< "$upload")"
echo "created upload $upload_id ($(jq -r .objectKey <<< "$upload"))"

curl -fsS -X "$(jq -r .method <<< "$upload")" \
  -H "Content-Type: $(jq -r '.headers["Content-Type"]' <<< "$upload")" \
  --upload-file "$file" \
  "$(jq -r .uploadUrl <<< "$upload")" > /dev/null
echo "uploaded $size bytes to S3"

status=""
for _ in $(seq 60); do
  job="$(curl -fsS "$api/api/uploads/$upload_id")"
  status="$(jq -r .status <<< "$job")"
  if [ "$status" = "COMPLETED" ]; then
    echo "job completed after $(jq -r .attempts <<< "$job") attempt(s): $(jq -r .transcript <<< "$job")"
    exit 0
  fi
  sleep 1
done

echo "job did not complete within 60s (last status: ${status:-unknown})" >&2
exit 1
