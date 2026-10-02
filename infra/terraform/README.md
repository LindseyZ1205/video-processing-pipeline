# Infrastructure

Terraform for the AWS resources the service runs on.

| Resource | Details |
|---|---|
| S3 bucket `video-uploads-…` | Private, SSE-S3 encrypted, CORS rule for browser PUTs, uploads expire after 30 days |
| S3 → SQS notification | `s3:ObjectCreated:*` for keys under `uploads/` only |
| SQS queue and dead-letter queue | SSE-SQS, 60 s visibility timeout, 3 deliveries before a message moves to the DLQ, 14 days of DLQ retention |
| SQS queue policy | Lets the upload bucket in this account, and nothing else, send events |
| DynamoDB table `video-processing-jobs` | On-demand capacity, TTL on `expiresAt` |
| IAM policy `video-processing-pipeline-service` | Least-privilege runtime permissions, to attach to the service's role |

Everything the service needs to know about these resources is in one output, `service_environment`: the environment
variables that point the service at them.

## Apply to AWS

```bash
cd infra/terraform
terraform init
terraform apply
terraform output -json service_environment
```

Before a team shares this, configure a remote state backend (an S3 bucket, for example). Right now the state stays in
a local file.

The provider is pinned to the 5.x line. That matches the LocalStack 4.6 image that CI applies this configuration to.
`.terraform.lock.hcl` records the exact version, 5.100.0, with its checksums for Linux, macOS and Windows, so every
`terraform init` installs the same verified build. After changing the provider version, regenerate it:

```bash
terraform providers lock -platform=linux_amd64 -platform=linux_arm64 \
  -platform=darwin_amd64 -platform=darwin_arm64 -platform=windows_amd64
```

Moving to 6.x means bumping the pin, re-locking, and letting CI run.

## Try it on LocalStack

CI does this on every push. It applies the configuration to LocalStack, starts the service with the outputs, and runs
[`scripts/smoke-test.sh`](../../scripts/smoke-test.sh). To do the same by hand:

```bash
docker compose up -d localstack
pipx install terraform-local        # provides tflocal, a terraform wrapper that targets LocalStack
cd infra/terraform
tflocal init && tflocal apply -auto-approve
eval "$(terraform output -json service_environment | jq -r 'to_entries[] | "export \(.key)=\(.value)"')"
cd ../..
PIPELINE_AWS_ENDPOINT=http://localhost:4566 PIPELINE_AWS_ACCESSKEY=test PIPELINE_AWS_SECRETKEY=test ./gradlew bootRun
./scripts/smoke-test.sh             # in a second terminal
```
