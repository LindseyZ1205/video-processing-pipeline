#!/usr/bin/env bash
# Moves the messages in the dead-letter queue back to the event queue with an SQS message move task, and waits until
# the move is done. Run it once the cause of the failures is fixed (docs/runbook.md). Uses the AWS CLI v2 and its usual
# credentials; against LocalStack, set AWS_ENDPOINT_URL=http://localhost:4566. Needs jq.
#
#   scripts/redrive-dlq.sh [event-queue-name] [dead-letter-queue-name]
#
# The names default to PIPELINE_QUEUE_NAME and PIPELINE_QUEUE_DEADLETTERNAME, the variables Terraform outputs in
# service_environment, and then to the service's defaults.
set -euo pipefail

queue="${1:-${PIPELINE_QUEUE_NAME:-video-upload-events}}"
dead_letter_queue="${2:-${PIPELINE_QUEUE_DEADLETTERNAME:-$queue-dlq}}"

queue_url() {
  aws sqs get-queue-url --queue-name "$1" --query QueueUrl --output text
}

queue_attribute() {
  aws sqs get-queue-attributes --queue-url "$1" --attribute-names "$2" --query "Attributes.$2" --output text
}

dead_letter_url="$(queue_url "$dead_letter_queue")"
waiting="$(queue_attribute "$dead_letter_url" ApproximateNumberOfMessages)"
if [ "$waiting" = "0" ]; then
  echo "$dead_letter_queue has no messages waiting; nothing to redrive"
  exit 0
fi

source_arn="$(queue_attribute "$dead_letter_url" QueueArn)"
destination_arn="$(queue_attribute "$(queue_url "$queue")" QueueArn)"
echo "moving about $waiting message(s) from $dead_letter_queue back to $queue"
aws sqs start-message-move-task --source-arn "$source_arn" --destination-arn "$destination_arn" > /dev/null

status=""
for _ in $(seq 300); do
  task="$(aws sqs list-message-move-tasks --source-arn "$source_arn" --max-results 1 --query 'Results[0]' --output json)"
  status="$(jq -r .Status <<< "$task")"
  case "$status" in
    COMPLETED)
      echo "moved $(jq -r .ApproximateNumberOfMessagesMoved <<< "$task") message(s); the worker picks them up as usual"
      exit 0
      ;;
    FAILED | CANCELLED)
      echo "the move task ended $status: $(jq -r '.FailureReason // "no reason given"' <<< "$task")" >&2
      exit 1
      ;;
  esac
  sleep 1
done

echo "the move task is still ${status:-running} after 5 minutes; follow it with:" >&2
echo "  aws sqs list-message-move-tasks --source-arn $source_arn" >&2
exit 1
