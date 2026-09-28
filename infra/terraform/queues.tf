resource "aws_sqs_queue" "dead_letter" {
  name                      = "${var.queue_name}-dlq"
  message_retention_seconds = 1209600 # 14 days, the maximum: time to inspect and redrive
  sqs_managed_sse_enabled   = true
}

resource "aws_sqs_queue" "events" {
  name                       = var.queue_name
  visibility_timeout_seconds = var.visibility_timeout_seconds
  receive_wait_time_seconds  = 20     # long polling for any consumer that doesn't set it
  message_retention_seconds  = 345600 # 4 days
  # SSE-SQS rather than a KMS key: S3 can publish to it without a key policy.
  sqs_managed_sse_enabled = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dead_letter.arn
    maxReceiveCount     = var.max_receive_count
  })
}

data "aws_caller_identity" "current" {}

# S3 needs explicit permission to publish to the queue, limited to the upload bucket in this account.
data "aws_iam_policy_document" "upload_events" {
  statement {
    sid       = "AllowUploadBucketToSendEvents"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.events.arn]

    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_s3_bucket.uploads.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_sqs_queue_policy" "events" {
  queue_url = aws_sqs_queue.events.id
  policy    = data.aws_iam_policy_document.upload_events.json
}
