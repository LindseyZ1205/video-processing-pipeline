# Runtime permissions for the service, and nothing more. Attach the policy to whatever role runs it
# (ECS task role, EC2 instance profile, ...).
data "aws_iam_policy_document" "service" {
  statement {
    sid = "SignUploadsAndReadThem"
    # PutObject: a presigned URL carries the signer's permissions, so the browser's upload needs it.
    # GetObject: HeadObject before processing, and the OpenAI provider downloads the file.
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["${aws_s3_bucket.uploads.arn}/uploads/*"]
  }

  statement {
    sid = "ConsumeUploadEvents"
    # ChangeMessageVisibility: the heartbeat keeps a message hidden while its transcription is still running.
    # GetQueueAttributes: the worker reads the redrive policy, so a failed last delivery doesn't wait out a backoff.
    # SendMessage: a throttled event is sent again with a delay, so the rate limit doesn't use up a delivery.
    actions = [
      "sqs:GetQueueUrl", "sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility",
      "sqs:GetQueueAttributes", "sqs:SendMessage",
    ]
    resources = [aws_sqs_queue.events.arn]
  }

  statement {
    sid       = "PublishQueueDepthMetrics"
    actions   = ["sqs:GetQueueUrl", "sqs:GetQueueAttributes"]
    resources = [aws_sqs_queue.events.arn, aws_sqs_queue.dead_letter.arn]
  }

  statement {
    sid       = "TrackJobs"
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem"]
    resources = [aws_dynamodb_table.jobs.arn]
  }
}

resource "aws_iam_policy" "service" {
  name        = "video-processing-pipeline-service"
  description = "Runtime permissions for the video-processing-pipeline service"
  policy      = data.aws_iam_policy_document.service.json
}
