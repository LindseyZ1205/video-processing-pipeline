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
    sid       = "ConsumeUploadEvents"
    actions   = ["sqs:GetQueueUrl", "sqs:ReceiveMessage", "sqs:DeleteMessage"]
    resources = [aws_sqs_queue.events.arn]
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
