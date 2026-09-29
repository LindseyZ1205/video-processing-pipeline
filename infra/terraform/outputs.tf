output "bucket_name" {
  description = "Upload bucket, as generated from bucket_prefix."
  value       = aws_s3_bucket.uploads.id
}

output "queue_url" {
  description = "Upload event queue."
  value       = aws_sqs_queue.events.id
}

output "dead_letter_queue_url" {
  description = "Where upload events land after max_receive_count failed deliveries."
  value       = aws_sqs_queue.dead_letter.id
}

output "jobs_table_name" {
  description = "Job state table."
  value       = aws_dynamodb_table.jobs.name
}

output "service_policy_arn" {
  description = "IAM policy to attach to the service's role."
  value       = aws_iam_policy.service.arn
}

output "service_environment" {
  description = "Environment variables that point the service at these resources."
  value = {
    PIPELINE_AWS_REGION               = var.region
    PIPELINE_BUCKET                   = aws_s3_bucket.uploads.id
    PIPELINE_QUEUE_NAME               = aws_sqs_queue.events.name
    PIPELINE_QUEUE_DEADLETTERNAME     = aws_sqs_queue.dead_letter.name
    PIPELINE_JOBS_TABLE               = aws_dynamodb_table.jobs.name
    PIPELINE_WORKER_VISIBILITYTIMEOUT = "${var.visibility_timeout_seconds}s"
  }
}
