variable "region" {
  description = "AWS region for every resource."
  type        = string
  default     = "us-east-1"
}

variable "bucket_prefix" {
  description = "Prefix for the upload bucket's name. Bucket names are global, so AWS appends a unique suffix."
  type        = string
  default     = "video-uploads-"

  validation {
    condition     = length(var.bucket_prefix) <= 37
    error_message = "bucket_prefix can be at most 37 characters."
  }
}

variable "queue_name" {
  description = "SQS queue that receives upload events. The dead-letter queue is named \"<queue_name>-dlq\"."
  type        = string
  default     = "video-upload-events"
}

variable "jobs_table_name" {
  description = "DynamoDB table that holds job state."
  type        = string
  default     = "video-processing-jobs"
}

variable "visibility_timeout_seconds" {
  description = "How long a received message stays hidden. The service uses the same value as its job lease and retry delay."
  type        = number
  default     = 60

  validation {
    condition     = var.visibility_timeout_seconds >= 1 && var.visibility_timeout_seconds <= 43200
    error_message = "SQS allows a visibility timeout between 1 second and 12 hours."
  }
}

variable "max_receive_count" {
  description = "Deliveries before SQS moves a message to the dead-letter queue."
  type        = number
  default     = 3

  validation {
    condition     = var.max_receive_count >= 1 && var.max_receive_count <= 1000
    error_message = "SQS allows a maxReceiveCount between 1 and 1000."
  }
}

variable "allowed_upload_origins" {
  description = "Browser origins allowed to PUT files with presigned URLs (the bucket's CORS rule)."
  type        = list(string)
  default     = ["http://localhost:8080"]
}

variable "upload_retention_days" {
  description = "Days before an S3 lifecycle rule deletes uploaded files."
  type        = number
  default     = 30
}
