resource "aws_dynamodb_table" "jobs" {
  name         = var.jobs_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "jobId"

  attribute {
    name = "jobId"
    type = "S"
  }

  # The service stamps every job with an expiry (epoch seconds); DynamoDB deletes expired items at no cost.
  ttl {
    attribute_name = "expiresAt"
    enabled        = true
  }
}
