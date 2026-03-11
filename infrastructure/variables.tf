variable "project_name" {
  type        = string
  default     = "snowflake-10k-pipeline"
  description = "Project name prefix for AWS resources"
}

variable "aws_region" {
  type        = string
  default     = "us-east-1"
  description = "AWS region"
}

variable "s3_bucket_name" {
  type        = string
  description = "S3 bucket for pipeline data"
}

variable "sec_user_agent" {
  type        = string
  description = "SEC-compliant User-Agent string"
}

variable "lambda_package_path" {
  type        = string
  description = "Path to deployment zip for Lambda"
}

variable "notification_email" {
  type        = string
  description = "Email for SNS notifications"
}

variable "schedule_expression" {
  type        = string
  default     = "rate(1 year)"
  description = "EventBridge schedule for the pipeline"
}
