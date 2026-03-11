output "s3_bucket" {
  value       = aws_s3_bucket.pipeline.bucket
  description = "Pipeline S3 bucket"
}

output "lambda_function_name" {
  value       = aws_lambda_function.trigger_pipeline.function_name
  description = "Pipeline trigger Lambda"
}

output "sns_topic_arn" {
  value       = aws_sns_topic.pipeline.arn
  description = "SNS topic for pipeline notifications"
}

output "eventbridge_rule" {
  value       = aws_cloudwatch_event_rule.pipeline_schedule.name
  description = "EventBridge schedule rule name"
}
