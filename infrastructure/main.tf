terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.90"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

resource "aws_s3_bucket" "pipeline" {
  bucket = var.s3_bucket_name
}

resource "aws_s3_bucket_versioning" "pipeline" {
  bucket = aws_s3_bucket.pipeline.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "pipeline" {
  bucket = aws_s3_bucket.pipeline.id

  rule {
    id     = "archive-after-90-days"
    status = "Enabled"

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    filter {}
  }
}

resource "aws_sqs_queue" "dlq" {
  name = "${var.project_name}-dlq"
}

resource "aws_sns_topic" "pipeline" {
  name = "${var.project_name}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.pipeline.arn
  protocol  = "email"
  endpoint  = var.notification_email
}

resource "aws_iam_role" "lambda_role" {
  name = "${var.project_name}-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "lambda_policy" {
  name = "${var.project_name}-lambda-policy"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.pipeline.arn,
          "${aws_s3_bucket.pipeline.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "sns:Publish"
        ]
        Resource = [aws_sns_topic.pipeline.arn]
      },
      {
        Effect = "Allow"
        Action = [
          "sqs:SendMessage"
        ]
        Resource = [aws_sqs_queue.dlq.arn]
      }
    ]
  })
}

resource "aws_lambda_function" "trigger_pipeline" {
  function_name = "${var.project_name}-trigger"
  role          = aws_iam_role.lambda_role.arn
  runtime       = "python3.11"
  handler       = "src.cloud.lambda_handler.lambda_handler"
  filename      = var.lambda_package_path
  timeout       = 900
  memory_size   = 1024

  dead_letter_config {
    target_arn = aws_sqs_queue.dlq.arn
  }

  environment {
    variables = {
      AWS_REGION      = var.aws_region
      S3_BUCKET       = aws_s3_bucket.pipeline.bucket
      SEC_USER_AGENT  = var.sec_user_agent
      COMPANY_TICKER  = "SNOW"
      COMPANY_CIK     = "0001640147"
      RAW_PREFIX      = "raw/snowflake/10k"
      PROCESSED_PREFIX = "processed/snowflake/10k"
      MODELS_PREFIX   = "models/snowflake"
      LOGS_PREFIX     = "logs/snowflake"
      SNS_TOPIC_ARN   = aws_sns_topic.pipeline.arn
    }
  }
}

resource "aws_cloudwatch_event_rule" "pipeline_schedule" {
  name                = "${var.project_name}-schedule"
  schedule_expression = var.schedule_expression
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.pipeline_schedule.name
  target_id = "TriggerPipelineLambda"
  arn       = aws_lambda_function.trigger_pipeline.arn
}

resource "aws_lambda_permission" "allow_events" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.trigger_pipeline.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.pipeline_schedule.arn
}

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "${var.project_name}-lambda-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_actions       = [aws_sns_topic.pipeline.arn]

  dimensions = {
    FunctionName = aws_lambda_function.trigger_pipeline.function_name
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_duration" {
  alarm_name          = "${var.project_name}-lambda-duration"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Duration"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Average"
  threshold           = 600000
  alarm_actions       = [aws_sns_topic.pipeline.arn]

  dimensions = {
    FunctionName = aws_lambda_function.trigger_pipeline.function_name
  }
}

resource "aws_cloudwatch_dashboard" "pipeline" {
  dashboard_name = "${var.project_name}-dashboard"
  dashboard_body = jsonencode({
    widgets = [
      {
        type = "metric"
        x    = 0
        y    = 0
        width = 12
        height = 6
        properties = {
          metrics = [["AWS/Lambda", "Invocations", "FunctionName", aws_lambda_function.trigger_pipeline.function_name], [".", "Errors", ".", "."]]
          period  = 300
          stat    = "Sum"
          title   = "Pipeline Success vs Errors"
        }
      },
      {
        type = "metric"
        x    = 12
        y    = 0
        width = 12
        height = 6
        properties = {
          metrics = [["AWS/Lambda", "Duration", "FunctionName", aws_lambda_function.trigger_pipeline.function_name]]
          period  = 300
          stat    = "Average"
          title   = "Pipeline Runtime"
        }
      }
    ]
  })
}
