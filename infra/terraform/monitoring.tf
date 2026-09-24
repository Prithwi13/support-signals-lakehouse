# ------------------------------------------------------------------ alerting
resource "aws_sns_topic" "alerts" {
  name = "${var.project}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# Any blocking data quality check failed in a run
resource "aws_cloudwatch_metric_alarm" "dq_errors" {
  alarm_name          = "${var.project}-dq-error-checks-failed"
  alarm_description   = "A severity=error data quality check failed; the pipeline stopped before publishing bad data."
  namespace           = "SupportSignals/DataQuality"
  metric_name         = "ErrorChecksFailed"
  statistic           = "Maximum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

# Freshness: no DQ metrics at all for 24h means the daily run didn't happen
resource "aws_cloudwatch_metric_alarm" "pipeline_stale" {
  alarm_name          = "${var.project}-pipeline-stale"
  alarm_description   = "No pipeline run published DQ metrics in the last 24 hours."
  namespace           = "SupportSignals/DataQuality"
  metric_name         = "ErrorChecksFailed"
  statistic           = "SampleCount"
  period              = 86400
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

# Glue job failures -> SNS via EventBridge
resource "aws_cloudwatch_event_rule" "glue_failed" {
  name = "${var.project}-glue-failed"
  event_pattern = jsonencode({
    source      = ["aws.glue"]
    detail-type = ["Glue Job State Change"]
    detail      = { jobName = [aws_glue_job.spark.name], state = ["FAILED", "TIMEOUT", "ERROR"] }
  })
}

resource "aws_cloudwatch_event_target" "glue_failed" {
  rule = aws_cloudwatch_event_rule.glue_failed.name
  arn  = aws_sns_topic.alerts.arn
}

data "aws_iam_policy_document" "sns_events" {
  statement {
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com", "cloudwatch.amazonaws.com"]
    }
  }
}

resource "aws_sns_topic_policy" "alerts" {
  arn    = aws_sns_topic.alerts.arn
  policy = data.aws_iam_policy_document.sns_events.json
}

# ------------------------------------------------------------------ cost guardrail (account-wide)
resource "aws_budgets_budget" "monthly" {
  count        = var.alert_email == "" ? 0 : 1
  name         = "${var.project}-monthly"
  budget_type  = "COST"
  limit_amount = var.monthly_budget_usd
  limit_unit   = "USD"
  time_unit    = "MONTHLY"
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}
