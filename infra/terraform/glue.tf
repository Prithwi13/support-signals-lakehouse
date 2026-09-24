# ------------------------------------------------------------------ IAM for Glue
data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${var.project}-glue"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_access" {
  statement {
    sid       = "LakeReadWrite"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
  }
  statement {
    sid       = "DataQualityMetrics"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["SupportSignals/DataQuality"]
    }
  }
}

resource "aws_iam_role_policy" "glue_access" {
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_access.json
}

# ------------------------------------------------------------------ one Glue job, any Spark stage
resource "aws_glue_job" "spark" {
  name              = "${var.project}-spark"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0" # Spark 3.5, Python 3.11
  worker_type       = "G.1X"
  number_of_workers = var.glue_workers
  timeout           = 60
  max_retries       = 0 # Airflow owns retries, so failures aren't retried twice

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lake.id}/${aws_s3_object.glue_entrypoint.key}"
  }

  execution_property { max_concurrent_runs = 1 }

  default_arguments = {
    "--extra-py-files"                   = "s3://${aws_s3_bucket.lake.id}/${aws_s3_object.package.key}"
    "--extra-files"                      = join(",", [for o in aws_s3_object.config : "s3://${aws_s3_bucket.lake.id}/${o.key}"])
    "--additional-python-modules"        = "pyyaml,scikit-learn==1.5.2"
    "--storage_root"                     = local.data_root
    "--state_root"                       = local.state_root
    "--stage_args"                       = "silver"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-job-insights"              = "true"
    "--job-language"                     = "python"
  }
}
