# ------------------------------------------------------------------ Redshift Serverless
# Billed only while queries run (per RPU-second). base_capacity 8 is the minimum.
resource "aws_redshiftserverless_namespace" "this" {
  namespace_name        = var.project
  db_name               = "dev"
  admin_username        = "admin"
  manage_admin_password = true # password generated and stored in Secrets Manager
  iam_roles             = [aws_iam_role.redshift_copy.arn]
  default_iam_role_arn  = aws_iam_role.redshift_copy.arn
}

resource "aws_redshiftserverless_workgroup" "this" {
  namespace_name      = aws_redshiftserverless_namespace.this.namespace_name
  workgroup_name      = var.project
  base_capacity       = var.redshift_base_rpu
  publicly_accessible = false
}

# Role Redshift assumes to COPY Parquet from the gold prefix (read-only, gold only)
data "aws_iam_policy_document" "redshift_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com", "redshift-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "redshift_copy" {
  name               = "${var.project}-redshift-copy"
  assume_role_policy = data.aws_iam_policy_document.redshift_assume.json
}

data "aws_iam_policy_document" "redshift_copy" {
  statement {
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/lake/gold/*"]
  }
}

resource "aws_iam_role_policy" "redshift_copy" {
  role   = aws_iam_role.redshift_copy.id
  policy = data.aws_iam_policy_document.redshift_copy.json
}
