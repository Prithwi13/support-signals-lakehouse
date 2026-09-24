# ------------------------------------------------------------------ Athena (serverless SQL over the lake)
# The gold star schema is registered in the Glue Data Catalog and queried with Athena.
# Pay per TB scanned; the workgroup enforces a per-query scan cap as a cost guardrail.
resource "aws_glue_catalog_database" "signals" {
  name        = replace(var.project, "-", "_")
  description = "Support signals lakehouse: gold star schema + mart views"
}

resource "aws_athena_workgroup" "signals" {
  name          = var.project
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.id}/athena-results/"
      encryption_configuration { encryption_option = "SSE_S3" }
    }
  }
}
