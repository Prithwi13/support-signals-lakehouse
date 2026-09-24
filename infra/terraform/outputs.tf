output "lake_bucket" { value = aws_s3_bucket.lake.id }
output "storage_root" { value = local.data_root }
output "state_root" { value = local.state_root }
output "glue_job_name" { value = aws_glue_job.spark.name }
output "redshift_workgroup" { value = one(aws_redshiftserverless_workgroup.this[*].workgroup_name) }
output "athena_workgroup" { value = aws_athena_workgroup.signals.name }
output "athena_database" { value = aws_glue_catalog_database.signals.name }
output "redshift_copy_role_arn" { value = aws_iam_role.redshift_copy.arn }
output "alerts_topic_arn" { value = aws_sns_topic.alerts.arn }

output "env_exports" {
  description = "Paste into your shell (or Airflow env) to run the pipeline against AWS"
  value       = <<-EOT
    export SIGNALS_RUN_MODE=aws
    export SIGNALS_STORAGE_ROOT=${local.data_root}
    export SIGNALS_STATE_ROOT=${local.state_root}
    export SIGNALS_GLUE_JOB=${aws_glue_job.spark.name}
    export ATHENA_WORKGROUP=${aws_athena_workgroup.signals.name}
    export ATHENA_DATABASE=${aws_glue_catalog_database.signals.name}
    export REDSHIFT_WORKGROUP=${coalesce(one(aws_redshiftserverless_workgroup.this[*].workgroup_name), "disabled")}
    export REDSHIFT_DATABASE=dev
    export REDSHIFT_COPY_ROLE_ARN=${aws_iam_role.redshift_copy.arn}
  EOT
}
