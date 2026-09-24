variable "region" {
  type    = string
  default = "us-east-1"
}

variable "project" {
  type    = string
  default = "support-signals"
}

variable "package_zip_path" {
  description = "Path to dist/signals.zip built by `make package`"
  type        = string
  default     = "../../dist/signals.zip"
}

variable "glue_workers" {
  type    = number
  default = 2
}

variable "redshift_base_rpu" {
  description = "Redshift Serverless base capacity (8 = minimum, cheapest)"
  type        = number
  default     = 8
}

variable "alert_email" {
  description = "Email for DQ/Glue alerts and the budget alarm (leave empty to skip)"
  type        = string
  default     = ""
}

variable "monthly_budget_usd" {
  type    = string
  default = "20"
}

variable "force_destroy" {
  description = "Allow `terraform destroy` to delete a non-empty bucket (handy for a portfolio project)"
  type        = bool
  default     = true
}
