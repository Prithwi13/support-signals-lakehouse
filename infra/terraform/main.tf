terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.70" }
  }
  # For team use, switch to an S3 backend with DynamoDB locking:
  # backend "s3" { bucket = "..." key = "support-signals/terraform.tfstate" region = "us-east-1" dynamodb_table = "tf-locks" }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { Project = var.project, ManagedBy = "terraform" }
  }
}

data "aws_caller_identity" "me" {}

locals {
  bucket     = "${var.project}-${data.aws_caller_identity.me.account_id}-${var.region}"
  data_root  = "s3://${local.bucket}/lake"
  state_root = "s3://${local.bucket}/lake/_state"
}

# ------------------------------------------------------------------ data lake bucket
resource "aws_s3_bucket" "lake" {
  bucket        = local.bucket
  force_destroy = var.force_destroy
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "bronze-to-infrequent-access"
    status = "Enabled"
    filter { prefix = "lake/bronze/" }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }
  rule {
    id     = "expire-old-snapshot-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration { noncurrent_days = 14 }
  }
}

# ------------------------------------------------------------------ code artifacts for Glue
resource "aws_s3_object" "glue_entrypoint" {
  bucket = aws_s3_bucket.lake.id
  key    = "artifacts/glue_entrypoint.py"
  source = "${path.module}/../../glue/glue_entrypoint.py"
  etag   = filemd5("${path.module}/../../glue/glue_entrypoint.py")
}

resource "aws_s3_object" "package" {
  bucket = aws_s3_bucket.lake.id
  key    = "artifacts/signals.zip"
  source = var.package_zip_path
  etag   = filemd5(var.package_zip_path)
}

resource "aws_s3_object" "config" {
  for_each = toset(["pipeline.yaml", "service_map.yaml", "quality.yaml"])
  bucket   = aws_s3_bucket.lake.id
  key      = "artifacts/config/${each.key}"
  source   = "${path.module}/../../config/${each.key}"
  etag     = filemd5("${path.module}/../../config/${each.key}")
}
