-- Load one published gold snapshot into Redshift.
-- Template variables (filled in by src/signals/warehouse/load_redshift.py):
--   {table}      target table name, e.g. fact_issue
--   {s3_uri}     s3://<bucket>/gold/<table>/v=<run_id>/
--   {iam_role}   ARN of the role Redshift assumes to read S3
-- Pattern: COPY into a staging copy, then swap inside one transaction so
-- dashboards never see a half-loaded table.
DROP TABLE IF EXISTS staging.{table};
CREATE TABLE staging.{table} (LIKE gold.{table});
COPY staging.{table} FROM '{s3_uri}' IAM_ROLE '{iam_role}' FORMAT AS PARQUET;
BEGIN;
DELETE FROM gold.{table};
INSERT INTO gold.{table} SELECT * FROM staging.{table};
COMMIT;
DROP TABLE staging.{table};
