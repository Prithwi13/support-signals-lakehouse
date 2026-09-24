-- Athena / Glue Data Catalog external tables over the published gold snapshots.
-- The db name and per-table snapshot locations are filled in by src/signals/warehouse/load_athena.py.
-- Each run re-points LOCATION at the newest snapshot (ALTER TABLE ... SET LOCATION),
-- which is the catalog-level version of the pipeline's atomic pointer flip.
CREATE EXTERNAL TABLE IF NOT EXISTS {db}.dim_date (
  date_key int, calendar_date date, year int, quarter int, month int,
  year_month string, iso_week int, day_of_week int, is_weekend boolean
) STORED AS PARQUET LOCATION '{loc_dim_date}';

CREATE EXTERNAL TABLE IF NOT EXISTS {db}.dim_repo (
  repo_key bigint, repo string, owner string, repo_name string
) STORED AS PARQUET LOCATION '{loc_dim_repo}';

CREATE EXTERNAL TABLE IF NOT EXISTS {db}.dim_service (
  service_key bigint, service_slug string, service_name string
) STORED AS PARQUET LOCATION '{loc_dim_service}';

CREATE EXTERNAL TABLE IF NOT EXISTS {db}.dim_issue_scd2 (
  issue_id bigint, issue_key string, state string, labels_str string, service_slug string,
  milestone string, assignee_count int, valid_from timestamp, valid_to timestamp,
  is_current boolean, attr_hash string, `_run_id` string
) STORED AS PARQUET LOCATION '{loc_dim_issue_scd2}';

CREATE EXTERNAL TABLE IF NOT EXISTS {db}.bridge_issue_label (
  issue_id bigint, label string
) STORED AS PARQUET LOCATION '{loc_bridge_issue_label}';

CREATE EXTERNAL TABLE IF NOT EXISTS {db}.fact_issue (
  issue_id bigint, issue_key string, repo_key bigint, service_key bigint,
  created_date_key int, closed_date_key int,
  created_at timestamp, first_response_at timestamp, closed_at timestamp,
  state string, is_closed boolean, is_bug boolean, is_feature_request boolean,
  comment_count int, reaction_count int, assignee_count int, body_len int,
  has_code_block boolean, author_association string,
  hours_to_first_response double, hours_to_close double, open_age_hours double,
  first_response_sla_breached boolean, snapshot_at timestamp
) STORED AS PARQUET LOCATION '{loc_fact_issue}';
