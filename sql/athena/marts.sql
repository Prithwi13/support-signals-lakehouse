-- Mart views for Athena (Trino SQL). Same metrics as sql/marts/*.sql (DuckDB/Redshift);
-- Trino uses approx_percentile instead of PERCENTILE_CONT.
CREATE OR REPLACE VIEW {db}.marts_service_health_monthly AS
SELECT d.year_month, s.service_slug,
       COUNT(*) AS issues_opened,
       SUM(IF(f.is_closed, 1, 0)) AS issues_closed_to_date,
       approx_percentile(f.hours_to_first_response, 0.5) AS median_hours_to_first_response,
       approx_percentile(f.hours_to_first_response, 0.9) AS p90_hours_to_first_response,
       approx_percentile(f.hours_to_close, 0.5) AS median_hours_to_close,
       AVG(IF(f.first_response_sla_breached, 1.0, 0.0)) AS first_response_sla_breach_rate,
       AVG(IF(f.is_bug, 1.0, 0.0)) AS bug_share
FROM {db}.fact_issue f
JOIN {db}.dim_date d ON d.date_key = f.created_date_key
JOIN {db}.dim_service s ON s.service_key = f.service_key
GROUP BY 1, 2;

CREATE OR REPLACE VIEW {db}.marts_backlog_by_service AS
SELECT s.service_slug, r.repo,
       COUNT(*) AS open_issues,
       SUM(IF(f.first_response_at IS NULL, 1, 0)) AS open_awaiting_first_response,
       ROUND(AVG(f.open_age_hours) / 24.0, 1) AS avg_open_age_days,
       ROUND(MAX(f.open_age_hours) / 24.0, 1) AS oldest_open_days
FROM {db}.fact_issue f
JOIN {db}.dim_service s ON s.service_key = f.service_key
JOIN {db}.dim_repo r ON r.repo_key = f.repo_key
WHERE NOT f.is_closed
GROUP BY 1, 2;

CREATE OR REPLACE VIEW {db}.marts_repeat_issue_rate AS
WITH dup AS (SELECT DISTINCT issue_id FROM {db}.bridge_issue_label WHERE label LIKE '%duplicate%')
SELECT d.year_month, s.service_slug,
       COUNT(*) AS closed_issues,
       SUM(IF(dup.issue_id IS NOT NULL, 1, 0)) AS duplicate_issues,
       AVG(IF(dup.issue_id IS NOT NULL, 1.0, 0.0)) AS repeat_issue_rate
FROM {db}.fact_issue f
JOIN {db}.dim_date d ON d.date_key = f.closed_date_key
JOIN {db}.dim_service s ON s.service_key = f.service_key
LEFT JOIN dup ON dup.issue_id = f.issue_id
WHERE f.is_closed
GROUP BY 1, 2;

CREATE OR REPLACE VIEW {db}.marts_open_backlog_asof_weekly AS
WITH bounds AS (SELECT MIN(CAST(valid_from AS date)) lo, MAX(CAST(valid_from AS date)) hi FROM {db}.dim_issue_scd2),
weeks AS (
  SELECT DISTINCT CAST(d.calendar_date AS timestamp) + INTERVAL '1' DAY AS asof_ts
  FROM {db}.dim_date d CROSS JOIN bounds b
  WHERE d.day_of_week = 1 AND d.calendar_date BETWEEN b.lo AND b.hi
)
SELECT w.asof_ts, v.service_slug, COUNT(*) AS open_issues_asof
FROM weeks w
JOIN {db}.dim_issue_scd2 v ON v.valid_from <= w.asof_ts AND w.asof_ts < v.valid_to
WHERE v.state = 'open'
GROUP BY 1, 2;
