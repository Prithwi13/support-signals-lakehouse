-- Point-in-time ("as of") query powered by the SCD2 dimension:
-- how many issues per service were open at the end of each week, using the
-- attribute values that were true at that moment (not today's values).
CREATE OR REPLACE VIEW marts.open_backlog_asof_weekly AS
WITH weeks AS (
    SELECT DISTINCT CAST(calendar_date AS TIMESTAMP) + INTERVAL '1 day' AS asof_ts
    FROM gold.dim_date
    WHERE day_of_week = 1                       -- Sundays
      AND calendar_date <= (SELECT MAX(CAST(valid_from AS DATE)) FROM gold.dim_issue_scd2)
      AND calendar_date >= (SELECT MIN(CAST(valid_from AS DATE)) FROM gold.dim_issue_scd2)
)
SELECT
    w.asof_ts,
    v.service_slug,
    COUNT(*) AS open_issues_asof
FROM weeks w
JOIN gold.dim_issue_scd2 v
  ON v.valid_from <= w.asof_ts AND w.asof_ts < v.valid_to
WHERE v.state = 'open'
GROUP BY 1, 2;
