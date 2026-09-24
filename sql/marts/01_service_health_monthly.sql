-- KPI mart: monthly support health per AWS service (feeds the dashboard's main view).
-- Written for both DuckDB and Redshift. Redshift requires every sort-based aggregate
-- (PERCENTILE_CONT/MEDIAN) in one SELECT to share the same ORDER BY column, so the
-- response-time and resolution-time percentiles are computed in separate CTEs.
CREATE OR REPLACE VIEW marts.service_health_monthly AS
WITH base AS (
    SELECT d.year_month, s.service_slug, f.*
    FROM gold.fact_issue f
    JOIN gold.dim_date    d ON d.date_key    = f.created_date_key
    JOIN gold.dim_service s ON s.service_key = f.service_key
),
counts AS (
    SELECT year_month, service_slug,
           COUNT(*)                                                    AS issues_opened,
           SUM(CASE WHEN is_closed THEN 1 ELSE 0 END)                  AS issues_closed_to_date,
           AVG(CASE WHEN first_response_sla_breached THEN 1.0 ELSE 0.0 END) AS first_response_sla_breach_rate,
           AVG(CASE WHEN is_bug THEN 1.0 ELSE 0.0 END)                 AS bug_share
    FROM base GROUP BY 1, 2
),
response AS (
    SELECT year_month, service_slug,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY hours_to_first_response) AS median_hours_to_first_response,
           PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY hours_to_first_response) AS p90_hours_to_first_response
    FROM base WHERE hours_to_first_response IS NOT NULL GROUP BY 1, 2
),
resolution AS (
    SELECT year_month, service_slug,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY hours_to_close) AS median_hours_to_close
    FROM base WHERE hours_to_close IS NOT NULL GROUP BY 1, 2
)
SELECT c.year_month, c.service_slug, c.issues_opened, c.issues_closed_to_date,
       r.median_hours_to_first_response, r.p90_hours_to_first_response,
       z.median_hours_to_close, c.first_response_sla_breach_rate, c.bug_share
FROM counts c
LEFT JOIN response   r ON r.year_month = c.year_month AND r.service_slug = c.service_slug
LEFT JOIN resolution z ON z.year_month = c.year_month AND z.service_slug = c.service_slug;
