-- Repeat-issue rate: share of closed issues that were duplicates of an existing report.
-- High values point at missing docs / known issues customers can't find (self-service gap).
CREATE OR REPLACE VIEW marts.repeat_issue_rate AS
WITH dup AS (
    SELECT DISTINCT issue_id FROM gold.bridge_issue_label WHERE label LIKE '%duplicate%'
)
SELECT
    d.year_month,
    s.service_slug,
    COUNT(*)                                                    AS closed_issues,
    SUM(CASE WHEN dup.issue_id IS NOT NULL THEN 1 ELSE 0 END)   AS duplicate_issues,
    AVG(CASE WHEN dup.issue_id IS NOT NULL THEN 1.0 ELSE 0.0 END) AS repeat_issue_rate
FROM gold.fact_issue f
JOIN gold.dim_date    d ON d.date_key    = f.closed_date_key
JOIN gold.dim_service s ON s.service_key = f.service_key
LEFT JOIN dup ON dup.issue_id = f.issue_id
WHERE f.is_closed
GROUP BY 1, 2;
