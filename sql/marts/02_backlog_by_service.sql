-- Current open backlog per service/repo, including issues still waiting on a first reply.
CREATE OR REPLACE VIEW marts.backlog_by_service AS
SELECT
    s.service_slug,
    r.repo,
    COUNT(*)                                                           AS open_issues,
    SUM(CASE WHEN f.first_response_at IS NULL THEN 1 ELSE 0 END)       AS open_awaiting_first_response,
    ROUND(AVG(f.open_age_hours) / 24.0, 1)                             AS avg_open_age_days,
    ROUND(MAX(f.open_age_hours) / 24.0, 1)                             AS oldest_open_days
FROM gold.fact_issue f
JOIN gold.dim_service s ON s.service_key = f.service_key
JOIN gold.dim_repo    r ON r.repo_key    = f.repo_key
WHERE NOT f.is_closed
GROUP BY 1, 2;
