-- Redshift Serverless DDL for the gold star schema.
-- Distribution/sort choices:
--   * small dimensions -> DISTSTYLE ALL (copied to every node, joins never shuffle)
--   * fact_issue       -> DISTKEY(service_key) co-locates the most common join/group-by,
--                         SORTKEY(created_date_key) makes date-range scans prune blocks
--   * dim_issue_scd2   -> DISTKEY(issue_id), SORTKEY(issue_id, valid_from) for as-of lookups
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key INTEGER NOT NULL PRIMARY KEY, calendar_date DATE NOT NULL,
    year INTEGER, quarter INTEGER, month INTEGER, year_month VARCHAR(7),
    iso_week INTEGER, day_of_week INTEGER, is_weekend BOOLEAN
) DISTSTYLE ALL SORTKEY (date_key);

CREATE TABLE IF NOT EXISTS gold.dim_repo (
    repo_key BIGINT NOT NULL PRIMARY KEY, repo VARCHAR(200) NOT NULL,
    owner VARCHAR(100), repo_name VARCHAR(100)
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS gold.dim_service (
    service_key BIGINT NOT NULL PRIMARY KEY, service_slug VARCHAR(100) NOT NULL, service_name VARCHAR(100)
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS gold.dim_issue_scd2 (
    issue_id BIGINT NOT NULL, issue_key VARCHAR(250) NOT NULL,
    state VARCHAR(16), labels_str VARCHAR(4000), service_slug VARCHAR(100),
    milestone VARCHAR(500), assignee_count INTEGER,
    valid_from TIMESTAMP NOT NULL, valid_to TIMESTAMP NOT NULL, is_current BOOLEAN NOT NULL,
    attr_hash CHAR(64), _run_id VARCHAR(64),
    PRIMARY KEY (issue_id, valid_from)
) DISTKEY (issue_id) SORTKEY (issue_id, valid_from);

CREATE TABLE IF NOT EXISTS gold.bridge_issue_label (
    issue_id BIGINT NOT NULL, label VARCHAR(200) NOT NULL
) DISTKEY (issue_id) SORTKEY (label);

CREATE TABLE IF NOT EXISTS gold.fact_issue (
    issue_id BIGINT NOT NULL PRIMARY KEY, issue_key VARCHAR(250),
    repo_key BIGINT NOT NULL REFERENCES gold.dim_repo(repo_key),
    service_key BIGINT NOT NULL REFERENCES gold.dim_service(service_key),
    created_date_key INTEGER NOT NULL REFERENCES gold.dim_date(date_key),
    closed_date_key INTEGER REFERENCES gold.dim_date(date_key),
    created_at TIMESTAMP, first_response_at TIMESTAMP, closed_at TIMESTAMP,
    state VARCHAR(16), is_closed BOOLEAN, is_bug BOOLEAN, is_feature_request BOOLEAN,
    comment_count INTEGER, reaction_count INTEGER, assignee_count INTEGER,
    body_len INTEGER, has_code_block BOOLEAN, author_association VARCHAR(32),
    hours_to_first_response DOUBLE PRECISION, hours_to_close DOUBLE PRECISION,
    open_age_hours DOUBLE PRECISION, first_response_sla_breached BOOLEAN,
    snapshot_at TIMESTAMP
) DISTKEY (service_key) SORTKEY (created_date_key);
-- Note: Redshift does not enforce PK/FK constraints; the planner uses them as hints,
-- and the pipeline's DQ checks (unique, foreign_key) are what actually enforce them.
