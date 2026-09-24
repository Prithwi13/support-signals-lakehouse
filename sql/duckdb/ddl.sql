-- Local warehouse (DuckDB) — mirrors sql/redshift/ddl.sql so marts run on both.
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE OR REPLACE TABLE gold.dim_date (
    date_key     INTEGER PRIMARY KEY,
    calendar_date DATE NOT NULL,
    year         INTEGER, quarter INTEGER, month INTEGER,
    year_month   VARCHAR, iso_week INTEGER, day_of_week INTEGER, is_weekend BOOLEAN
);

CREATE OR REPLACE TABLE gold.dim_repo (
    repo_key   BIGINT PRIMARY KEY,
    repo       VARCHAR NOT NULL,
    owner      VARCHAR, repo_name VARCHAR
);

CREATE OR REPLACE TABLE gold.dim_service (
    service_key  BIGINT PRIMARY KEY,
    service_slug VARCHAR NOT NULL,
    service_name VARCHAR
);

CREATE OR REPLACE TABLE gold.dim_issue_scd2 (
    issue_id       BIGINT NOT NULL,
    issue_key      VARCHAR NOT NULL,
    state          VARCHAR, labels_str VARCHAR, service_slug VARCHAR,
    milestone      VARCHAR, assignee_count INTEGER,
    valid_from     TIMESTAMP NOT NULL,
    valid_to       TIMESTAMP NOT NULL,
    is_current     BOOLEAN NOT NULL,
    attr_hash      VARCHAR, _run_id VARCHAR,
    PRIMARY KEY (issue_id, valid_from)
);

CREATE OR REPLACE TABLE gold.bridge_issue_label (
    issue_id BIGINT NOT NULL,
    label    VARCHAR NOT NULL
);

CREATE OR REPLACE TABLE gold.fact_issue (
    issue_id                  BIGINT PRIMARY KEY,
    issue_key                 VARCHAR,
    repo_key                  BIGINT NOT NULL REFERENCES gold.dim_repo(repo_key),
    service_key               BIGINT NOT NULL REFERENCES gold.dim_service(service_key),
    created_date_key          INTEGER NOT NULL REFERENCES gold.dim_date(date_key),
    closed_date_key           INTEGER REFERENCES gold.dim_date(date_key),
    created_at                TIMESTAMP, first_response_at TIMESTAMP, closed_at TIMESTAMP,
    state                     VARCHAR, is_closed BOOLEAN, is_bug BOOLEAN, is_feature_request BOOLEAN,
    comment_count             INTEGER, reaction_count INTEGER, assignee_count INTEGER,
    body_len                  INTEGER, has_code_block BOOLEAN, author_association VARCHAR,
    hours_to_first_response   DOUBLE, hours_to_close DOUBLE, open_age_hours DOUBLE,
    first_response_sla_breached BOOLEAN,
    snapshot_at               TIMESTAMP
);
