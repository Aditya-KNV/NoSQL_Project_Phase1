-- ─── sql/schema.sql ──────────────────────────────────────────────────────────
-- Run this once to create the reporting database and tables.
-- psql -U postgres -f sql/schema.sql

CREATE DATABASE IF NOT EXISTS nosql_etl;
\c nosql_etl;

CREATE TABLE IF NOT EXISTS run_metadata (
    run_id          TEXT PRIMARY KEY,
    pipeline        TEXT NOT NULL,
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP,
    runtime_seconds FLOAT,
    total_records   BIGINT,
    total_batches   INT,
    batch_size      INT,
    avg_batch_size  FLOAT,
    malformed_count BIGINT
);

-- Q1: Daily Traffic Summary
CREATE TABLE IF NOT EXISTS q1_daily_traffic (
    id            SERIAL PRIMARY KEY,
    run_id        TEXT,
    pipeline      TEXT,
    batch_id      INT,
    executed_at   TIMESTAMP,
    log_date      TEXT,
    status_code   INT,
    request_count BIGINT,
    total_bytes   BIGINT
);

-- Q2: Top Requested Resources
CREATE TABLE IF NOT EXISTS q2_top_resources (
    id                  SERIAL PRIMARY KEY,
    run_id              TEXT,
    pipeline            TEXT,
    batch_id            INT,
    executed_at         TIMESTAMP,
    resource_path       TEXT,
    request_count       BIGINT,
    total_bytes         BIGINT,
    distinct_host_count BIGINT
);

-- Q3: Hourly Error Analysis
CREATE TABLE IF NOT EXISTS q3_hourly_errors (
    id                   SERIAL PRIMARY KEY,
    run_id               TEXT,
    pipeline             TEXT,
    batch_id             INT,
    executed_at          TIMESTAMP,
    log_date             TEXT,
    log_hour             INT,
    error_request_count  BIGINT,
    total_request_count  BIGINT,
    error_rate           FLOAT,
    distinct_error_hosts BIGINT
);
