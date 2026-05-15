-- ─── sql/schema.sql ──────────────────────────────────────────────────────────
-- Run this once to create the reporting database and tables.
-- psql -U postgres -f sql/schema.sql

CREATE DATABASE IF NOT EXISTS nosql_etl;
\c nosql_etl;

-- unchanged from Phase 1 — only batch_size column kept for configured size
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

-- NEW Phase 2: one row per batch per run (batch_id is now a string e.g. "batch-001")
CREATE TABLE IF NOT EXISTS batch_metadata (
    id              SERIAL PRIMARY KEY,
    run_id          TEXT,
    pipeline        TEXT,
    batch_id        TEXT,
    batch_date      DATE,
    source_file     TEXT,
    batch_size      INT,
    avg_batch_size  FLOAT,
    records_processed INT,
    malformed_count INT,
    runtime_seconds FLOAT,
    batch_start_ts  TIMESTAMP,
    batch_end_ts    TIMESTAMP
);

-- Q1: Daily Traffic Summary — unchanged columns from Phase 1
-- batch_id changed INT → TEXT to hold "batch-001" style IDs
CREATE TABLE IF NOT EXISTS q1_daily_traffic (
    id            SERIAL PRIMARY KEY,
    run_id        TEXT,
    pipeline      TEXT,
    batch_id      TEXT,
    executed_at   TIMESTAMP,
    log_date      TEXT,
    status_code   INT,
    request_count BIGINT,
    total_bytes   BIGINT
);

-- Q2: Top Requested Resources — unchanged columns from Phase 1
CREATE TABLE IF NOT EXISTS q2_top_resources (
    id                  SERIAL PRIMARY KEY,
    run_id              TEXT,
    pipeline            TEXT,
    batch_id            TEXT,
    executed_at         TIMESTAMP,
    resource_path       TEXT,
    request_count       BIGINT,
    total_bytes         BIGINT,
    distinct_host_count BIGINT
);

-- Q3: Hourly Error Analysis — unchanged columns from Phase 1
CREATE TABLE IF NOT EXISTS q3_hourly_errors (
    id                   SERIAL PRIMARY KEY,
    run_id               TEXT,
    pipeline             TEXT,
    batch_id             TEXT,
    executed_at          TIMESTAMP,
    log_date             TEXT,
    log_hour             INT,
    error_request_count  BIGINT,
    total_request_count  BIGINT,
    error_rate           FLOAT,
    distinct_error_hosts BIGINT
);
