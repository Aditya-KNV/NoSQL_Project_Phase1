# ─── loader/db_loader.py ─────────────────────────────────────────────────────
"""
Creates the reporting tables (if not present) and inserts query results
into PostgreSQL, tagged with pipeline name, run_id, batch_id, and timestamp.
"""

import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import uuid


def get_connection(pg_config: dict):
    return psycopg2.connect(**pg_config)


def init_schema(pg_config: dict):
    """Create reporting tables if they do not exist."""
    ddl = """
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

    CREATE TABLE IF NOT EXISTS q1_daily_traffic (
        id          SERIAL PRIMARY KEY,
        run_id      TEXT,
        pipeline    TEXT,
        batch_id    INT,
        executed_at TIMESTAMP,
        log_date    TEXT,
        status_code INT,
        request_count BIGINT,
        total_bytes   BIGINT
    );

    CREATE TABLE IF NOT EXISTS q2_top_resources (
        id                SERIAL PRIMARY KEY,
        run_id            TEXT,
        pipeline          TEXT,
        batch_id          INT,
        executed_at       TIMESTAMP,
        resource_path     TEXT,
        request_count     BIGINT,
        total_bytes       BIGINT,
        distinct_host_count BIGINT
    );

    CREATE TABLE IF NOT EXISTS q3_hourly_errors (
        id                  SERIAL PRIMARY KEY,
        run_id              TEXT,
        pipeline            TEXT,
        batch_id            INT,
        executed_at         TIMESTAMP,
        log_date            TEXT,
        log_hour            INT,
        error_request_count BIGINT,
        total_request_count BIGINT,
        error_rate          FLOAT,
        distinct_error_hosts BIGINT
    );
    """
    conn = get_connection(pg_config)
    cur  = conn.cursor()
    cur.execute(ddl)
    conn.commit()
    cur.close()
    conn.close()
    print("[DB] Schema initialised.")


def new_run_id() -> str:
    return str(uuid.uuid4())


def save_run_metadata(pg_config, run_id, pipeline, started_at, finished_at,
                      total_records, total_batches, batch_size, malformed_count):
    runtime = (finished_at - started_at).total_seconds()
    avg_batch = total_records / total_batches if total_batches else 0

    sql = """
    INSERT INTO run_metadata
        (run_id, pipeline, started_at, finished_at, runtime_seconds,
         total_records, total_batches, batch_size, avg_batch_size, malformed_count)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (run_id) DO UPDATE SET
        finished_at     = EXCLUDED.finished_at,
        runtime_seconds = EXCLUDED.runtime_seconds,
        total_records   = EXCLUDED.total_records,
        total_batches   = EXCLUDED.total_batches,
        avg_batch_size  = EXCLUDED.avg_batch_size,
        malformed_count = EXCLUDED.malformed_count;
    """
    conn = get_connection(pg_config)
    cur  = conn.cursor()
    cur.execute(sql, (run_id, pipeline, started_at, finished_at, runtime,
                      total_records, total_batches, batch_size, avg_batch,
                      malformed_count))
    conn.commit()
    cur.close()
    conn.close()


def save_q1(pg_config, run_id, pipeline, batch_id, rows):
    """rows: list of (log_date, status_code, request_count, total_bytes)"""
    if not rows:
        return
    now = datetime.utcnow()
    data = [(run_id, pipeline, batch_id, now, r[0], r[1], r[2], r[3]) for r in rows]
    sql  = """
    INSERT INTO q1_daily_traffic
        (run_id, pipeline, batch_id, executed_at, log_date, status_code, request_count, total_bytes)
    VALUES %s
    """
    conn = get_connection(pg_config)
    cur  = conn.cursor()
    execute_values(cur, sql, data)
    conn.commit()
    cur.close()
    conn.close()


def save_q2(pg_config, run_id, pipeline, batch_id, rows):
    """rows: list of (resource_path, request_count, total_bytes, distinct_host_count)"""
    if not rows:
        return
    now  = datetime.utcnow()
    data = [(run_id, pipeline, batch_id, now, r[0], r[1], r[2], r[3]) for r in rows]
    sql  = """
    INSERT INTO q2_top_resources
        (run_id, pipeline, batch_id, executed_at, resource_path, request_count, total_bytes, distinct_host_count)
    VALUES %s
    """
    conn = get_connection(pg_config)
    cur  = conn.cursor()
    execute_values(cur, sql, data)
    conn.commit()
    cur.close()
    conn.close()


def save_q3(pg_config, run_id, pipeline, batch_id, rows):
    """rows: list of (log_date, log_hour, error_req_count, total_req_count, error_rate, distinct_error_hosts)"""
    if not rows:
        return
    now  = datetime.utcnow()
    data = [(run_id, pipeline, batch_id, now, r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows]
    sql  = """
    INSERT INTO q3_hourly_errors
        (run_id, pipeline, batch_id, executed_at, log_date, log_hour,
         error_request_count, total_request_count, error_rate, distinct_error_hosts)
    VALUES %s
    """
    conn = get_connection(pg_config)
    cur  = conn.cursor()
    execute_values(cur, sql, data)
    conn.commit()
    cur.close()
    conn.close()
