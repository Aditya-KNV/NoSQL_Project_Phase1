# ─── loader/db_loader.py ─────────────────────────────────────────────────────
"""
Creates the reporting tables (if not present) and inserts query results
into PostgreSQL, tagged with pipeline name, run_id, batch_id, and timestamp.

Phase 2 additions (marked NEW):
  - save_batch_metadata()  — per-batch tracking row
  - batch_id is now TEXT ("batch-001") instead of INT
"""

import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import uuid


def get_connection(pg_config: dict):
    return psycopg2.connect(**pg_config)


def init_schema(pg_config: dict):
    """Create reporting tables if they do not exist."""
    conn = get_connection(pg_config)
    cur  = conn.cursor()
    with open("sql/schema.sql", "r") as f:
        # skip the CREATE DATABASE and \c lines — psycopg2 connects directly
        ddl = "\n".join(
            line for line in f
            if not line.strip().startswith("CREATE DATABASE")
            and not line.strip().startswith("\\c")
        )
    cur.execute(ddl)
    conn.commit()
    cur.close()
    conn.close()
    print("[DB] Schema initialised.")


def new_run_id() -> str:
    return str(uuid.uuid4())


# ── run_metadata ──────────────────────────────────────────────────────────────
# unchanged from Phase 1

def save_run_metadata(pg_config, run_id, pipeline, started_at, finished_at,
                      total_records, total_batches, batch_size, malformed_count):
    runtime   = (finished_at - started_at).total_seconds()
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


# ── batch_metadata — NEW Phase 2 ─────────────────────────────────────────────

def save_batch_metadata(pg_config, run_id, pipeline, batch_id, batch_date,
                        source_file, batch_size, avg_batch_size,
                        malformed_count, runtime_seconds,
                        batch_start_ts, batch_end_ts):
    sql = """
    INSERT INTO batch_metadata
        (run_id, pipeline, batch_id, batch_date, source_file,
         batch_size, avg_batch_size, records_processed,
         malformed_count, runtime_seconds, batch_start_ts, batch_end_ts)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    conn = get_connection(pg_config)
    cur  = conn.cursor()
    cur.execute(sql, (run_id, pipeline, batch_id, batch_date, source_file,
                      batch_size, avg_batch_size, batch_size,
                      malformed_count, runtime_seconds,
                      batch_start_ts, batch_end_ts))
    conn.commit()
    cur.close()
    conn.close()


# ── q1_daily_traffic ──────────────────────────────────────────────────────────
# unchanged from Phase 1 except batch_id is now TEXT

def save_q1(pg_config, run_id, pipeline, batch_id, rows):
    """rows: list of (log_date, status_code, request_count, total_bytes)"""
    if not rows:
        return
    now  = datetime.utcnow()
    data = [(run_id, pipeline, batch_id, now, r[0], r[1], r[2], r[3]) for r in rows]
    sql  = """
    INSERT INTO q1_daily_traffic
        (run_id, pipeline, batch_id, executed_at,
         log_date, status_code, request_count, total_bytes)
    VALUES %s
    """
    conn = get_connection(pg_config)
    cur  = conn.cursor()
    execute_values(cur, sql, data)
    conn.commit()
    cur.close()
    conn.close()


# ── q2_top_resources ──────────────────────────────────────────────────────────
# unchanged from Phase 1 except batch_id is now TEXT

def save_q2(pg_config, run_id, pipeline, batch_id, rows):
    """rows: list of (resource_path, request_count, total_bytes, distinct_host_count)"""
    if not rows:
        return
    now  = datetime.utcnow()
    data = [(run_id, pipeline, batch_id, now, r[0], r[1], r[2], r[3]) for r in rows]
    sql  = """
    INSERT INTO q2_top_resources
        (run_id, pipeline, batch_id, executed_at,
         resource_path, request_count, total_bytes, distinct_host_count)
    VALUES %s
    """
    conn = get_connection(pg_config)
    cur  = conn.cursor()
    execute_values(cur, sql, data)
    conn.commit()
    cur.close()
    conn.close()


# ── q3_hourly_errors ──────────────────────────────────────────────────────────
# unchanged from Phase 1 except batch_id is now TEXT

def save_q3(pg_config, run_id, pipeline, batch_id, rows):
    """rows: list of (log_date, log_hour, error_req_count, total_req_count,
                      error_rate, distinct_error_hosts)"""
    if not rows:
        return
    now  = datetime.utcnow()
    data = [(run_id, pipeline, batch_id, now,
             r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows]
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
