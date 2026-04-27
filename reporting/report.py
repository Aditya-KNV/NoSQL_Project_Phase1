# ─── reporting/report.py ─────────────────────────────────────────────────────
"""
Reads results from PostgreSQL and displays a formatted report.
For MongoDB: shows final batch (cumulative full dataset).
For Pig: aggregates across all batches (since each batch is per-slice only).
"""

import psycopg2
from tabulate import tabulate


def _conn(pg_config):
    return psycopg2.connect(**pg_config)


def get_latest_run_id(pg_config, pipeline=None):
    sql = "SELECT run_id FROM run_metadata"
    params = []
    if pipeline:
        sql += " WHERE LOWER(pipeline) = LOWER(%s)"
        params.append(pipeline)
    sql += " ORDER BY started_at DESC LIMIT 1"
    conn = _conn(pg_config)
    cur  = conn.cursor()
    cur.execute(sql, params)
    row = cur.fetchone()
    cur.close(); conn.close()
    return row[0] if row else None


def show_report(pg_config, run_id=None, pipeline=None):
    if run_id is None:
        run_id = get_latest_run_id(pg_config, pipeline)
    if run_id is None:
        print("[Report] No runs found in the database.")
        return

    conn = _conn(pg_config)
    cur  = conn.cursor()

    # ── Run metadata ──────────────────────────────────────────────────────────
    cur.execute("""
        SELECT pipeline, run_id, started_at, finished_at,
               runtime_seconds, total_records, total_batches,
               batch_size, avg_batch_size, malformed_count
        FROM run_metadata WHERE run_id = %s
    """, (run_id,))
    meta = cur.fetchone()
    if not meta:
        print(f"[Report] Run ID {run_id} not found.")
        return

    (pipeline_name, rid, started, finished, runtime,
     total_rec, total_batch, batch_sz, avg_batch, malformed) = meta

    print("\n" + "="*70)
    print("  ETL RUN REPORT")
    print("="*70)
    print(f"  Pipeline       : {pipeline_name}")
    print(f"  Run ID         : {rid}")
    print(f"  Started        : {started}")
    print(f"  Finished       : {finished}")
    print(f"  Runtime (s)    : {runtime:.2f}")
    print(f"  Total Records  : {total_rec:,}")
    print(f"  Total Batches  : {total_batch}")
    print(f"  Batch Size     : {batch_sz:,}")
    print(f"  Avg Batch Size : {avg_batch:,.1f}")
    print(f"  Malformed Rows : {malformed:,}")
    print("="*70)

    is_pig = pipeline_name.lower() == "pig"

    # ── Q1: Daily Traffic ─────────────────────────────────────────────────────
    if is_pig:
        cur.execute("""
            SELECT log_date, status_code,
                   SUM(request_count) as request_count,
                   SUM(total_bytes) as total_bytes
            FROM q1_daily_traffic
            WHERE run_id = %s
            GROUP BY log_date, status_code
            ORDER BY log_date, status_code
            LIMIT 30
        """, (run_id,))
    else:
        max_batch = _max_batch(cur, "q1_daily_traffic", run_id)
        cur.execute("""
            SELECT log_date, status_code, request_count, total_bytes
            FROM q1_daily_traffic
            WHERE run_id = %s AND batch_id = %s
            ORDER BY log_date, status_code
            LIMIT 30
        """, (run_id, max_batch))
    rows = cur.fetchall()
    print("\n  Q1 — Daily Traffic Summary")
    print(tabulate(rows,
                   headers=["Log Date", "Status Code", "Request Count", "Total Bytes"],
                   tablefmt="pretty", intfmt=","))

    # ── Q2: Top Resources ─────────────────────────────────────────────────────
    if is_pig:
        cur.execute("""
            SELECT resource_path,
                   SUM(request_count) as request_count,
                   SUM(total_bytes) as total_bytes,
                   MAX(distinct_host_count) as distinct_host_count
            FROM q2_top_resources
            WHERE run_id = %s
            GROUP BY resource_path
            ORDER BY request_count DESC
            LIMIT 20
        """, (run_id,))
    else:
        max_batch = _max_batch(cur, "q2_top_resources", run_id)
        cur.execute("""
            SELECT resource_path, request_count, total_bytes, distinct_host_count
            FROM q2_top_resources
            WHERE run_id = %s AND batch_id = %s
            ORDER BY request_count DESC
            LIMIT 20
        """, (run_id, max_batch))
    rows = cur.fetchall()
    print("\n  Q2 — Top 20 Requested Resources")
    print(tabulate(rows,
                   headers=["Resource Path", "Request Count", "Total Bytes", "Distinct Hosts"],
                   tablefmt="pretty", intfmt=","))

    # ── Q3: Hourly Errors ─────────────────────────────────────────────────────
    if is_pig:
        cur.execute("""
            SELECT log_date, log_hour,
                   SUM(error_request_count) as error_request_count,
                   SUM(total_request_count) as total_request_count,
                   ROUND((SUM(error_request_count)::numeric /
                          NULLIF(SUM(total_request_count),0)), 4) as error_rate,
                   SUM(distinct_error_hosts) as distinct_error_hosts
            FROM q3_hourly_errors
            WHERE run_id = %s
            GROUP BY log_date, log_hour
            ORDER BY log_date, log_hour
            LIMIT 30
        """, (run_id,))
    else:
        max_batch = _max_batch(cur, "q3_hourly_errors", run_id)
        cur.execute("""
            SELECT log_date, log_hour, error_request_count,
                   total_request_count, ROUND(error_rate::numeric, 4),
                   distinct_error_hosts
            FROM q3_hourly_errors
            WHERE run_id = %s AND batch_id = %s
            ORDER BY log_date, log_hour
            LIMIT 30
        """, (run_id, max_batch))
    rows = cur.fetchall()
    print("\n  Q3 — Hourly Error Analysis")
    print(tabulate(rows,
                   headers=["Log Date", "Hour", "Error Reqs", "Total Reqs",
                             "Error Rate", "Distinct Error Hosts"],
                   tablefmt="pretty"))

    print("\n" + "="*70 + "\n")
    cur.close()
    conn.close()


def _max_batch(cur, table, run_id):
    cur.execute(f"SELECT MAX(batch_id) FROM {table} WHERE run_id = %s", (run_id,))
    row = cur.fetchone()
    return row[0] if row and row[0] else 1
