# ─── reporting/report.py ─────────────────────────────────────────────────────
"""
Reads results from PostgreSQL and displays a formatted report.

After printing the batch summary table, the user is prompted to pick
a specific batch. Each batch's Q1/Q2/Q3 results are shown independently —
no aggregation across batches.
"""

import psycopg2
from tabulate import tabulate


def _conn(pg_config):
    return psycopg2.connect(**pg_config)


def get_latest_run_id(pg_config, pipeline=None):
    sql    = "SELECT run_id FROM run_metadata"
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


def show_report(pg_config, run_id=None, pipeline=None, batch_id=None):
    """
    batch_id : str | None
        None        → prompt the user interactively
        "batch-001" → show that specific batch directly
    """
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
    print(f"  Avg Batch Size : {avg_batch:,.1f}")
    print(f"  Malformed Rows : {malformed:,}")
    print("="*70)

    # ── Batch summary table ───────────────────────────────────────────────────
    cur.execute("""
        SELECT batch_id, batch_date, source_file, batch_size,
               malformed_count, runtime_seconds
        FROM batch_metadata
        WHERE run_id = %s
        ORDER BY batch_id
    """, (run_id,))
    batches  = cur.fetchall()
    batch_ids = [row[0] for row in batches]

    if batches:
        print("\n  BATCH SUMMARY")
        print(tabulate(batches,
                       headers=["Batch ID", "Date", "Source File",
                                 "Records", "Malformed", "Runtime (s)"],
                       tablefmt="pretty"))

    # ── Batch selection ───────────────────────────────────────────────────────
    selected = _select_batch(batch_ids, batch_id)

    batch_date = next((str(row[1]) for row in batches if row[0] == selected), "")
    print(f"\n  Showing Q1 / Q2 / Q3 for {selected}"
          + (f"  ({batch_date})" if batch_date else "") + "\n")

    # ── Q1: Daily Traffic ─────────────────────────────────────────────────────
    cur.execute("""
        SELECT log_date, status_code, request_count, total_bytes
        FROM q1_daily_traffic
        WHERE run_id = %s AND batch_id = %s
        ORDER BY log_date, status_code
        LIMIT 30
    """, (run_id, selected))
    rows = cur.fetchall()
    print("  Q1 — Daily Traffic Summary"
          + ("  [first 30 rows]" if len(rows) == 30 else ""))
    print(tabulate(rows,
                   headers=["Log Date", "Status Code", "Request Count", "Total Bytes"],
                   tablefmt="pretty", intfmt=","))

    # ── Q2: Top Resources ─────────────────────────────────────────────────────
    cur.execute("""
        SELECT resource_path, request_count, total_bytes, distinct_host_count
        FROM q2_top_resources
        WHERE run_id = %s AND batch_id = %s
        ORDER BY request_count DESC
        LIMIT 20
    """, (run_id, selected))
    rows = cur.fetchall()
    print("\n  Q2 — Top 20 Requested Resources")
    print(tabulate(rows,
                   headers=["Resource Path", "Request Count", "Total Bytes", "Distinct Hosts"],
                   tablefmt="pretty", intfmt=","))

    # ── Q3: Hourly Errors ─────────────────────────────────────────────────────
    cur.execute("""
        SELECT log_date, log_hour, error_request_count,
               total_request_count, ROUND(error_rate::numeric, 4),
               distinct_error_hosts
        FROM q3_hourly_errors
        WHERE run_id = %s AND batch_id = %s
        ORDER BY log_date, log_hour
        LIMIT 30
    """, (run_id, selected))
    rows = cur.fetchall()
    print("\n  Q3 — Hourly Error Analysis"
          + ("  [first 30 rows]" if len(rows) == 30 else ""))
    print(tabulate(rows,
                   headers=["Log Date", "Hour", "Error Reqs", "Total Reqs",
                             "Error Rate", "Distinct Error Hosts"],
                   tablefmt="pretty"))

    print("\n" + "="*70 + "\n")
    cur.close()
    conn.close()


# ── Batch selection helper ────────────────────────────────────────────────────

def _select_batch(batch_ids: list, batch_id_arg) -> str:
    """
    Resolve which batch to show.

    batch_id_arg:
      None        → prompt the user interactively
      "batch-001" → validate and return it directly
    """
    if not batch_ids:
        return ""

    last = batch_ids[-1]

    # Non-interactive: specific batch passed in
    if batch_id_arg is not None:
        if batch_id_arg in batch_ids:
            return batch_id_arg
        print(f"  [Report] batch_id '{batch_id_arg}' not found. "
              f"Defaulting to last batch ({last}).")
        return last

    # Interactive prompt
    print("\n  Select batch to view Q1 / Q2 / Q3:")
    for i, bid in enumerate(batch_ids, start=1):
        marker = "  ← last" if bid == last else ""
        print(f"    {i:>3}.  {bid}{marker}")
    print(f"\n      [Enter] defaults to last batch ({last})")

    choice = input("\n  Enter number or batch ID: ").strip()

    if choice == "":
        return last
    if choice.isdigit():
        idx = int(choice) - 1
        if 0 <= idx < len(batch_ids):
            return batch_ids[idx]
        print(f"  Invalid number. Defaulting to last batch ({last}).")
        return last
    if choice in batch_ids:
        return choice

    print(f"  Unrecognised input. Defaulting to last batch ({last}).")
    return last
