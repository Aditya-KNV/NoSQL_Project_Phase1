# ─── pipelines/hive_pipeline.py ──────────────────────────────────────────────
"""
Hive Pipeline — Phase 2

Requires HiveServer2 running.
Dependencies:
    pip install pyhive thrift thrift-sasl sasl

Queries:
  Q1 — Daily Traffic Summary
  Q2 — Top 20 Requested Resources
  Q3 — Hourly Error Analysis
"""

import os
import shutil
import time
from datetime import datetime

from pyhive import hive

from loader.batch_loader import load_batches
from loader.db_loader import (
    new_run_id,
    save_run_metadata,
    save_batch_metadata,
    save_q1,
    save_q2,
    save_q3,
)

Q2_TOP_N = 20


def run(
    pg_config: dict,
    log_files: list,
    hive_host: str = "localhost",
    hive_port: int = 10000,
    hive_db: str = "default",
    hive_tmp: str = "hive_tmp",
):

    run_id     = new_run_id()
    pipeline   = "Hive"
    started_at = datetime.utcnow()

    os.makedirs(hive_tmp, exist_ok=True)

    # ── Hive connection ──────────────────────────────────────────────────────
    hive_conn = hive.Connection(
        host=hive_host,
        port=hive_port,
        database=hive_db
    )

    cursor = hive_conn.cursor()

    # ── Hive settings ───────────────────────────────────────────────────────
    cursor.execute("SET hive.execution.engine=mr")
    cursor.execute("SET mapreduce.framework.name=yarn")
    cursor.execute("SET hive.exec.mode.local.auto=false")
    cursor.execute("SET dfs.replication=1")

    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {hive_db}")
    cursor.execute(f"USE {hive_db}")

    batches_done  = 0
    total_records = 0
    total_malform = 0

    # ────────────────────────────────────────────────────────────────────────
    # Batch loop
    # ────────────────────────────────────────────────────────────────────────
    for batch in load_batches(log_files):

        print(f"\n  [Hive] Processing {batch['batch_id']} ({batch['date']})")

        t0 = time.perf_counter()

        # ── Prepare TSV locally ─────────────────────────────────────────────
        tsv_dir  = os.path.join(hive_tmp, "batch_data")
        tsv_path = os.path.join(tsv_dir, "batch.tsv")

        shutil.rmtree(tsv_dir, ignore_errors=True)
        os.makedirs(tsv_dir)

        with open(tsv_path, "w", encoding="utf-8") as f:
            for r in batch["records"]:

                host   = (r.get("host") or "").replace("\t", " ")
                status = str(r.get("status_code") or "")
                path   = (r.get("resource_path") or "").replace("\t", " ")
                date   = r.get("log_date", "")
                hour   = str(r.get("log_hour", ""))
                byt    = str(r.get("bytes_transferred") or 0)

                f.write(
                    f"{host}\t{status}\t{path}\t{date}\t{hour}\t{byt}\n"
                )

        # ── Upload TSV to HDFS ──────────────────────────────────────────────
        abs_tsv_path = os.path.abspath(tsv_path)

        hdfs_dir  = f"/user/praneeth/hive_input/{batch['batch_id']}"
        hdfs_file = f"{hdfs_dir}/batch.tsv"

        os.system(f"hdfs dfs -mkdir -p {hdfs_dir}")
        os.system(f"hdfs dfs -put -f {abs_tsv_path} {hdfs_file}")

        # ── Hive temp table ─────────────────────────────────────────────────
        tbl = "batch_tmp"

        cursor.execute(f"DROP TABLE IF EXISTS {tbl}")

        cursor.execute(f"""
            CREATE TABLE {tbl} (
                host              STRING,
                status_code       INT,
                resource_path     STRING,
                log_date          STRING,
                log_hour          INT,
                bytes_transferred BIGINT
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY '\\t'
            STORED AS TEXTFILE
        """)

        # ── Load HDFS data into Hive ────────────────────────────────────────
        cursor.execute(
            f"LOAD DATA INPATH '{hdfs_file}' "
            f"OVERWRITE INTO TABLE {tbl}"
        )

        # ── Execute queries ─────────────────────────────────────────────────
        q1_rows = _q1(cursor, tbl)
        print("    Q1 done")

        q2_rows = _q2(cursor, tbl)
        print("    Q2 done")

        q3_rows = _q3(cursor, tbl)
        print("    Q3 done")

        # ── Cleanup Hive table + HDFS temp ─────────────────────────────────
        cursor.execute(f"DROP TABLE IF EXISTS {tbl}")

        os.system(f"hdfs dfs -rm -r {hdfs_dir}")

        # ── Batch metrics ───────────────────────────────────────────────────
        elapsed = time.perf_counter() - t0

        batches_done  += 1
        total_records += batch["batch_size"]
        total_malform += batch["malformed_count"]

        avg_sz = total_records / batches_done

        # ── Save metadata/results ──────────────────────────────────────────
        save_batch_metadata(
            pg_config,
            run_id,
            pipeline,
            batch["batch_id"],
            batch["date"],
            batch["source_file"],
            batch["batch_size"],
            round(avg_sz, 2),
            batch["malformed_count"],
            round(elapsed, 4),
            batch["start_ts"],
            batch["end_ts"],
        )

        save_q1(
            pg_config,
            run_id,
            pipeline,
            batch["batch_id"],
            q1_rows,
        )

        save_q2(
            pg_config,
            run_id,
            pipeline,
            batch["batch_id"],
            q2_rows,
        )

        save_q3(
            pg_config,
            run_id,
            pipeline,
            batch["batch_id"],
            q3_rows,
        )

        print(
            f"  [Hive] {batch['batch_id']} ({batch['date']}) — "
            f"{batch['batch_size']:,} records, "
            f"{batch['malformed_count']:,} malformed, "
            f"{elapsed:.2f}s"
        )

    # ── Final metadata ─────────────────────────────────────────────────────
    finished_at = datetime.utcnow()

    save_run_metadata(
        pg_config,
        run_id,
        pipeline,
        started_at,
        finished_at,
        total_records,
        batches_done,
        0,
        total_malform,
    )

    cursor.close()
    hive_conn.close()

    print(f"\n  [Hive] Done. run_id={run_id}")

    return run_id


# ────────────────────────────────────────────────────────────────────────────
# Queries
# ────────────────────────────────────────────────────────────────────────────

def _q1(cursor, tbl) -> list:

    cursor.execute(f"""
        SELECT
            log_date,
            status_code,
            COUNT(*) AS request_count,
            SUM(bytes_transferred) AS total_bytes
        FROM {tbl}
        GROUP BY log_date, status_code
        ORDER BY log_date, status_code
    """)

    return list(cursor.fetchall())


def _q2(cursor, tbl) -> list:

    cursor.execute(f"""
        SELECT
            resource_path,
            COUNT(*)               AS request_count,
            SUM(bytes_transferred) AS total_bytes,
            COUNT(DISTINCT host)   AS distinct_host_count
        FROM {tbl}
        GROUP BY resource_path
        ORDER BY request_count DESC
        LIMIT {Q2_TOP_N}
    """)

    return list(cursor.fetchall())


def _q3(cursor, tbl) -> list:

    cursor.execute(f"""
        SELECT
            log_date,
            log_hour,
            COUNT(*) AS total
        FROM {tbl}
        GROUP BY log_date, log_hour
    """)

    totals = {(r[0], r[1]): r[2] for r in cursor.fetchall()}

    cursor.execute(f"""
        SELECT
            log_date,
            log_hour,
            COUNT(*)             AS error_count,
            COUNT(DISTINCT host) AS distinct_error_hosts
        FROM {tbl}
        WHERE status_code >= 400
          AND status_code <= 599
        GROUP BY log_date, log_hour
        ORDER BY log_date, log_hour
    """)

    rows = []

    for r in cursor.fetchall():

        date, hour, err_count, distinct = r

        total = totals.get((date, hour), err_count)

        err_rate = round(err_count / total, 4) if total else 0.0

        rows.append(
            (
                date,
                hour,
                err_count,
                total,
                err_rate,
                distinct,
            )
        )

    return rows