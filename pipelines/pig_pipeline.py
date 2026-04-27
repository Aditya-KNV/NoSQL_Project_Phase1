# ─── pipelines/pig_pipeline.py ───────────────────────────────────────────────
"""
Apache Pig ETL Pipeline (MapReduce mode on Hadoop/HDFS).

Flow per batch:
  1. Write parsed records to a temp CSV file locally.
  2. Upload CSV batch to HDFS.
  3. Generate Pig Latin scripts for Q1, Q2, Q3.
  4. Execute each script via: pig -x mapreduce script.pig
  5. Wait for HDFS output to appear.
  6. Read output back from HDFS into PostgreSQL.
  7. Clean up HDFS batch files after each batch.
"""

import os
import csv
import time
import subprocess
import shutil
from datetime import datetime

from parser.log_parser import parse_files
from loader.db_loader  import (init_schema, new_run_id, save_run_metadata,
                                save_q1, save_q2, save_q3)

PIPELINE_NAME  = "Pig"
TMP_DIR        = "/tmp/pig_etl"
HDFS_INPUT     = "/nasa/pig_input"
HDFS_OUTPUT    = "/nasa/pig_output"
PIG_BIN        = "/usr/local/pig/bin/pig"


# ─────────────────────────────────────────────────────────────────────────────
# HDFS helpers
# ─────────────────────────────────────────────────────────────────────────────

def _hdfs(args: list) -> bool:
    result = subprocess.run(["hdfs", "dfs"] + args,
                            capture_output=True, text=True)
    return result.returncode == 0


def _hdfs_put(local_path: str, hdfs_path: str) -> bool:
    result = subprocess.run(
        ["hdfs", "dfs", "-put", "-f", local_path, hdfs_path],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"[Pig HDFS PUT ERROR] {result.stderr[-500:]}")
    return result.returncode == 0


def _hdfs_getmerge(hdfs_path: str, local_path: str) -> bool:
    # Remove local file if exists
    if os.path.exists(local_path):
        os.remove(local_path)
    result = subprocess.run(
        ["hdfs", "dfs", "-getmerge", hdfs_path, local_path],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"[Pig HDFS GETMERGE ERROR] {result.stderr[-500:]}")
    return result.returncode == 0


def _hdfs_rm(hdfs_path: str):
    subprocess.run(
        ["hdfs", "dfs", "-rm", "-r", "-f", hdfs_path],
        capture_output=True, text=True
    )


def _hdfs_mkdir(hdfs_path: str):
    subprocess.run(
        ["hdfs", "dfs", "-mkdir", "-p", hdfs_path],
        capture_output=True, text=True
    )


def _hdfs_exists(hdfs_path: str) -> bool:
    result = subprocess.run(
        ["hdfs", "dfs", "-test", "-e", hdfs_path],
        capture_output=True, text=True
    )
    return result.returncode == 0


# ─────────────────────────────────────────────────────────────────────────────
# Write batch to CSV
# ─────────────────────────────────────────────────────────────────────────────

def _write_csv(batch: list, path: str):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter='\t')
        for r in batch:
            writer.writerow([
                r["host"],
                r["log_date"],
                r["log_hour"],
                r["http_method"],
                r["resource_path"],
                r["protocol_version"],
                r["status_code"],
                r["bytes_transferred"],
            ])


# ─────────────────────────────────────────────────────────────────────────────
# Pig script generators
# ─────────────────────────────────────────────────────────────────────────────

def _pig_q1(hdfs_input: str, hdfs_output: str) -> str:
    return f"""
records = LOAD '{hdfs_input}' USING PigStorage('\t') AS (
    host:chararray,
    log_date:chararray,
    log_hour:int,
    http_method:chararray,
    resource_path:chararray,
    protocol_version:chararray,
    status_code:int,
    bytes_transferred:long
);

filtered = FILTER records BY log_date IS NOT NULL AND status_code IS NOT NULL;

grouped = GROUP filtered BY (log_date, status_code);

q1 = FOREACH grouped GENERATE
    FLATTEN(group)                  AS (log_date, status_code),
    COUNT(filtered)                 AS request_count,
    SUM(filtered.bytes_transferred) AS total_bytes;

q1_sorted = ORDER q1 BY log_date ASC, status_code ASC;

STORE q1_sorted INTO '{hdfs_output}' USING PigStorage('\t');
"""


def _pig_q2(hdfs_input: str, hdfs_output: str) -> str:
    return f"""
records = LOAD '{hdfs_input}' USING PigStorage('\t') AS (
    host:chararray,
    log_date:chararray,
    log_hour:int,
    http_method:chararray,
    resource_path:chararray,
    protocol_version:chararray,
    status_code:int,
    bytes_transferred:long
);

filtered = FILTER records BY resource_path IS NOT NULL;

grouped = GROUP filtered BY resource_path;

q2 = FOREACH grouped GENERATE
    group                               AS resource_path,
    COUNT(filtered)                     AS request_count,
    SUM(filtered.bytes_transferred)     AS total_bytes,
    COUNT(filtered)                     AS distinct_host_count;

STORE q2 INTO '{hdfs_output}' USING PigStorage('\t');
"""


def _pig_q3(hdfs_input: str, hdfs_output: str) -> str:
    return f"""
records = LOAD '{hdfs_input}' USING PigStorage('\t') AS (
    host:chararray,
    log_date:chararray,
    log_hour:int,
    http_method:chararray,
    resource_path:chararray,
    protocol_version:chararray,
    status_code:int,
    bytes_transferred:long
);

filtered = FILTER records BY log_date IS NOT NULL AND log_hour IS NOT NULL;
errors   = FILTER filtered BY status_code >= 400 AND status_code <= 599;

grouped_all    = GROUP filtered BY (log_date, log_hour);
grouped_errors = GROUP errors   BY (log_date, log_hour);

total_counts = FOREACH grouped_all GENERATE
    FLATTEN(group)  AS (log_date, log_hour),
    COUNT(filtered) AS total_request_count;

error_counts = FOREACH grouped_errors GENERATE
    FLATTEN(group)       AS (log_date, log_hour),
    COUNT(errors)        AS error_request_count,
    COUNT(errors)        AS distinct_error_hosts;

joined = JOIN total_counts BY (log_date, log_hour)
         LEFT OUTER,
         error_counts BY (log_date, log_hour);

q3 = FOREACH joined GENERATE
    total_counts::log_date         AS log_date,
    total_counts::log_hour         AS log_hour,
    (error_counts::error_request_count IS NULL ? 0L :
        error_counts::error_request_count)             AS error_request_count,
    total_counts::total_request_count                  AS total_request_count,
    (error_counts::error_request_count IS NULL ? 0.0 :
        (double)error_counts::error_request_count /
        (double)total_counts::total_request_count)     AS error_rate,
    (error_counts::distinct_error_hosts IS NULL ? 0L :
        error_counts::distinct_error_hosts)            AS distinct_error_hosts;

q3_sorted = ORDER q3 BY log_date ASC, log_hour ASC;

STORE q3_sorted INTO '{hdfs_output}' USING PigStorage('\t');
"""


# ─────────────────────────────────────────────────────────────────────────────
# Run a Pig script and wait for HDFS output to appear
# ─────────────────────────────────────────────────────────────────────────────

def _run_pig(script_content: str, script_path: str, hdfs_out: str) -> bool:
    with open(script_path, "w") as f:
        f.write(script_content)

    result = subprocess.run(
        [PIG_BIN, "-x", "mapreduce", "-f", script_path],
        capture_output=True, text=True
    )

    if result.returncode != 0:
        print(f"[Pig ERROR]\n{result.stderr[-2000:]}")
        return False

    # Wait up to 60s for HDFS output directory to appear
    for _ in range(30):
        if _hdfs_exists(hdfs_out):
            return True
        time.sleep(2)

    print(f"[Pig] Timeout waiting for HDFS output: {hdfs_out}")
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Read merged output file
# ─────────────────────────────────────────────────────────────────────────────

def _read_output(merged_file: str) -> list:
    rows = []
    if not os.path.exists(merged_file):
        return rows
    with open(merged_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(line.split("\t"))
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Main pipeline entry point
# ─────────────────────────────────────────────────────────────────────────────

def run(config: dict):
    pg_config  = config["PG_CONFIG"]
    batch_size = config["BATCH_SIZE"]
    log_files  = config["LOG_FILES"]

    init_schema(pg_config)
    run_id     = new_run_id()
    started_at = datetime.utcnow()
    start_time = time.time()

    total_records   = 0
    total_batches   = 0
    total_malformed = 0

    # Clean and create local tmp dir
    if os.path.exists(TMP_DIR):
        shutil.rmtree(TMP_DIR)
    os.makedirs(TMP_DIR)

    # Create HDFS working directories
    _hdfs_mkdir(HDFS_INPUT)
    _hdfs_mkdir(HDFS_OUTPUT)

    print(f"\n[Pig] Run ID: {run_id}")
    print(f"[Pig] Processing files: {log_files}")
    print(f"[Pig] Running in MapReduce mode on Hadoop\n")

    for batch_id, batch, malformed in parse_files(log_files, batch_size):
        total_batches   += 1
        total_records   += len(batch)
        total_malformed += malformed

        print(f"  Batch {batch_id:>4} | records={len(batch):>7} | malformed={malformed}")

        batch_dir = os.path.join(TMP_DIR, f"batch_{batch_id}")
        os.makedirs(batch_dir, exist_ok=True)

        # Write CSV locally
        csv_path = os.path.join(batch_dir, "input.csv")
        _write_csv(batch, csv_path)

        # Upload to HDFS
        hdfs_csv = f"{HDFS_INPUT}/batch_{batch_id}.csv"
        _hdfs_rm(hdfs_csv)
        if not _hdfs_put(csv_path, hdfs_csv):
            print(f"  [ERROR] Failed to upload batch {batch_id} to HDFS, skipping.")
            continue

        # ── Q1 ────────────────────────────────────────────────────────────────
        hdfs_q1_out  = f"{HDFS_OUTPUT}/batch_{batch_id}_q1"
        local_q1_out = os.path.join(batch_dir, "q1_merged.csv")
        q1_script    = os.path.join(batch_dir, "q1.pig")
        _hdfs_rm(hdfs_q1_out)
        if _run_pig(_pig_q1(hdfs_csv, hdfs_q1_out), q1_script, hdfs_q1_out):
            if _hdfs_getmerge(hdfs_q1_out, local_q1_out):
                rows = _read_output(local_q1_out)
                save_q1(pg_config, run_id, PIPELINE_NAME, batch_id,
                        [(r[0], int(r[1]), int(r[2]), int(r[3]))
                         for r in rows if len(r) == 4 and all(x.strip() for x in r)])
                print(f"    Q1 saved: {len(rows)} rows")

        # ── Q2 ────────────────────────────────────────────────────────────────
        hdfs_q2_out  = f"{HDFS_OUTPUT}/batch_{batch_id}_q2"
        local_q2_out = os.path.join(batch_dir, "q2_merged.csv")
        q2_script    = os.path.join(batch_dir, "q2.pig")
        _hdfs_rm(hdfs_q2_out)
        if _run_pig(_pig_q2(hdfs_csv, hdfs_q2_out), q2_script, hdfs_q2_out):
            if _hdfs_getmerge(hdfs_q2_out, local_q2_out):
                rows = _read_output(local_q2_out)
                valid = [(r[0], int(r[1]), int(r[2]), int(r[3]))
                         for r in rows if len(r) == 4 and all(x.strip() for x in r)]
                # Sort by request_count desc and take top 20
                valid = sorted(valid, key=lambda x: x[1], reverse=True)[:20]
                save_q2(pg_config, run_id, PIPELINE_NAME, batch_id, valid)
                print(f"    Q2 saved: {len(valid)} rows")

        # ── Q3 ────────────────────────────────────────────────────────────────
        hdfs_q3_out  = f"{HDFS_OUTPUT}/batch_{batch_id}_q3"
        local_q3_out = os.path.join(batch_dir, "q3_merged.csv")
        q3_script    = os.path.join(batch_dir, "q3.pig")
        _hdfs_rm(hdfs_q3_out)
        if _run_pig(_pig_q3(hdfs_csv, hdfs_q3_out), q3_script, hdfs_q3_out):
            if _hdfs_getmerge(hdfs_q3_out, local_q3_out):
                rows = _read_output(local_q3_out)
                save_q3(pg_config, run_id, PIPELINE_NAME, batch_id,
                        [(r[0], int(r[1]), int(r[2]), int(r[3]), float(r[4]), int(r[5]))
                         for r in rows if len(r) == 6 and all(x.strip() for x in r)])
                print(f"    Q3 saved: {len(rows)} rows")

        # Clean up HDFS and local batch files
        _hdfs_rm(hdfs_csv)
        _hdfs_rm(hdfs_q1_out)
        _hdfs_rm(hdfs_q2_out)
        _hdfs_rm(hdfs_q3_out)
        shutil.rmtree(batch_dir)

    finished_at = datetime.utcnow()
    save_run_metadata(pg_config, run_id, PIPELINE_NAME,
                      started_at, finished_at,
                      total_records, total_batches,
                      batch_size, total_malformed)

    runtime = time.time() - start_time
    print(f"\n[Pig] Done in {runtime:.2f}s | "
          f"batches={total_batches} | records={total_records} | malformed={total_malformed}")

    return run_id
