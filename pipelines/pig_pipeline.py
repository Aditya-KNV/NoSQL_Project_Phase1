# ─── pipelines/pig_pipeline.py ───────────────────────────────────────────────
"""
Apache Pig Pipeline — Phase 2  (local mode, no Hadoop)

Phase 2 changes from Phase 1 pig_local_pipeline.py:
  - Timestamp-based batching (one batch per calendar day)
  - TSV now includes log_date, log_hour, bytes columns
  - save_batch_metadata() called per batch
  - pig_pipeline.py replaces both pig_pipeline.py (Hadoop) and
    pig_local_pipeline.py — local mode only

Queries:
  Q1 — Daily Traffic Summary:   GROUP BY (log_date, status)
  Q2 — Top 20 Requested Resources: GROUP BY resource_path  LIMIT 20
  Q3 — Hourly Error Analysis:   filter status 400-599, GROUP BY (log_date, log_hour)
"""

import os
import shutil
import subprocess
import time
import uuid
from datetime import datetime

from loader.batch_loader import load_batches
from loader.db_loader import (
    new_run_id, save_run_metadata, save_batch_metadata,
    save_q1, save_q2, save_q3,
)

Q2_TOP_N = 20


def run(pg_config: dict, log_files: list, pig_home: str = "", pig_tmp: str = "pig_tmp"):
    run_id     = new_run_id()
    pipeline   = "Pig"
    started_at = datetime.utcnow()

    os.makedirs(pig_tmp, exist_ok=True)
    pig_bin = os.path.join(pig_home, "bin", "pig") if pig_home else "pig"

    batches_done  = 0
    total_records = 0
    total_malform = 0

    for batch in load_batches(log_files):
        t0 = time.perf_counter()

        # TSV: host, status_code, resource_path, log_date, log_hour, bytes_transferred
        tsv_path = os.path.join(pig_tmp, "batch_input.tsv")
        with open(tsv_path, "w", encoding="utf-8") as f:
            for r in batch["records"]:
                host   = (r.get("host")          or "").replace("\t", " ")
                status = str(r.get("status_code") or "")
                path   = (r.get("resource_path")  or "").replace("\t", " ")
                date   = r.get("log_date",  "")
                hour   = str(r.get("log_hour", ""))
                byt    = str(r.get("bytes_transferred") or 0)
                f.write(f"{host}\t{status}\t{path}\t{date}\t{hour}\t{byt}\n")

        q1_rows = _run_q1(pig_bin, pig_tmp, tsv_path)
        q2_rows = _run_q2(pig_bin, pig_tmp, tsv_path)
        q3_rows = _run_q3(pig_bin, pig_tmp, tsv_path)

        elapsed = time.perf_counter() - t0
        batches_done  += 1
        total_records += batch["batch_size"]
        total_malform += batch["malformed_count"]
        avg_sz = total_records / batches_done

        save_batch_metadata(
            pg_config, run_id, pipeline,
            batch["batch_id"], batch["date"], batch["source_file"],
            batch["batch_size"], round(avg_sz, 2),
            batch["malformed_count"], round(elapsed, 4),
            batch["start_ts"], batch["end_ts"],
        )
        save_q1(pg_config, run_id, pipeline, batch["batch_id"], q1_rows)
        save_q2(pg_config, run_id, pipeline, batch["batch_id"], q2_rows)
        save_q3(pg_config, run_id, pipeline, batch["batch_id"], q3_rows)

        print(f"  [Pig] {batch['batch_id']} ({batch['date']}) — "
              f"{batch['batch_size']:,} records, "
              f"{batch['malformed_count']:,} malformed, {elapsed:.2f}s")

    finished_at = datetime.utcnow()
    save_run_metadata(
        pg_config, run_id, pipeline, started_at, finished_at,
        total_records, batches_done, 0, total_malform,
    )
    print(f"\n  [Pig] Done. run_id={run_id}")
    return run_id


# ── Pig helpers ───────────────────────────────────────────────────────────────

def _exec_pig(pig_bin, pig_tmp, script: str):
    script_path = os.path.join(pig_tmp, "query.pig")
    with open(script_path, "w") as f:
        f.write(script)
    result = subprocess.run(
        [pig_bin, "-x", "local", script_path],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Pig failed (rc={result.returncode}):\n{result.stderr[-2000:]}"
        )


def _read_output(out_dir: str) -> list:
    lines = []
    for fname in sorted(os.listdir(out_dir)):
        if fname.startswith("part-"):
            with open(os.path.join(out_dir, fname)) as f:
                lines.extend(f.read().splitlines())
    return lines


# ── Q1 ────────────────────────────────────────────────────────────────────────

def _run_q1(pig_bin, pig_tmp, tsv) -> list:
    out_dir = os.path.join(pig_tmp, "q1_out")
    shutil.rmtree(out_dir, ignore_errors=True)
    _exec_pig(pig_bin, pig_tmp, f"""
logs = LOAD '{tsv}' USING PigStorage('\\t')
       AS (host:chararray, status_code:int, resource_path:chararray,
           log_date:chararray, log_hour:int, bytes_transferred:long);
grp  = GROUP logs BY (log_date, status_code);
res  = FOREACH grp GENERATE
           FLATTEN(group)          AS (log_date, status_code),
           COUNT(logs)             AS request_count,
           SUM(logs.bytes_transferred) AS total_bytes;
srt  = ORDER res BY log_date ASC, status_code ASC;
STORE srt INTO '{out_dir}' USING PigStorage('\\t');
""")
    rows = []
    for line in _read_output(out_dir):
        p = line.split("\t")
        if len(p) == 4:
            try:
                rows.append((p[0], int(p[1]), int(p[2]), int(p[3])))
            except ValueError:
                pass
    return rows


# ── Q2 ────────────────────────────────────────────────────────────────────────

def _run_q2(pig_bin, pig_tmp, tsv) -> list:
    out_dir = os.path.join(pig_tmp, "q2_out")
    shutil.rmtree(out_dir, ignore_errors=True)
    _exec_pig(pig_bin, pig_tmp, f"""
logs = LOAD '{tsv}' USING PigStorage('\\t')
       AS (host:chararray, status_code:int, resource_path:chararray,
           log_date:chararray, log_hour:int, bytes_transferred:long);
grp  = GROUP logs BY resource_path;
res  = FOREACH grp {{
           unique_hosts = DISTINCT logs.host;
           GENERATE
               group                       AS resource_path,
               COUNT(logs)                 AS request_count,
               SUM(logs.bytes_transferred) AS total_bytes,
               COUNT(unique_hosts)         AS distinct_host_count;
       }}
srt  = ORDER res BY request_count DESC;
top  = LIMIT srt {Q2_TOP_N};
STORE top INTO '{out_dir}' USING PigStorage('\\t');
""")
    rows = []
    for line in _read_output(out_dir):
        p = line.split("\t")
        if len(p) == 4:
            try:
                rows.append((p[0], int(p[1]), int(p[2]), int(p[3])))
            except ValueError:
                pass
    return rows


# ── Q3 ────────────────────────────────────────────────────────────────────────

def _run_q3(pig_bin, pig_tmp, tsv) -> list:
    # Pass 1: total requests per (date, hour)
    total_dir = os.path.join(pig_tmp, "q3_total_out")
    shutil.rmtree(total_dir, ignore_errors=True)
    _exec_pig(pig_bin, pig_tmp, f"""
logs = LOAD '{tsv}' USING PigStorage('\\t')
       AS (host:chararray, status_code:int, resource_path:chararray,
           log_date:chararray, log_hour:int, bytes_transferred:long);
grp  = GROUP logs BY (log_date, log_hour);
tot  = FOREACH grp GENERATE
           FLATTEN(group) AS (log_date, log_hour),
           COUNT(logs)    AS total_count;
STORE tot INTO '{total_dir}' USING PigStorage('\\t');
""")
    totals = {}
    for line in _read_output(total_dir):
        p = line.split("\t")
        if len(p) == 3:
            try:
                totals[(p[0], int(p[1]))] = int(p[2])
            except ValueError:
                pass

    # Pass 2: error requests per (date, hour)
    error_dir = os.path.join(pig_tmp, "q3_error_out")
    shutil.rmtree(error_dir, ignore_errors=True)
    _exec_pig(pig_bin, pig_tmp, f"""
logs = LOAD '{tsv}' USING PigStorage('\\t')
       AS (host:chararray, status_code:int, resource_path:chararray,
           log_date:chararray, log_hour:int, bytes_transferred:long);
errs = FILTER logs BY status_code >= 400 AND status_code <= 599;
grp  = GROUP errs BY (log_date, log_hour);
res  = FOREACH grp {{
           unique_hosts = DISTINCT errs.host;
           GENERATE
               FLATTEN(group)        AS (log_date, log_hour),
               COUNT(errs)           AS error_count,
               COUNT(unique_hosts)   AS distinct_error_hosts;
       }}
srt  = ORDER res BY log_date ASC, log_hour ASC;
STORE srt INTO '{error_dir}' USING PigStorage('\\t');
""")
    rows = []
    for line in _read_output(error_dir):
        p = line.split("\t")
        if len(p) == 4:
            try:
                log_date  = p[0]
                log_hour  = int(p[1])
                err_count = int(p[2])
                distinct  = int(p[3])
                total     = totals.get((log_date, log_hour), err_count)
                err_rate  = round(err_count / total, 4) if total else 0.0
                rows.append((log_date, log_hour, err_count, total, err_rate, distinct))
            except ValueError:
                pass
    return rows
