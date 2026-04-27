# ─── pipelines/mapreduce_pipeline.py ─────────────────────────────────────────
"""
MapReduce ETL Pipeline (using mrjob local runner — no Hadoop needed).

Because mrjob runs as a separate process (spawned via subprocess), we:
  1. Write parsed batches to temp files.
  2. Run separate MRJob classes for Q1, Q2, Q3 on each temp file.
  3. Collect results and save to PostgreSQL.
"""

import os
import sys
import json
import time
import tempfile
from datetime import datetime
from collections import defaultdict

from mrjob.job  import MRJob
from mrjob.step import MRStep

from parser.log_parser import parse_files
from loader.db_loader  import (init_schema, new_run_id, save_run_metadata,
                                save_q1, save_q2, save_q3)

PIPELINE_NAME = "MapReduce"


# ─────────────────────────────────────────────────────────────────────────────
# MRJob definitions (each class = one query)
# ─────────────────────────────────────────────────────────────────────────────

class MRQ1DailyTraffic(MRJob):
    """Query 1: Daily Traffic Summary"""

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]

    def mapper(self, _, line):
        try:
            rec = json.loads(line)
            key = f"{rec['log_date']}|{rec['status_code']}"
            yield key, (1, rec['bytes_transferred'])
        except Exception:
            pass

    def reducer(self, key, values):
        req_count = 0
        total_bytes = 0
        for count, b in values:
            req_count   += count
            total_bytes += b
        parts = key.split("|")
        yield key, {"log_date": parts[0], "status_code": int(parts[1]),
                    "request_count": req_count, "total_bytes": total_bytes}


class MRQ2TopResources(MRJob):
    """Query 2: Top 20 Requested Resources"""

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]

    def mapper(self, _, line):
        try:
            rec = json.loads(line)
            yield rec['resource_path'], (1, rec['bytes_transferred'], rec['host'])
        except Exception:
            pass

    def reducer(self, resource_path, values):
        req_count   = 0
        total_bytes = 0
        hosts       = set()
        for count, b, host in values:
            req_count   += count
            total_bytes += b
            hosts.add(host)
        yield resource_path, {"resource_path": resource_path,
                               "request_count": req_count,
                               "total_bytes":   total_bytes,
                               "distinct_host_count": len(hosts)}


class MRQ3HourlyErrors(MRJob):
    """Query 3: Hourly Error Analysis"""

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]

    def mapper(self, _, line):
        try:
            rec    = json.loads(line)
            key    = f"{rec['log_date']}|{rec['log_hour']}"
            status = rec['status_code']
            is_err = 1 if 400 <= status <= 599 else 0
            yield key, (1, is_err, rec['host'] if is_err else None)
        except Exception:
            pass

    def reducer(self, key, values):
        total = 0
        errors = 0
        err_hosts = set()
        for total_inc, is_err, host in values:
            total  += total_inc
            errors += is_err
            if host:
                err_hosts.add(host)
        parts      = key.split("|")
        error_rate = errors / total if total else 0
        yield key, {"log_date":             parts[0],
                    "log_hour":             int(parts[1]),
                    "error_request_count":  errors,
                    "total_request_count":  total,
                    "error_rate":           error_rate,
                    "distinct_error_hosts": len(err_hosts)}


# ─────────────────────────────────────────────────────────────────────────────
# Helper: run one MRJob class against a temp file, return parsed results
# ─────────────────────────────────────────────────────────────────────────────

def _run_mrjob(job_class, input_path: str) -> list:
    job = job_class(args=[input_path, "--runner=local", "--no-bootstrap-mrjob"])
    results = []
    with job.make_runner() as runner:
        runner.run()
        for _, value in job.parse_output(runner.cat_output()):
            results.append(value)
    return results


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

    tmp_dir = config.get("MR_TMP_DIR", "mr_tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    print(f"\n[MapReduce] Run ID: {run_id}")
    print(f"[MapReduce] Processing files: {log_files}\n")

    for batch_id, batch, malformed in parse_files(log_files, batch_size):
        total_batches   += 1
        total_records   += len(batch)
        total_malformed += malformed

        print(f"  Batch {batch_id:>4} | records={len(batch):>7} | malformed={malformed}")

        # Write batch as JSONL to a temp file
        tmp_file = os.path.join(tmp_dir, f"batch_{batch_id}.jsonl")
        with open(tmp_file, "w", encoding="utf-8") as f:
            for rec in batch:
                f.write(json.dumps(rec) + "\n")

        # ── Q1 ────────────────────────────────────────────────────────────────
        q1_results = _run_mrjob(MRQ1DailyTraffic, tmp_file)
        save_q1(pg_config, run_id, PIPELINE_NAME, batch_id,
                [(r["log_date"], r["status_code"], r["request_count"], r["total_bytes"])
                 for r in q1_results])

        # ── Q2 (top 20 sorted) ────────────────────────────────────────────────
        q2_results = sorted(_run_mrjob(MRQ2TopResources, tmp_file),
                            key=lambda x: x["request_count"], reverse=True)[:20]
        save_q2(pg_config, run_id, PIPELINE_NAME, batch_id,
                [(r["resource_path"], r["request_count"], r["total_bytes"], r["distinct_host_count"])
                 for r in q2_results])

        # ── Q3 ────────────────────────────────────────────────────────────────
        q3_results = _run_mrjob(MRQ3HourlyErrors, tmp_file)
        save_q3(pg_config, run_id, PIPELINE_NAME, batch_id,
                [(r["log_date"], r["log_hour"], r["error_request_count"],
                  r["total_request_count"], r["error_rate"], r["distinct_error_hosts"])
                 for r in q3_results])

        # Clean up temp file to save disk space
        os.remove(tmp_file)

    finished_at = datetime.utcnow()
    save_run_metadata(pg_config, run_id, PIPELINE_NAME,
                      started_at, finished_at,
                      total_records, total_batches,
                      batch_size, total_malformed)

    runtime = time.time() - start_time
    print(f"\n[MapReduce] Done in {runtime:.2f}s | "
          f"batches={total_batches} | records={total_records} | malformed={total_malformed}")

    return run_id
