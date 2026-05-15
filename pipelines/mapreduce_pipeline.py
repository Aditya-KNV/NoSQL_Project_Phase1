# ─── pipelines/mapreduce_pipeline.py ─────────────────────────────────────────
"""
MapReduce Pipeline — Phase 2  (mrjob local runner)

Phase 2 completes the Phase 1 scaffold:
  - Full MRJob implementations for Q1, Q2, Q3
  - Timestamp-based batching via batch_loader
  - PostgreSQL writes via db_loader
  - JSON parsing inside mappers (cleaning is part of the MR job)

Queries:
  Q1 — Daily Traffic Summary
  Q2 — Top 20 Requested Resources
  Q3 — Hourly Error Analysis (two MRJobs joined in Python)

NOTE on imports:
  loader and db_loader imports are inside run() deliberately.
  mrjob copies this file to a temp directory and re-runs it as a
  subprocess for each mapper/reducer. Those subprocesses must not
  import project modules (loader/, db_loader) that don't exist in
  the temp dir. Only json, os, mrjob are safe at module level.

NOTE on __main__:
  mrjob requires each MRJob class to call .run() under __main__
  when the script is invoked as a subprocess by the local runner.
  The MRJOB_CLASS env var is set by _run_job() to select the right class.
"""

import os
import json
import time
from datetime import datetime

from mrjob.job  import MRJob
from mrjob.step import MRStep

Q2_TOP_N = 20


# ── Q1: Daily Traffic Summary ─────────────────────────────────────────────────

class MRDailyTraffic(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper, combiner=self.combiner,
                       reducer=self.reducer)]

    def mapper(self, _, line):
        try:
            r      = json.loads(line)
            date   = r.get("log_date", "")
            status = r.get("status_code")
            bytes_ = r.get("bytes_transferred") or 0
            if date and status is not None:
                yield (date, status), (1, bytes_)
        except Exception:
            pass

    def combiner(self, key, values):
        c, b = 0, 0
        for cv, bv in values:
            c += cv; b += bv
        yield key, (c, b)

    def reducer(self, key, values):
        c, b = 0, 0
        for cv, bv in values:
            c += cv; b += bv
        yield key, (c, b)


# ── Q2: Top 20 Requested Resources ───────────────────────────────────────────

class MRTopResources(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(mapper=self.sort_mapper, reducer=self.top_reducer),
        ]

    def mapper(self, _, line):
        try:
            r      = json.loads(line)
            path   = r.get("resource_path", "")
            host   = r.get("host", "")
            bytes_ = r.get("bytes_transferred") or 0
            if path:
                yield path, (1, bytes_, host)
        except Exception:
            pass

    def reducer(self, path, values):
        count, total_bytes, hosts = 0, 0, set()
        for c, b, h in values:
            count += c; total_bytes += b
            if h: hosts.add(h)
        yield None, (count, total_bytes, len(hosts), path)

    def sort_mapper(self, _, val):
        count, total_bytes, distinct, path = val
        yield None, (-count, total_bytes, distinct, path)

    def top_reducer(self, _, vals):
        for neg_count, total_bytes, distinct, path in sorted(vals)[:Q2_TOP_N]:
            yield path, (-neg_count, total_bytes, distinct)


# ── Q3: Hourly totals ─────────────────────────────────────────────────────────

class MRHourlyTotals(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper, combiner=self.combiner,
                       reducer=self.reducer)]

    def mapper(self, _, line):
        try:
            r    = json.loads(line)
            date = r.get("log_date", "")
            hour = r.get("log_hour")
            if date and hour is not None:
                yield (date, hour), 1
        except Exception:
            pass

    def combiner(self, key, counts):
        yield key, sum(counts)

    def reducer(self, key, counts):
        yield key, sum(counts)


# ── Q3: Hourly errors ─────────────────────────────────────────────────────────

class MRHourlyErrors(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]

    def mapper(self, _, line):
        try:
            r      = json.loads(line)
            status = r.get("status_code", 0)
            date   = r.get("log_date", "")
            hour   = r.get("log_hour")
            host   = r.get("host", "")
            if 400 <= status <= 599 and date and hour is not None:
                yield (date, hour), (1, host)
        except Exception:
            pass

    def reducer(self, key, values):
        count, hosts = 0, set()
        for c, h in values:
            count += c
            if h: hosts.add(h)
        yield key, (count, len(hosts))


# ── Pipeline run function ─────────────────────────────────────────────────────

def run(pg_config: dict, log_files: list, mr_tmp: str = "mr_tmp"):
    # Import project modules here — not at module level — so mrjob mapper
    # subprocesses (which run this file from a temp dir) don't fail on them
    from loader.batch_loader import load_batches
    from loader.db_loader import (
        new_run_id, save_run_metadata, save_batch_metadata,
        save_q1, save_q2, save_q3,
    )

    run_id     = new_run_id()
    pipeline   = "MapReduce"
    started_at = datetime.utcnow()

    os.makedirs(mr_tmp, exist_ok=True)

    batches_done  = 0
    total_records = 0
    total_malform = 0

    for batch in load_batches(log_files):
        t0 = time.perf_counter()

        jsonl_path = os.path.abspath(os.path.join(mr_tmp, "batch.jsonl"))
        with open(jsonl_path, "w", encoding="utf-8") as f:
            for r in batch["records"]:
                f.write(json.dumps(r) + "\n")

        q1_rows = _run_q1(jsonl_path)
        q2_rows = _run_q2(jsonl_path)
        q3_rows = _run_q3(jsonl_path)

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

        print(f"  [MapReduce] {batch['batch_id']} ({batch['date']}) — "
              f"{batch['batch_size']:,} records, "
              f"{batch['malformed_count']:,} malformed, {elapsed:.2f}s")

    finished_at = datetime.utcnow()
    save_run_metadata(
        pg_config, run_id, pipeline, started_at, finished_at,
        total_records, batches_done, 0, total_malform,
    )
    print(f"\n  [MapReduce] Done. run_id={run_id}")
    return run_id


def _run_job(job_class, input_path: str) -> list:
    abs_input = os.path.abspath(input_path)
    os.environ["MRJOB_CLASS"] = job_class.__name__
    mr = job_class(args=["--runner=local", "--no-conf", abs_input])
    rows = []
    with mr.make_runner() as runner:
        runner.run()
        output = list(runner.cat_output())
    for key, value in mr.parse_output(iter(output)):
        rows.append((key, value))
    return rows


def _run_q1(jsonl_path) -> list:
    return [(k[0], k[1], v[0], v[1])
            for k, v in _run_job(MRDailyTraffic, jsonl_path)]


def _run_q2(jsonl_path) -> list:
    return [(path, v[0], v[1], v[2])
            for path, v in _run_job(MRTopResources, jsonl_path)]


def _run_q3(jsonl_path) -> list:
    totals = {(k[0], k[1]): v
              for k, v in _run_job(MRHourlyTotals, jsonl_path)}
    rows = []
    for key, (err_count, distinct) in _run_job(MRHourlyErrors, jsonl_path):
        date, hour = key[0], key[1]
        total    = totals.get((date, hour), err_count)
        err_rate = round(err_count / total, 4) if total else 0.0
        rows.append((date, hour, err_count, total, err_rate, distinct))
    return sorted(rows)


# ── Required by mrjob: invoked when running as mapper/reducer subprocess ──────

if __name__ == "__main__":
    job_map = {
        "MRDailyTraffic": MRDailyTraffic,
        "MRTopResources": MRTopResources,
        "MRHourlyTotals": MRHourlyTotals,
        "MRHourlyErrors": MRHourlyErrors,
    }
    job_name = os.environ.get("MRJOB_CLASS")
    if job_name in job_map:
        job_map[job_name].run()
