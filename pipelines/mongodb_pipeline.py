# ─── pipelines/mongodb_pipeline.py ───────────────────────────────────────────
"""
MongoDB Pipeline — Phase 2

Queries (same as Phase 1):
  Q1 — Daily Traffic Summary
  Q2 — Top 20 Requested Resources
  Q3 — Hourly Error Analysis

Phase 2 changes from Phase 1:
  - Timestamp-based batching via batch_loader (one batch per calendar day)
  - Temp collection per batch: drop → insert → query → drop
  - save_batch_metadata() called per batch
  - log_date / log_hour taken directly from parser output (already present)
"""

import time
import uuid
from datetime import datetime

from pymongo import MongoClient
from loader.batch_loader import load_batches
from loader.db_loader import (
    get_connection, new_run_id,
    save_run_metadata, save_batch_metadata,
    save_q1, save_q2, save_q3,
)

Q2_TOP_N = 20


def run(pg_config: dict, mongo_uri: str, mongo_db: str, log_files: list):
    run_id     = new_run_id()
    pipeline   = "MongoDB"
    started_at = datetime.utcnow()

    client = MongoClient(mongo_uri)
    db     = client[mongo_db]

    batches_done  = 0
    total_records = 0
    total_malform = 0
    batch_size_cfg = 0   # timestamp batching — no fixed size

    for batch in load_batches(log_files):
        t0  = time.perf_counter()
        col = db["_batch_tmp"]
        col.drop()

        if batch["records"]:
            col.insert_many(batch["records"], ordered=False)

        q1_rows = _q1(col)
        q2_rows = _q2(col)
        q3_rows = _q3(col)
        col.drop()

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

        print(f"  [MongoDB] {batch['batch_id']} ({batch['date']}) — "
              f"{batch['batch_size']:,} records, "
              f"{batch['malformed_count']:,} malformed, {elapsed:.2f}s")

    finished_at = datetime.utcnow()
    save_run_metadata(
        pg_config, run_id, pipeline, started_at, finished_at,
        total_records, batches_done, batch_size_cfg, total_malform,
    )
    client.close()
    print(f"\n  [MongoDB] Done. run_id={run_id}")
    return run_id


# ── Q1: Daily Traffic Summary ─────────────────────────────────────────────────
def _q1(col):
    pipeline = [
        {"$group": {
            "_id":           {"log_date": "$log_date", "status": "$status_code"},
            "request_count": {"$sum": 1},
            "total_bytes":   {"$sum": {"$ifNull": ["$bytes_transferred", 0]}},
        }},
        {"$sort": {"_id.log_date": 1, "_id.status": 1}},
    ]
    return [
        (doc["_id"]["log_date"], doc["_id"]["status"],
         doc["request_count"],  doc["total_bytes"])
        for doc in col.aggregate(pipeline)
    ]


# ── Q2: Top 20 Requested Resources ───────────────────────────────────────────
def _q2(col):
    pipeline = [
        {"$group": {
            "_id":           "$resource_path",
            "request_count": {"$sum": 1},
            "total_bytes":   {"$sum": {"$ifNull": ["$bytes_transferred", 0]}},
            "hosts":         {"$addToSet": "$host"},
        }},
        {"$project": {
            "request_count":       1,
            "total_bytes":         1,
            "distinct_host_count": {"$size": "$hosts"},
        }},
        {"$sort":  {"request_count": -1}},
        {"$limit": Q2_TOP_N},
    ]
    return [
        (doc["_id"], doc["request_count"],
         doc["total_bytes"], doc["distinct_host_count"])
        for doc in col.aggregate(pipeline)
    ]


# ── Q3: Hourly Error Analysis ─────────────────────────────────────────────────
def _q3(col):
    # Total requests per (date, hour)
    totals = {
        (d["_id"]["log_date"], d["_id"]["log_hour"]): d["total"]
        for d in col.aggregate([
            {"$group": {
                "_id":   {"log_date": "$log_date", "log_hour": "$log_hour"},
                "total": {"$sum": 1},
            }}
        ])
    }
    # Error requests per (date, hour)
    rows = []
    for doc in col.aggregate([
        {"$match": {"status_code": {"$gte": 400, "$lte": 599}}},
        {"$group": {
            "_id":         {"log_date": "$log_date", "log_hour": "$log_hour"},
            "error_count": {"$sum": 1},
            "error_hosts": {"$addToSet": "$host"},
        }},
        {"$sort": {"_id.log_date": 1, "_id.log_hour": 1}},
    ]):
        key       = (doc["_id"]["log_date"], doc["_id"]["log_hour"])
        total     = totals.get(key, doc["error_count"])
        err_count = doc["error_count"]
        rows.append((
            doc["_id"]["log_date"], doc["_id"]["log_hour"],
            err_count, total,
            round(err_count / total, 4) if total else 0.0,
            len(doc["error_hosts"]),
        ))
    return rows
