# ─── pipelines/mongodb_pipeline.py ───────────────────────────────────────────
"""
MongoDB ETL Pipeline.

Flow per batch:
  1. Insert parsed records into MongoDB collection.
  2. Run aggregation pipelines for Q1, Q2, Q3 on the full collection
     (results always reflect the cumulative dataset up to this batch).
  3. Save aggregated results to PostgreSQL via db_loader.
"""

import time
from datetime import datetime
from pymongo import MongoClient, DESCENDING

from parser.log_parser import parse_files
from loader.db_loader  import (init_schema, new_run_id, save_run_metadata,
                                save_q1, save_q2, save_q3)


PIPELINE_NAME = "MongoDB"


def _q1_aggregation():
    return [
        {"$group": {
            "_id": {"log_date": "$log_date", "status_code": "$status_code"},
            "request_count": {"$sum": 1},
            "total_bytes":   {"$sum": "$bytes_transferred"},
        }},
        {"$project": {
            "_id": 0,
            "log_date":      "$_id.log_date",
            "status_code":   "$_id.status_code",
            "request_count": 1,
            "total_bytes":   1,
        }},
        {"$sort": {"log_date": 1, "status_code": 1}},
    ]


def _q2_aggregation():
    return [
        {"$group": {
            "_id": "$resource_path",
            "request_count":      {"$sum": 1},
            "total_bytes":        {"$sum": "$bytes_transferred"},
            "distinct_hosts":     {"$addToSet": "$host"},
        }},
        {"$project": {
            "_id": 0,
            "resource_path":      "$_id",
            "request_count":      1,
            "total_bytes":        1,
            "distinct_host_count": {"$size": "$distinct_hosts"},
        }},
        {"$sort":  {"request_count": -1}},
        {"$limit": 20},
    ]


def _q3_aggregation():
    return [
        {"$group": {
            "_id": {"log_date": "$log_date", "log_hour": "$log_hour"},
            "total_request_count": {"$sum": 1},
            "error_request_count": {"$sum": {
                "$cond": [
                    {"$and": [
                        {"$gte": ["$status_code", 400]},
                        {"$lte": ["$status_code", 599]},
                    ]}, 1, 0
                ]
            }},
            "error_hosts": {"$addToSet": {
                "$cond": [
                    {"$and": [
                        {"$gte": ["$status_code", 400]},
                        {"$lte": ["$status_code", 599]},
                    ]}, "$host", "$$REMOVE"
                ]
            }},
        }},
        {"$project": {
            "_id": 0,
            "log_date":            "$_id.log_date",
            "log_hour":            "$_id.log_hour",
            "error_request_count": 1,
            "total_request_count": 1,
            "error_rate": {
                "$cond": [
                    {"$gt": ["$total_request_count", 0]},
                    {"$divide": ["$error_request_count", "$total_request_count"]},
                    0
                ]
            },
            "distinct_error_hosts": {"$size": "$error_hosts"},
        }},
        {"$sort": {"log_date": 1, "log_hour": 1}},
    ]


def run(config: dict):
    pg_config  = config["PG_CONFIG"]
    mongo_uri  = config["MONGO_URI"]
    mongo_db   = config["MONGO_DB"]
    mongo_col  = config["MONGO_COL"]
    batch_size = config["BATCH_SIZE"]
    log_files  = config["LOG_FILES"]

    # ── Setup ─────────────────────────────────────────────────────────────────
    init_schema(pg_config)
    run_id     = new_run_id()
    started_at = datetime.utcnow()
    start_time = time.time()

    client = MongoClient(mongo_uri)
    db     = client[mongo_db]

    # Drop old data so each run is clean
    db[mongo_col].drop()
    collection = db[mongo_col]

    total_records   = 0
    total_batches   = 0
    total_malformed = 0

    print(f"\n[MongoDB] Run ID: {run_id}")
    print(f"[MongoDB] Processing files: {log_files}\n")

    for batch_id, batch, malformed in parse_files(log_files, batch_size):
        total_batches   += 1
        total_records   += len(batch)
        total_malformed += malformed

        print(f"  Batch {batch_id:>4} | records={len(batch):>7} | malformed={malformed}")

        # ── Insert into MongoDB ───────────────────────────────────────────────
        if batch:
            collection.insert_many(batch, ordered=False)

        # ── Run aggregations on cumulative data ───────────────────────────────
        q1_results = list(collection.aggregate(_q1_aggregation(), allowDiskUse=True))
        q2_results = list(collection.aggregate(_q2_aggregation(), allowDiskUse=True))
        q3_results = list(collection.aggregate(_q3_aggregation(), allowDiskUse=True))

        # ── Save to PostgreSQL ────────────────────────────────────────────────
        save_q1(pg_config, run_id, PIPELINE_NAME, batch_id,
                [(r["log_date"], r["status_code"], r["request_count"], r["total_bytes"])
                 for r in q1_results])

        save_q2(pg_config, run_id, PIPELINE_NAME, batch_id,
                [(r["resource_path"], r["request_count"], r["total_bytes"], r["distinct_host_count"])
                 for r in q2_results])

        save_q3(pg_config, run_id, PIPELINE_NAME, batch_id,
                [(r["log_date"], r["log_hour"], r["error_request_count"],
                  r["total_request_count"], r["error_rate"], r["distinct_error_hosts"])
                 for r in q3_results])

    finished_at = datetime.utcnow()

    save_run_metadata(pg_config, run_id, PIPELINE_NAME,
                      started_at, finished_at,
                      total_records, total_batches,
                      batch_size, total_malformed)

    runtime = time.time() - start_time
    print(f"\n[MongoDB] Done in {runtime:.2f}s | "
          f"batches={total_batches} | records={total_records} | malformed={total_malformed}")

    client.close()
    return run_id
