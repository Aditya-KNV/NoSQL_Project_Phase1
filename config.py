# ─── config.py ───────────────────────────────────────────────────────────────
# Central configuration for the ETL tool.
# Edit the values below to match your local setup.

# ── Input log files ──────────────────────────────────────────────────────────
LOG_FILES = [
    "data/NASA_access_log_Jul95",   # decompressed July log
    "data/NASA_access_log_Aug95",   # decompressed August log
]

# ── Batch size (number of log records per batch) ──────────────────────────────
BATCH_SIZE = 500000

# ── PostgreSQL connection ─────────────────────────────────────────────────────
PG_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "nosql_etl",
    "user":     "postgres",
    "password": "postgres",   # ← change this
}

# ── MongoDB connection ────────────────────────────────────────────────────────
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB  = "nasa_logs"
MONGO_COL = "raw_logs"

# ── MapReduce temp directory (used by mrjob local runner) ────────────────────
MR_TMP_DIR = "mr_tmp"
