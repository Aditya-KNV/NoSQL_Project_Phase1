# ─── config.py ───────────────────────────────────────────────────────────────
# Central configuration for the Multi-Pipeline ETL Tool — Phase 2
# Edit the values below to match your local setup.

# ── Input log files ──────────────────────────────────────────────────────────
LOG_FILES = [
    "data/NASA_access_log_Jul95/access_log_Jul95",   # decompressed July log
    "data/NASA_access_log_Aug95/access_log_Aug95"    # decompressed August log
]

# test
# LOG_FILES = ["data/test_sample.log"]

# ── Batch splitting ───────────────────────────────────────────────────────────
# Batches are always split by calendar day derived from log timestamps.
# Each unique date in the logs becomes one batch (batch-001, batch-002, ...).
# BATCH_SIZE is kept for the configurable pre-run prompt (shown in CLI menu).
BATCH_SIZE = 500_000   # max records per batch; batches that exceed this are
                        # sub-split within the same day (optional safety cap)

# ── PostgreSQL connection ─────────────────────────────────────────────────────
PG_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "nosql_etl",
    "user":     "postgres",
    "password": "postgres",   # ← change this
}

# ── MongoDB connection ────────────────────────────────────────────────────────
MONGO_URI  = "mongodb://localhost:27017"
MONGO_DB   = "nasa_logs"
MONGO_COL  = "raw_logs"

# ── Local Pig settings ────────────────────────────────────────────────────────
PIG_HOME   = "/usr/local/pig"    # path where pig binary lives; blank = use PATH
PIG_TMP    = "pig_tmp"           # scratch dir for Pig TSV + scripts + output

# ── Hive settings ────────────────────────────────────────────────────────────
HIVE_HOST  = "localhost"
HIVE_PORT  = 10000
HIVE_DB    = "nasa_logs"

# ── MapReduce temp directory (mrjob local runner) ────────────────────────────
MR_TMP_DIR = "mr_tmp"
