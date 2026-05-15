# DAS 839 — NoSQL Systems End-Semester Project — Phase 2

**Multi-Pipeline ETL and Reporting Framework for NASA Web Server Log Analytics**

| | |
|---|---|
| Dataset | NASA HTTP access logs, July–August 1995 |
| Records | 3,461,607 total, 6 malformed |
| Pipelines | MongoDB, Apache Pig (local), MapReduce (mrjob), Hive |
| Result Store | PostgreSQL 16 |
| Batching | One batch per calendar day (timestamp-based) |

---

## Project Structure

```
nosql_etl_project/
├── config.py                       # DB credentials, file paths, pipeline settings
├── main.py                         # Entry point — interactive menu or CLI flags
├── requirements.txt
│
├── sql/
│   └── schema.sql                  # PostgreSQL DDL — run once before first use
│
├── parser/
│   └── log_parser.py               # Shared NASA CLF parser (unchanged from Phase 1)
│
├── loader/
│   ├── batch_loader.py             # NEW: timestamp-based batch splitter
│   └── db_loader.py                # PostgreSQL writes (extended from Phase 1)
│
├── pipelines/
│   ├── mongodb_pipeline.py         # Phase 1 pipeline — updated for per-batch batching
│   ├── pig_pipeline.py             # Phase 1 local pipeline — updated for batching
│   ├── mapreduce_pipeline.py       # Phase 1 scaffold — fully completed
│   └── hive_pipeline.py            # NEW pipeline
│
└── reporting/
    └── report.py                   # PostgreSQL report printer (extended from Phase 1)
```

---

## Queries

| | Query | Description |
|---|---|---|
| Q1 | Daily Traffic Summary | Per `(log_date, status_code)`: request count + total bytes |
| Q2 | Top 20 Requested Resources | Top 20 paths by requests, with bytes and distinct hosts |
| Q3 | Hourly Error Analysis | Per `(log_date, log_hour)` for status 400–599: error count, total, rate, distinct hosts |

All three queries run on every batch independently. Results are stored per-batch — no cross-batch aggregation.

---

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure `config.py`
Edit PostgreSQL credentials, MongoDB URI, and log file paths.

### 3. Initialise the database
```bash
python main.py --init-db
```
Or manually:
```bash
psql -U postgres -f sql/schema.sql
```

### 4. Place log files
```
data/NASA_access_log_Jul95/access_log_Jul95
data/NASA_access_log_Aug95/access_log_Aug95
```

## Running

### Interactive
```bash
python main.py
```

Presents a menu:
```
  1. MongoDB
  2. Apache Pig  (local mode)
  3. MapReduce   (mrjob local)
  4. Hive        (HiveServer2)
  5. Show report for last run
  0. Exit
```

### Non-interactive
```bash
python main.py --pipeline mongo
python main.py --pipeline pig
python main.py --pipeline mapreduce
python main.py --pipeline hive
python main.py --report
python main.py --report --run-id <run_id>
python main.py --report --batch-id batch-001   # skip prompt, go straight to batch
```

### Viewing results

After a pipeline finishes, the report prints a run summary and batch table, then prompts you to select which batch to view Q1/Q2/Q3 for:

```
  Select batch to view Q1 / Q2 / Q3:
      1.  batch-001
      2.  batch-002
      ...
     62.  batch-058  ← last

      [Enter] defaults to last batch (batch-058)

  Enter number or batch ID:
```

Enter a number, a batch ID (e.g. `batch-015`), or press Enter for the last batch. Each batch covers one calendar day — its Q1/Q2/Q3 results are that day's data only.

---

## Batching

Phase 2 splits the logs by calendar day. Each unique date in the log files becomes one independent batch. Both July and August files are processed together — the splitter reads across files, so a date that appears in both (edge case) would be merged into one batch.

```
July  1, 1995  → batch-001
July  2, 1995  → batch-002
...
August 31, 1995 → batch-N
batch-malformed  → any unparseable lines (all files combined)
```

---

## Pipeline Notes

### MongoDB
Inserts each batch into a temporary collection `_batch_tmp`, runs `$group` aggregations, then drops the collection. Per-batch isolation — nothing persists between batches.

### Apache Pig (local mode)
Writes batch to a TSV file, generates Pig Latin scripts, executes `pig -x local`, parses output files. No Hadoop required. Replaces both `pig_pipeline.py` (Hadoop) and `pig_local_pipeline.py` from Phase 1.

### MapReduce (mrjob local)
Four MRJob classes: `MRDailyTraffic` (Q1), `MRTopResources` (Q2), `MRHourlyTotals` + `MRHourlyErrors` (Q3). Q3 requires two separate jobs joined in Python to compute error rate. Runs locally — no YARN/Hadoop cluster needed.

### Hive
Requires HiveServer2 running (`hive --service hiveserver2`). Loads each batch TSV into a temporary table, runs HiveQL GROUP BY queries, drops the table. Two queries for Q3 joined in Python.

---

## Infrastructure

| Component | Technology | Version |
|---|---|---|
| Orchestration | Python | 3.12 |
| Pipeline 1 | MongoDB | 7.0 |
| Pipeline 2 | Apache Pig | 0.17 (local mode) |
| Pipeline 3 | MapReduce | mrjob 0.7 (local) |
| Pipeline 4 | Hive | HiveServer2 3.1 |
| Result Store | PostgreSQL | 16 |

---

## Group Members

| Name | Roll Number |
|---|---|
| Kothamasu Naga Venkata Aditya | IMT2023033 |
| Abhijit Dibbidi | IMT2023054 |
| Vishnu Balla | IMT2023097 |
| Praneeth M | IMT2023555 |

