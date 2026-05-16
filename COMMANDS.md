# COMMANDS.md — Step-by-Step Run Instructions

Complete setup from scratch on a fresh Ubuntu/WSL2 machine.
Every command is copy-pasteable. Run them in order.

---

## 0. Check what you already have

Run these first. Skip any install in section 1 for tools that already report a version.

```bash
# ── Python ────────────────────────────────────────────────────────────────────
python3 --version
# Good:    Python 3.10.x or higher
# Missing: command not found → install in section 1

pip3 --version
# Good:    pip 23.x or higher
# Missing: command not found → install in section 1

# ── PostgreSQL ────────────────────────────────────────────────────────────────
psql --version
# Good:    psql (PostgreSQL) 14.x or higher
# Missing: command not found → install in section 1

sudo service postgresql status
# Good:    active (running)
# Bad:     inactive → just start it in section 5, do not reinstall

# ── MongoDB ───────────────────────────────────────────────────────────────────
mongod --version
# Good:    db version v4.4.x or higher
# Missing: command not found → install in section 1

sudo service mongod status
# Good:    active (running)
# Bad:     inactive → just start it in section 5, do not reinstall

# ── Java (required by Pig) ───────────────────────────────────────────────────
java -version
# Good:    openjdk version "11.x.x" or higher
# Missing: command not found → install in section 1

echo $JAVA_HOME
# Good:    /usr/lib/jvm/java-11-openjdk-amd64  (any non-empty path)
# Bad:     (blank) → add export to ~/.bashrc in section 1

# ── Apache Pig ────────────────────────────────────────────────────────────────
pig -version
# Good:    Apache Pig version 0.17.0
# Missing: command not found → install in section 1

echo $PIG_HOME
# Good:    /usr/local/pig  (non-empty)
# Bad:     (blank) → add export to ~/.bashrc in section 1

# ── Git ───────────────────────────────────────────────────────────────────────
git --version
# Good:    git version 2.x.x
# Missing: sudo apt install -y git

```

**What each tool is needed for:**

| Tool | Min version | Pipeline(s) |
|------|-------------|-------------|
| Python | 3.10 | all |
| PostgreSQL | 14 | all (result store) |
| MongoDB | 4.4 | MongoDB only |
| Java | 11 | Pig |
| Pig | 0.17 | Pig only |
| mrjob (pip) | 0.7 | MapReduce (no system install needed) |
| Hive | 3.1 | Hive

**Minimum installs by pipeline:**

| If you only want… | You need… | You can skip… |
|-------------------|-----------|---------------|
| MongoDB | Python, PostgreSQL, MongoDB | Java, Pig |
| MapReduce | Python, PostgreSQL | MongoDB, Java, Pig |
| Pig | Python, PostgreSQL, Java, Pig | MongoDB |
| All 4  | Python, PostgreSQL, MongoDB, Java, Pig, Hive |

---

## 1. Prerequisites — install what's missing

Skip any block where section 0 showed the tool already installed.

```bash
# Always safe to run first
sudo apt update && sudo apt upgrade -y
```

**Python 3.12** — skip if `python3 --version` showed 3.10+
```bash
sudo apt install -y python3.12 python3.12-venv python3.12-dev
```

**PostgreSQL 16** — skip if `psql --version` returned anything
```bash
sudo apt install -y postgresql postgresql-contrib
```

**MongoDB 7** — skip if `mongod --version` returned anything
```bash
curl -fsSL https://www.mongodb.org/static/pgp/server-7.0.asc \
  | sudo gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg --dearmor
echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] \
  https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" \
  | sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list
sudo apt update && sudo apt install -y mongodb-org
```

**Java 11** — skip if `java -version` showed 11+
```bash
sudo apt install -y default-jdk
echo 'export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))' >> ~/.bashrc
source ~/.bashrc
# Verify
echo $JAVA_HOME    # should be non-empty
```

**Apache Pig 0.17** — skip if `pig -version` returned anything
```bash
wget https://archive.apache.org/dist/pig/pig-0.17.0/pig-0.17.0.tar.gz
tar -xzf pig-0.17.0.tar.gz -C /usr/local
sudo ln -s /usr/local/pig-0.17.0 /usr/local/pig
echo 'export PIG_HOME=/usr/local/pig' >> ~/.bashrc
echo 'export PATH=$PATH:$PIG_HOME/bin' >> ~/.bashrc
source ~/.bashrc
# Verify
pig -version
```

**PIG_HOME only** (Pig already installed but `echo $PIG_HOME` was blank)
```bash
which pig    # shows e.g. /usr/local/pig/bin/pig → PIG_HOME is /usr/local/pig
echo 'export PIG_HOME=/usr/local/pig' >> ~/.bashrc
source ~/.bashrc
```

**Git** — skip if `git --version` returned anything
```bash
sudo apt install -y git
```

**Apache Hive 3.1** — required for Hive pipeline
```bash
wget https://downloads.apache.org/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
tar -xzf apache-hive-3.1.3-bin.tar.gz
mv apache-hive-3.1.3-bin ~/hive

echo 'export HIVE_HOME=$HOME/hive' >> ~/.bashrc
echo 'export PATH=$HIVE_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
```
---

## 2. Clone the repo

```bash
git clone https://github.com/Aditya-KNV/NoSQL_Project_Phase1_v2.git
cd NoSQL_Project_Phase1_v2
```

---

## 3. Set up Python virtual environment

```bash
# Create venv
python3 -m venv venv

# Activate — run this every time you open a new terminal
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install all dependencies
pip install -r requirements.txt
```

Verify mrjob installed (MapReduce):
```bash
python -c "import mrjob; print(mrjob.__version__)"
# Good: 0.7.x
```

---

## 4. Download and place log files

```bash
mkdir -p data && cd data

wget https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
wget https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz

gunzip NASA_access_log_Jul95.gz
gunzip NASA_access_log_Aug95.gz

cd ..

# Verify line counts
wc -l data/NASA_access_log_Jul95   # ~1,891,715
wc -l data/NASA_access_log_Aug95   # ~1,569,898
```

---

## 5. Start services

```bash
# PostgreSQL
sudo service postgresql start
sudo service postgresql status    # confirm: active (running)

# MongoDB
sudo service mongod start
sudo service mongod status        # confirm: active (running)
```

---

## 6. Set up PostgreSQL user and database

```bash
sudo -u postgres psql << 'SQL'
ALTER USER postgres PASSWORD 'postgres';
CREATE DATABASE nosql_etl;
\q
SQL
```

If you want a different password, also update `PG_CONFIG["password"]` in `config.py`.

Verify connection works:
```bash
psql -h localhost -U postgres -d nosql_etl -c "SELECT 1;"
# Prompts for password → type: postgres
# Good: returns 1 row
```

---

## 7. Edit config.py

Open `config.py` and verify every value:

```python
LOG_FILES = [
    "data/NASA_access_log_Jul95",
    "data/NASA_access_log_Aug95",
]

PG_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "nosql_etl",
    "user":     "postgres",
    "password": "postgres",    # ← must match what you set in section 6
}

MONGO_URI  = "mongodb://localhost:27017"
MONGO_DB   = "nasa_logs"

PIG_HOME   = "/usr/local/pig"  # ← verify: ls /usr/local/pig/bin/pig
PIG_TMP    = "pig_tmp"

MR_TMP_DIR = "mr_tmp"
```

---

## 8. Initialise the PostgreSQL schema

```bash
python main.py --init-db
# Expected: [DB] Schema initialised.
```

Verify all tables exist:
```bash
psql -h localhost -U postgres -d nosql_etl -c "\dt"
```

Expected output:
```
 Schema |       Name        | Type  |  Owner
--------+-------------------+-------+----------
 public | batch_metadata    | table | postgres
 public | q1_daily_traffic  | table | postgres
 public | q2_top_resources  | table | postgres
 public | q3_hourly_errors  | table | postgres
 public | run_metadata      | table | postgres
```

---

## 9. Run the pipelines

Run MongoDB first — fastest and no extra dependencies beyond what's already set up.

```bash
# MongoDB (~4-5 min for full dataset)
python main.py --pipeline mongo

# MapReduce (~varies, runs locally via mrjob, no Hadoop needed)
python main.py --pipeline mapreduce

# Pig (~7-8 min, requires Java + Pig installed)
python main.py --pipeline pig

# Hive (~15-20 min, requires HiveServer2 + Hadoop services)
python main.py --pipeline hive

```

Or use the interactive menu:
```bash
python main.py
```

---

### Hive

Start Hadoop + Hive services:

```bash
# Hadoop DFS
start-dfs.sh

# YARN
start-yarn.sh

# Start HiveServer2
hiveserver2
```

Open another terminal:

```bash
cd NoSQL_Project_Phase1_v2
source venv/bin/activate
```

Verify Hive is working:

```bash
hive
```

Inside Hive:

```sql
CREATE DATABASE nasa_logs;
SHOW DATABASES;
```

Run Hive pipeline:

```bash
python main.py --pipeline hive
```

Interactive menu:

```bash
python main.py
```

Then select:

```text
4. Hive (HiveServer2)
```

Verify Hadoop jobs:

```bash
yarn application -list
```

Verify HiveServer2 running:

```bash
netstat -tulnp | grep 10000
```

## 10. View reports

```bash
# Latest run
python main.py --report

# Latest run for a specific pipeline
python main.py --report --pipeline mongo
python main.py --report --pipeline mapreduce
python main.py --report --pipeline pig
python main.py --report --pipeline hive

# Specific run by ID (run_id is printed when each pipeline finishes)
python main.py --report --run-id <run_id>
```

Query results directly in PostgreSQL:
```bash
psql -h localhost -U postgres -d nosql_etl
```

```sql
-- All runs, most recent first
SELECT pipeline, run_id, started_at, runtime_seconds,
       total_records, total_batches, malformed_count
FROM run_metadata ORDER BY started_at DESC LIMIT 10;

-- Batch breakdown for a run
SELECT batch_id, batch_date, batch_size, malformed_count, runtime_seconds
FROM batch_metadata
WHERE run_id = '<your_run_id>' ORDER BY batch_id;

-- Q1: daily traffic (first 20 rows)
SELECT log_date, status_code, request_count, total_bytes
FROM q1_daily_traffic
WHERE run_id = '<your_run_id>'
ORDER BY log_date, status_code LIMIT 20;

-- Q2: top 20 resources
SELECT resource_path, request_count, total_bytes, distinct_host_count
FROM q2_top_resources
WHERE run_id = '<your_run_id>'
ORDER BY request_count DESC LIMIT 20;

-- Q3: hourly errors (first 20 rows)
SELECT log_date, log_hour, error_request_count,
       total_request_count, error_rate, distinct_error_hosts
FROM q3_hourly_errors
WHERE run_id = '<your_run_id>'
ORDER BY log_date, log_hour LIMIT 20;

```

---

## 11. Clean everything and start fresh

Use this to fully reset before a re-run or demo.

```bash
# ── PostgreSQL: drop and recreate database ────────────────────────────────────
sudo -u postgres psql -c "DROP DATABASE IF EXISTS nosql_etl;"
sudo -u postgres psql -c "CREATE DATABASE nosql_etl;"

# ── MongoDB: drop nasa_logs database ─────────────────────────────────────────
mongosh --eval "db.getSiblingDB('nasa_logs').dropDatabase()"

# Verify MongoDB is clean
mongosh --eval "db.adminCommand({listDatabases:1}).databases.forEach(d => print(d.name))"
# nasa_logs should NOT appear

# ── Remove all scratch directories ───────────────────────────────────────────
rm -rf pig_tmp mr_tmp

# ── Remove Python cache ───────────────────────────────────────────────────────
find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null
find . -name "*.pyc" -delete 2>/dev/null

# ── Reinitialise schema ───────────────────────────────────────────────────────
python main.py --init-db
```

Recreate venv from scratch (only if you suspect package issues):
```bash
deactivate
rm -rf venv
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

---

## 12. Smoke tests — verify each pipeline before full run

Run these quick tests to confirm each pipeline works before processing 3.4M records.

### MongoDB
```bash
mongosh --eval "db.runCommand({ping:1})"
# Good: { ok: 1 }
```

### MapReduce (mrjob local — no Hadoop needed)
```bash
python -c "
from mrjob.job import MRJob
from mrjob.step import MRStep
import sys

class MRTest(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]
    def mapper(self, _, line):
        yield 'count', 1
    def reducer(self, key, values):
        yield key, sum(values)

# write a tiny test file
with open('/tmp/mr_test.txt', 'w') as f:
    f.write('a\nb\nc\n')

mr = MRTest(args=['--runner=local', '/tmp/mr_test.txt'])
with mr.make_runner() as runner:
    runner.run()
    for k, v in mr.parse_output(runner.cat_output()):
        print(k, v)
# Good: count 3
"
```

### Pig
```bash
echo -e "a\tb\na\tb" > /tmp/pig_test.tsv
pig -x local -e "
data = LOAD '/tmp/pig_test.tsv' USING PigStorage('\t') AS (f1:chararray, f2:chararray);
grp  = GROUP data ALL;
cnt  = FOREACH grp GENERATE COUNT(data);
DUMP cnt;
"
# Good: (2)
```

---

## 13. Typical session (after first-time setup)

```bash
cd NoSQL_Project_Phase1_v2
source venv/bin/activate

# Start services
sudo service postgresql start
sudo service mongod start

# Start Hadoop + Hive
start-dfs.sh
start-yarn.sh
hiveserver2

# Run pipelines
python main.py --pipeline mongo
python main.py --pipeline mapreduce
python main.py --pipeline pig
python main.py --pipeline hive

# View report
python main.py --report
```

---

## 14. Troubleshooting

**`psycopg2.OperationalError: could not connect to server`**
```bash
sudo service postgresql start
# If it still fails, check pg_hba.conf allows md5/scram auth for localhost:
sudo -u postgres psql -c "SHOW hba_file;"
# Then open that file and ensure localhost has 'md5' or 'scram-sha-256'
```

**`pymongo.errors.ServerSelectionTimeoutError`**
```bash
sudo service mongod start
# If mongod fails to start (lock file issue):
sudo rm /var/lib/mongodb/mongod.lock
sudo mongod --repair
sudo service mongod start
```

**`RuntimeError: Pig failed`**
```bash
java -version          # must be 11+
echo $JAVA_HOME        # must be non-empty
pig -version           # must show 0.17.0
ls $PIG_HOME/bin/pig   # file must exist
# Also check config.py: PIG_HOME must match actual install path
```

**`Error: output path already exists` (Pig)**
```bash
rm -rf pig_tmp
python main.py --pipeline pig
```

**`mrjob` errors / MapReduce pipeline fails**
```bash
# mrjob runs locally — no Hadoop needed
# Most common cause: records not serialising correctly
python -c "import mrjob; print(mrjob.__version__)"  # must be 0.7+
# If import fails:
pip install mrjob --upgrade
```

**`ModuleNotFoundError`**
```bash
source venv/bin/activate   # venv must be active
pip install -r requirements.txt
```

**`permission denied` on PostgreSQL**
```bash
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'postgres';"
```
**`Could not connect to localhost:10000`**
```bash
hiveserver2
netstat -tulnp | grep 10000
```

**`Database does not exist: nasa_logs`**
```bash
hive

CREATE DATABASE nasa_logs;
SHOW DATABASES;
```

**`Hive metastore/schema errors`**
```bash
rm -rf ~/metastore_db
rm -f ~/derby.log

$HIVE_HOME/bin/schematool -dbType derby -initSchema
```


