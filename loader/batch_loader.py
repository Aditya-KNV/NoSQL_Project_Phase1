# ─── loader/batch_loader.py ──────────────────────────────────────────────────
"""
Phase 2 batch loader — splits log files into one batch per calendar day.

Uses the same parse_line() from parser/log_parser.py as Phase 1.
Records are grouped by log_date (already present in the parsed dict).

Malformed lines are counted but dropped — not stored anywhere.

Each yielded batch dict:
    batch_id      : str        e.g. "batch-001"
    date          : str        "YYYY-MM-DD"
    source_file   : str
    batch_size    : int        count of valid records
    records       : list[dict] parsed dicts (malformed=False only)
    malformed_count : int      count of unparseable lines in this batch
    start_ts      : str | None earliest timestamp ISO string in batch
    end_ts        : str | None latest  timestamp ISO string in batch
"""

import os
from collections import defaultdict
from parser.log_parser import parse_line


def load_batches(log_files: list):
    """
    Generator. Reads all log_files, groups valid records by log_date,
    yields one batch dict per day in chronological order.
    Malformed lines are counted per day and then dropped.
    """
    day_records        = defaultdict(list)
    day_source         = {}
    day_malformed_count = defaultdict(int)
    ungrouped_malformed = 0   # malformed lines with no parseable date

    for path in log_files:
        src = os.path.basename(path)
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as fh:
                for line in fh:
                    if not line.strip():
                        continue
                    record = parse_line(line)
                    if record.get("malformed"):
                        ungrouped_malformed += 1
                    else:
                        day = record["log_date"]
                        day_records[day].append(record)
                        day_source[day] = src
        except FileNotFoundError:
            print(f"[batch_loader] File not found: {path}")

    sorted_days = sorted(day_records.keys())
    for idx, day in enumerate(sorted_days, start=1):
        records    = day_records[day]
        timestamps = [r["timestamp"] for r in records if r.get("timestamp")]
        # attach all ungrouped malformed count to first batch for run total tracking
        extra = ungrouped_malformed if idx == 1 else 0
        yield {
            "batch_id":        f"batch-{idx:03d}",
            "date":            day,
            "source_file":     day_source.get(day, ""),
            "batch_size":      len(records),
            "records":         records,
            "malformed_count": extra,
            "start_ts":        min(timestamps) if timestamps else None,
            "end_ts":          max(timestamps) if timestamps else None,
        }
