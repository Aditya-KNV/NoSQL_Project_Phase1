# ─── parser/log_parser.py ────────────────────────────────────────────────────
"""
Parses raw NASA HTTP log lines into structured dictionaries.

Expected raw format:
  host - - [DD/Mon/YYYY:HH:MM:SS -ZONE] "METHOD /path HTTP/x.x" status bytes

Returns a dict with keys:
  host, timestamp, log_date, log_hour, http_method,
  resource_path, protocol_version, status_code, bytes_transferred, raw_line

Malformed lines are returned with a special key: malformed=True
"""

import re
from datetime import datetime

# Pre-compiled regex for the CLF (Common Log Format) line
_LOG_RE = re.compile(
    r'(?P<host>\S+)'                         # host
    r'\s+\S+\s+\S+\s+'                       # ident / authuser (ignored)
    r'\[(?P<timestamp>[^\]]+)\]'             # [timestamp]
    r'\s+"(?P<request>[^"]*)"'               # "request"
    r'\s+(?P<status>\S+)'                    # status code
    r'\s+(?P<bytes>\S+)'                     # bytes
)

_REQUEST_RE = re.compile(
    r'(?P<method>\S+)\s+(?P<path>\S+)(?:\s+(?P<protocol>\S+))?'
)

MONTH_MAP = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
    "May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
    "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}


def parse_timestamp(ts_str: str):
    """
    Parse '01/Jul/1995:00:00:01 -0400' into a datetime object.
    Returns None on failure.
    """
    try:
        # Strip timezone offset for simplicity
        ts_str = ts_str.strip()
        if " " in ts_str:
            ts_str = ts_str.rsplit(" ", 1)[0]   # drop -0400
        return datetime.strptime(ts_str, "%d/%b/%Y:%H:%M:%S")
    except Exception:
        return None


def parse_line(raw_line: str) -> dict:
    """
    Parse one raw log line.
    Always returns a dict.  If parsing fails, dict contains malformed=True.
    """
    line = raw_line.strip()
    m = _LOG_RE.match(line)
    if not m:
        return {"malformed": True, "raw_line": line}

    host        = m.group("host")
    ts_str      = m.group("timestamp")
    request_str = m.group("request")
    status_raw  = m.group("status")
    bytes_raw   = m.group("bytes")

    # ── Parse timestamp ───────────────────────────────────────────────────────
    dt = parse_timestamp(ts_str)
    if dt is None:
        return {"malformed": True, "raw_line": line}

    log_date = dt.strftime("%Y-%m-%d")
    log_hour = dt.hour

    # ── Parse request field ───────────────────────────────────────────────────
    rm = _REQUEST_RE.match(request_str)
    if rm:
        http_method       = rm.group("method").upper()
        resource_path     = rm.group("path")
        protocol_version  = rm.group("protocol") or "UNKNOWN"
    else:
        http_method      = "UNKNOWN"
        resource_path    = request_str or "/"
        protocol_version = "UNKNOWN"

    # ── Parse status code ─────────────────────────────────────────────────────
    try:
        status_code = int(status_raw)
    except ValueError:
        status_code = 0

    # ── Parse bytes ───────────────────────────────────────────────────────────
    if bytes_raw == "-" or bytes_raw == "":
        bytes_transferred = 0
    else:
        try:
            bytes_transferred = int(bytes_raw)
        except ValueError:
            bytes_transferred = 0

    return {
        "malformed":          False,
        "host":               host,
        "timestamp":          dt.isoformat(),
        "log_date":           log_date,
        "log_hour":           log_hour,
        "http_method":        http_method,
        "resource_path":      resource_path,
        "protocol_version":   protocol_version,
        "status_code":        status_code,
        "bytes_transferred":  bytes_transferred,
        "raw_line":           line,
    }


def parse_files(file_paths: list, batch_size: int):
    """
    Generator that yields (batch_id, batch_records, malformed_count) tuples.
    batch_records is a list of parsed dicts (malformed=False only).
    malformed_count is count of bad lines in that batch.
    """
    batch_id       = 0
    batch          = []
    malformed_count = 0

    for file_path in file_paths:
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                for raw_line in f:
                    if not raw_line.strip():
                        continue

                    record = parse_line(raw_line)

                    if record.get("malformed"):
                        malformed_count += 1
                    else:
                        batch.append(record)

                    if len(batch) >= batch_size:
                        batch_id += 1
                        yield batch_id, batch, malformed_count
                        batch           = []
                        malformed_count = 0

        except FileNotFoundError:
            print(f"[ERROR] File not found: {file_path}")

    # Final (possibly partial) batch
    if batch:
        batch_id += 1
        yield batch_id, batch, malformed_count
