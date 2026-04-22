import re
from datetime import date, datetime
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def read_watermark(data_dir: str) -> date | None:
    path = Path(data_dir) / "pipeline" / "control.parquet"
    if not path.exists():
        return None
    table = pq.read_table(str(path))
    df = table.to_pandas()
    if df.empty:
        return None
    val = df["last_processed_date"].iloc[0]
    if hasattr(val, "date"):
        return val.date()
    return val


def write_watermark(data_dir: str, processed_date: date, run_id: str) -> None:
    path = Path(data_dir) / "pipeline" / "control.parquet"
    table = pa.table({
        "last_processed_date": pa.array([processed_date], type=pa.date32()),
        "updated_at": pa.array([datetime.utcnow()], type=pa.timestamp("us")),
        "updated_by_run_id": pa.array([run_id], type=pa.string()),
    })
    pq.write_table(table, str(path))


def sanitise_error_message(raw: str) -> str:
    traceback_marker = "Traceback (most recent call last)"
    if traceback_marker in raw:
        raw = raw[: raw.index(traceback_marker)]

    cleaned = re.sub(r'(/|\./|\.\./)[^\s,\'"]+', "[path redacted]", raw)
    cleaned = cleaned[:500]

    if not cleaned.strip():
        return "error detail redacted"
    return cleaned


def append_run_log(data_dir: str, row: dict) -> None:
    path = Path(data_dir) / "pipeline" / "run_log.parquet"

    if row.get("error_message") is not None:
        row = dict(row)
        row["error_message"] = sanitise_error_message(row["error_message"])

    schema = pa.schema([
        ("run_id", pa.string()),
        ("pipeline_type", pa.string()),
        ("model_name", pa.string()),
        ("layer", pa.string()),
        ("started_at", pa.timestamp("us")),
        ("completed_at", pa.timestamp("us")),
        ("status", pa.string()),
        ("records_processed", pa.int64()),
        ("records_written", pa.int64()),
        ("records_rejected", pa.int64()),
        ("error_message", pa.string()),
    ])

    new_table = pa.table({
        "run_id": pa.array([row["run_id"]], type=pa.string()),
        "pipeline_type": pa.array([row["pipeline_type"]], type=pa.string()),
        "model_name": pa.array([row["model_name"]], type=pa.string()),
        "layer": pa.array([row["layer"]], type=pa.string()),
        "started_at": pa.array([row["started_at"]], type=pa.timestamp("us")),
        "completed_at": pa.array([row["completed_at"]], type=pa.timestamp("us")),
        "status": pa.array([row["status"]], type=pa.string()),
        "records_processed": pa.array([row["records_processed"]], type=pa.int64()),
        "records_written": pa.array([row["records_written"]], type=pa.int64()),
        "records_rejected": pa.array([row.get("records_rejected")], type=pa.int64()),
        "error_message": pa.array([row.get("error_message")], type=pa.string()),
    })

    if path.exists():
        existing_table = pq.read_table(str(path))
        combined = pa.concat_tables([existing_table, new_table])
    else:
        combined = new_table

    pq.write_table(combined, str(path))


def run_log_exists(data_dir: str) -> bool:
    path = Path(data_dir) / "pipeline" / "run_log.parquet"
    if not path.exists():
        return False
    table = pq.read_table(str(path))
    return len(table) > 0
