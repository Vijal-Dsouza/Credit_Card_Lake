import sys
from datetime import date, datetime
from pathlib import Path

import duckdb

from config import PipelineConfig
from lake_io import append_run_log


def _verify_partition_integrity(path: str, expected: int) -> int:
    try:
        actual = duckdb.execute(f"SELECT COUNT(*) FROM '{path}'").fetchone()[0]
    except Exception as e:
        print(f"ERROR: Written Parquet unreadable at {path} — {e}")
        sys.exit(1)
    if actual != expected:
        print(f"ERROR: Row count mismatch — expected {expected}, read {actual} from {path}")
        sys.exit(1)
    return actual


def _warn_empty_source(
    config: PipelineConfig,
    run_id: str,
    model_name: str,
    started_at: datetime,
    error_message: str,
) -> None:
    Path(config.data_dir, "pipeline").mkdir(parents=True, exist_ok=True)
    append_run_log(config.data_dir, {
        "run_id": run_id,
        "pipeline_type": config.mode,
        "model_name": model_name,
        "layer": "BRONZE",
        "started_at": started_at,
        "completed_at": datetime.utcnow(),
        "status": "WARNING",
        "records_processed": 0,
        "records_written": 0,
        "records_rejected": None,
        "error_message": error_message,
    })


def load_bronze_transaction_codes(config: PipelineConfig, run_id: str) -> dict:
    out_path = Path(config.data_dir) / "bronze" / "transaction_codes" / "data.parquet"

    if out_path.exists():
        print("Bronze transaction_codes already loaded — skipping.")
        return {"status": "SKIPPED", "records_processed": 0, "records_written": 0}

    started_at = datetime.utcnow()
    src = (Path(config.source_dir) / "transaction_codes.csv").as_posix()
    source_count = duckdb.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{src}')"
    ).fetchone()[0]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    ingested_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
    out = out_path.as_posix()

    duckdb.execute(f"""
        COPY (
            SELECT *,
                'transaction_codes.csv' AS _source_file,
                CAST('{ingested_at}' AS TIMESTAMP) AS _ingested_at,
                '{run_id}' AS _pipeline_run_id
            FROM read_csv_auto('{src}')
        ) TO '{out}' (FORMAT PARQUET)
    """)

    verified_count = _verify_partition_integrity(out, source_count)

    if source_count == 0:
        _warn_empty_source(
            config, run_id, "transaction_codes", started_at,
            "transaction_codes.csv contained 0 rows — Bronze partition written but empty. "
            "Analyst review required.",
        )
        return {"status": "WARNING", "records_processed": 0, "records_written": 0}

    return {"status": "SUCCESS", "records_processed": source_count, "records_written": verified_count}
