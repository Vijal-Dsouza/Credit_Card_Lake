import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from dotenv import load_dotenv
import os


@dataclass
class PipelineConfig:
    mode: str
    data_dir: str
    source_dir: str
    start_date: date | None
    end_date: date | None


def _parse_date(value: str, name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        print(f"ERROR: {name} must be a valid date in YYYY-MM-DD format. Got: {value!r}")
        sys.exit(1)


def load_config() -> PipelineConfig:
    load_dotenv()

    mode = os.environ.get("PIPELINE_MODE", "").strip()
    if mode not in ("historical", "incremental"):
        print(
            f"ERROR: PIPELINE_MODE must be 'historical' or 'incremental'. "
            f"Got: {mode!r}"
        )
        sys.exit(1)

    data_dir = os.environ.get("DATA_DIR", "").strip()
    if not data_dir:
        print("ERROR: DATA_DIR is required but not set.")
        sys.exit(1)

    source_dir = os.environ.get("SOURCE_DIR", "").strip()
    if not source_dir:
        print("ERROR: SOURCE_DIR is required but not set.")
        sys.exit(1)

    start_date = None
    end_date = None

    if mode == "historical":
        raw_start = os.environ.get("START_DATE", "").strip()
        raw_end = os.environ.get("END_DATE", "").strip()
        if not raw_start:
            print("ERROR: START_DATE is required for historical mode.")
            sys.exit(1)
        if not raw_end:
            print("ERROR: END_DATE is required for historical mode.")
            sys.exit(1)
        start_date = _parse_date(raw_start, "START_DATE")
        end_date = _parse_date(raw_end, "END_DATE")
        if start_date > end_date:
            print(
                f"ERROR: START_DATE ({start_date}) must be <= END_DATE ({end_date})."
            )
            sys.exit(1)

    # R3: delete stale dbt catalog before any phase function runs
    catalog_path = Path(data_dir) / "pipeline" / "dbt_catalog.duckdb"
    catalog_path.unlink(missing_ok=True)

    return PipelineConfig(
        mode=mode,
        data_dir=data_dir,
        source_dir=source_dir,
        start_date=start_date,
        end_date=end_date,
    )
