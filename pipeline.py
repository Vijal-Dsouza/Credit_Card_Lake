import sys
from pathlib import Path
from datetime import date, timedelta

import duckdb

from config import load_config, PipelineConfig


def validate_source_files(config: PipelineConfig) -> None:
    missing = []

    if config.mode == "historical":
        current = config.start_date
        while current <= config.end_date:
            date_str = current.strftime("%Y-%m-%d")
            for prefix in ("transactions", "accounts"):
                f = Path(config.source_dir) / f"{prefix}_{date_str}.csv"
                if not f.exists():
                    missing.append(str(f))
            current += timedelta(days=1)

        codes = Path(config.source_dir) / "transaction_codes.csv"
        if not codes.exists():
            missing.append(str(codes))

    elif config.mode == "incremental":
        control_path = Path(config.data_dir) / "pipeline" / "control.parquet"
        if not control_path.exists():
            print(
                f"ERROR: control.parquet not found at {control_path}. "
                "Run historical pipeline first to initialise the watermark."
            )
            sys.exit(1)

        last_date = duckdb.execute(
            f"SELECT last_processed_date FROM '{control_path}'"
        ).fetchone()[0]
        next_date = last_date + timedelta(days=1)
        date_str = next_date.strftime("%Y-%m-%d")
        for prefix in ("transactions", "accounts"):
            f = Path(config.source_dir) / f"{prefix}_{date_str}.csv"
            if not f.exists():
                missing.append(str(f))

    if missing:
        print("ERROR: Required source files are missing:")
        for f in missing:
            print(f"  {f}")
        sys.exit(1)

    print("Source file pre-flight: PASS")


def main():
    config = load_config()
    validate_source_files(config)
    print(f"Startup validation complete. Pipeline mode: {config.mode}")


if __name__ == "__main__":
    main()
