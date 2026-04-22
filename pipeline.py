import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb

from config import load_config, PipelineConfig
from lake_io import append_run_log, read_watermark
from bronze_loaders import (
    load_bronze_transaction_codes,
    load_bronze_accounts,
    load_bronze_transactions,
)


@dataclass
class PhaseResult:
    success: bool
    records_processed: int
    records_written: int
    error: str | None


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


def _append_phase_log(
    data_dir: str,
    run_id: str,
    mode: str,
    model_name: str,
    started_at: datetime,
    result: dict,
    error_message: str | None = None,
) -> None:
    append_run_log(data_dir, {
        "run_id": run_id,
        "pipeline_type": mode,
        "model_name": model_name,
        "layer": "BRONZE",
        "started_at": started_at,
        "completed_at": datetime.utcnow(),
        "status": result.get("status", "SUCCESS"),
        "records_processed": result.get("records_processed", 0),
        "records_written": result.get("records_written", 0),
        "records_rejected": None,
        "error_message": error_message,
    })


def _append_failed_log(
    data_dir: str,
    run_id: str,
    mode: str,
    model_name: str,
    started_at: datetime,
    error_message: str,
) -> None:
    append_run_log(data_dir, {
        "run_id": run_id,
        "pipeline_type": mode,
        "model_name": model_name,
        "layer": "BRONZE",
        "started_at": started_at,
        "completed_at": datetime.utcnow(),
        "status": "FAILED",
        "records_processed": 0,
        "records_written": 0,
        "records_rejected": None,
        "error_message": error_message,
    })


def _call_loader(config, run_id, loader_fn, model_name, *loader_args):
    started_at = datetime.utcnow()
    try:
        result = loader_fn(config, *loader_args, run_id)
    except Exception as e:
        _append_failed_log(config.data_dir, run_id, config.mode, model_name, started_at, str(e))
        return None, str(e)
    if result.get("status") == "FAILED":
        err = result.get("error", "loader returned FAILED")
        _append_failed_log(config.data_dir, run_id, config.mode, model_name, started_at, err)
        return None, err
    _append_phase_log(config.data_dir, run_id, config.mode, model_name, started_at, result)
    return result, None


def _run_bronze_historical(config: PipelineConfig, run_id: str, totals: list) -> str | None:
    current = config.start_date
    while current <= config.end_date:
        date_str = current.strftime("%Y-%m-%d")

        acc_result, err = _call_loader(config, run_id, load_bronze_accounts, f"accounts_{date_str}", current)
        if err:
            return err
        totals[0] += acc_result["records_processed"]
        totals[1] += acc_result["records_written"]

        txn_result, err = _call_loader(config, run_id, load_bronze_transactions, f"transactions_{date_str}", current)
        if err:
            return err
        totals[0] += txn_result["records_processed"]
        totals[1] += txn_result["records_written"]

        current += timedelta(days=1)
    return None


def _run_bronze_incremental(config: PipelineConfig, run_id: str, totals: list) -> str | None:
    watermark = read_watermark(config.data_dir)
    next_date = watermark + timedelta(days=1)
    date_str = next_date.strftime("%Y-%m-%d")

    acc_result, err = _call_loader(config, run_id, load_bronze_accounts, f"accounts_{date_str}", next_date)
    if err:
        return err
    totals[0] += acc_result["records_processed"]
    totals[1] += acc_result["records_written"]

    txn_result, err = _call_loader(config, run_id, load_bronze_transactions, f"transactions_{date_str}", next_date)
    if err:
        return err
    totals[0] += txn_result["records_processed"]
    totals[1] += txn_result["records_written"]

    return None


def run_bronze_phase(config: PipelineConfig, run_id: str) -> PhaseResult:
    Path(config.data_dir, "pipeline").mkdir(parents=True, exist_ok=True)
    totals = [0, 0]

    tc_result, err = _call_loader(config, run_id, load_bronze_transaction_codes, "transaction_codes")
    if err:
        return PhaseResult(success=False, records_processed=0, records_written=0, error=err)
    totals[0] += tc_result["records_processed"]
    totals[1] += tc_result["records_written"]

    if config.mode == "historical":
        err = _run_bronze_historical(config, run_id, totals)
    else:
        err = _run_bronze_incremental(config, run_id, totals)

    if err:
        return PhaseResult(success=False, records_processed=totals[0], records_written=totals[1], error=err)
    return PhaseResult(success=True, records_processed=totals[0], records_written=totals[1], error=None)


def main():
    config = load_config()
    validate_source_files(config)
    run_id = datetime.utcnow().strftime("run-%Y%m%d-%H%M%S")
    print(f"Startup validation complete. Pipeline mode: {config.mode}. run_id: {run_id}")
    result = run_bronze_phase(config, run_id)
    print(f"Bronze phase: {result}")
    if not result.success:
        sys.exit(1)


if __name__ == "__main__":
    main()
