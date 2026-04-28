import os
import shutil
import subprocess
import sys
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb

from config import load_config, PipelineConfig
from lake_io import append_run_log, read_watermark, write_watermark
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
    _append_phase_log(config.data_dir, run_id, config.mode, model_name, started_at, result, result.get("error_message"))
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
    if watermark is None:
        return "Incremental mode requires a prior historical run — no watermark found in pipeline/control.parquet."
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


_DBT_PROJECT_DIR = Path(__file__).parent / "dbt_project"


def _append_log(
    data_dir: str,
    run_id: str,
    mode: str,
    model_name: str,
    layer: str,
    started_at: datetime,
    status: str,
    error_message: str | None = None,
) -> None:
    append_run_log(data_dir, {
        "run_id": run_id,
        "pipeline_type": mode,
        "model_name": model_name,
        "layer": layer,
        "started_at": started_at,
        "completed_at": datetime.utcnow(),
        "status": status,
        "records_processed": 0,
        "records_written": 0,
        "records_rejected": None,
        "error_message": error_message,
    })


def _rename_quarantine_partitions(data_dir: str) -> None:
    for f in Path(data_dir, "silver", "quarantine").rglob("rejected0.parquet"):
        f.replace(f.parent / "rejected.parquet")


def _run_dbt_build(model_name: str, config: PipelineConfig) -> subprocess.CompletedProcess:
    scripts_dir = Path(sys.executable).parent
    dbt_cmd = next(
        (str(c) for c in (scripts_dir / "dbt.exe", scripts_dir / "dbt.cmd", scripts_dir / "dbt") if c.exists()),
        shutil.which("dbt") or "dbt",
    )
    env = {**os.environ, "DATA_DIR": config.data_dir, "SOURCE_DIR": config.source_dir}
    return subprocess.run(
        [dbt_cmd, "build", "--select", model_name],
        cwd=str(_DBT_PROJECT_DIR),
        capture_output=True,
        text=True,
        env=env,
    )


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


def run_silver_phase(config: PipelineConfig, run_id: str) -> PhaseResult:
    run_log_path = Path(config.data_dir) / "pipeline" / "run_log.parquet"
    if run_log_path.exists():
        bronze_warnings = duckdb.execute(
            f"SELECT COUNT(*) FROM read_parquet('{run_log_path}') "
            "WHERE run_id = ? AND layer = 'BRONZE' AND status = 'WARNING'",
            [run_id],
        ).fetchone()[0]
        if bronze_warnings > 0:
            _append_log(
                config.data_dir, run_id, config.mode, "silver_phase_start", "SILVER",
                datetime.utcnow(), "WARNING",
                "One or more Bronze partitions have zero rows — Silver will process empty "
                "input for those dates. Analyst review recommended.",
            )

    tc_path = Path(config.data_dir) / "silver" / "transaction_codes" / "data.parquet"
    tc_started = datetime.utcnow()

    if not tc_path.exists():
        _append_log(
            config.data_dir, run_id, config.mode, "silver_transaction_codes", "SILVER",
            tc_started, "FAILED",
            "silver_transaction_codes absent or empty — cannot promote transactions",
        )
        return PhaseResult(success=False, records_processed=0, records_written=0,
                           error="silver_transaction_codes absent or empty — cannot promote transactions")

    row_count = duckdb.execute(f"SELECT COUNT(*) FROM read_parquet('{tc_path}')").fetchone()[0]
    if row_count == 0:
        _append_log(
            config.data_dir, run_id, config.mode, "silver_transaction_codes", "SILVER",
            tc_started, "FAILED",
            "silver_transaction_codes absent or empty — cannot promote transactions",
        )
        return PhaseResult(success=False, records_processed=0, records_written=0,
                           error="silver_transaction_codes absent or empty — cannot promote transactions")

    _append_log(
        config.data_dir, run_id, config.mode, "silver_transaction_codes", "SILVER",
        tc_started, "SKIPPED",
    )

    for model_name in ("silver_accounts", "silver_quarantine", "silver_transactions"):
        started_at = datetime.utcnow()
        proc = _run_dbt_build(model_name, config)
        if proc.returncode != 0:
            _append_log(
                config.data_dir, run_id, config.mode, model_name, "SILVER",
                started_at, "FAILED",
                proc.stderr or proc.stdout,
            )
            return PhaseResult(success=False, records_processed=0, records_written=0,
                               error=proc.stderr or proc.stdout)
        if model_name == "silver_quarantine":
            _rename_quarantine_partitions(config.data_dir)
        _append_log(
            config.data_dir, run_id, config.mode, model_name, "SILVER",
            started_at, "SUCCESS",
        )

    return PhaseResult(success=True, records_processed=0, records_written=0, error=None)


def run_gold_phase(config: PipelineConfig, run_id: str) -> PhaseResult:
    run_log_path = Path(config.data_dir) / "pipeline" / "run_log.parquet"
    if run_log_path.exists():
        upstream_warnings = duckdb.execute(
            f"SELECT COUNT(*) FROM read_parquet('{run_log_path}') "
            "WHERE run_id = ? AND layer IN ('BRONZE', 'SILVER') AND status = 'WARNING'",
            [run_id],
        ).fetchone()[0]
        if upstream_warnings > 0:
            _append_log(
                config.data_dir, run_id, config.mode, "gold_phase_start", "GOLD",
                datetime.utcnow(), "WARNING",
                "Upstream WARNING entries detected — Gold aggregations may reflect empty "
                "Bronze partitions. Check run_log for Bronze/Silver WARNING entries.",
            )

    for model_name in ("gold_daily_summary", "gold_weekly_account_summary"):
        started_at = datetime.utcnow()
        proc = _run_dbt_build(model_name, config)
        if proc.returncode != 0:
            _append_log(
                config.data_dir, run_id, config.mode, model_name, "GOLD",
                started_at, "FAILED",
                proc.stderr or proc.stdout,
            )
            return PhaseResult(success=False, records_processed=0, records_written=0,
                               error=proc.stderr or proc.stdout)
        _append_log(
            config.data_dir, run_id, config.mode, model_name, "GOLD",
            started_at, "SUCCESS",
        )

    return PhaseResult(success=True, records_processed=0, records_written=0, error=None)


def _check_silver_accounts(config: PipelineConfig, run_id: str) -> None:
    acc_path = Path(config.data_dir) / "silver" / "accounts" / "data.parquet"
    error_msg = (
        "silver_accounts absent or empty — incremental Silver phase requires a baseline "
        "accounts snapshot from the historical run. Re-run historical pipeline first."
    )
    if not acc_path.exists() or duckdb.execute(f"SELECT COUNT(*) FROM '{acc_path}'").fetchone()[0] == 0:
        _append_log(
            config.data_dir, run_id, config.mode, "pipeline_failed", "SILVER",
            datetime.utcnow(), "FAILED", error_msg,
        )
        print(f"ERROR: {error_msg}")
        sys.exit(1)


def _run_historical(config: PipelineConfig) -> None:
    run_id = str(uuid.uuid4())
    validate_source_files(config)
    _append_log(
        config.data_dir, run_id, config.mode, "pipeline_start", "BRONZE",
        datetime.utcnow(), "SUCCESS",
    )

    result = run_bronze_phase(config, run_id)
    if not result.success:
        _append_log(config.data_dir, run_id, config.mode, "pipeline_failed", "BRONZE",
                    datetime.utcnow(), "FAILED", result.error)
        print(f"ERROR: Bronze phase failed: {result.error}")
        sys.exit(1)

    result = run_silver_phase(config, run_id)
    if not result.success:
        _append_log(config.data_dir, run_id, config.mode, "pipeline_failed", "SILVER",
                    datetime.utcnow(), "FAILED", result.error)
        print(f"ERROR: Silver phase failed: {result.error}")
        sys.exit(1)

    result = run_gold_phase(config, run_id)
    if not result.success:
        _append_log(config.data_dir, run_id, config.mode, "pipeline_failed", "GOLD",
                    datetime.utcnow(), "FAILED", result.error)
        print(f"ERROR: Gold phase failed: {result.error}")
        sys.exit(1)

    try:
        write_watermark(config.data_dir, config.end_date, run_id)
    except Exception as e:
        _append_log(config.data_dir, run_id, config.mode, "pipeline_failed", "GOLD",
                    datetime.utcnow(), "FAILED", f"Watermark write failed: {e}")
        print(f"ERROR: Watermark write failed: {e}")
        sys.exit(1)

    wm = read_watermark(config.data_dir)
    assert wm == config.end_date, f"Watermark verify FAIL: wrote {config.end_date}, read {wm}"
    print(f"Pipeline complete. Watermark advanced to {config.end_date}.")
    sys.exit(0)


def _run_incremental(config: PipelineConfig) -> None:
    run_id = str(uuid.uuid4())

    watermark = read_watermark(config.data_dir)
    if watermark is None:
        print("No watermark found. Run historical pipeline first.")
        sys.exit(1)

    next_date = watermark + timedelta(days=1)
    validate_source_files(config)
    _check_silver_accounts(config, run_id)

    result = run_bronze_phase(config, run_id)
    if not result.success:
        _append_log(config.data_dir, run_id, config.mode, "pipeline_failed", "BRONZE",
                    datetime.utcnow(), "FAILED", result.error)
        print(f"ERROR: Bronze phase failed: {result.error}")
        sys.exit(1)

    result = run_silver_phase(config, run_id)
    if not result.success:
        _append_log(config.data_dir, run_id, config.mode, "pipeline_failed", "SILVER",
                    datetime.utcnow(), "FAILED", result.error)
        print(f"ERROR: Silver phase failed: {result.error}")
        sys.exit(1)

    result = run_gold_phase(config, run_id)
    if not result.success:
        _append_log(config.data_dir, run_id, config.mode, "pipeline_failed", "GOLD",
                    datetime.utcnow(), "FAILED", result.error)
        print(f"ERROR: Gold phase failed: {result.error}")
        sys.exit(1)

    try:
        write_watermark(config.data_dir, next_date, run_id)
    except Exception as e:
        _append_log(config.data_dir, run_id, config.mode, "pipeline_failed", "GOLD",
                    datetime.utcnow(), "FAILED", f"Watermark write failed: {e}")
        print(f"ERROR: Watermark write failed: {e}")
        sys.exit(1)

    wm = read_watermark(config.data_dir)
    assert wm == next_date, f"Watermark verify FAIL: wrote {next_date}, read {wm}"
    print(f"Incremental pipeline complete. Watermark advanced to {next_date}.")
    sys.exit(0)


def main():
    config = load_config()
    if config.mode == "historical":
        _run_historical(config)
    else:
        _run_incremental(config)


if __name__ == "__main__":
    main()
