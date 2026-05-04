#!/bin/bash
# Credit Card Transactions Lake — Regression Suite
# Portable verification commands from EXECUTION_PLAN.md (v1.7)
# Tasks: 1.3, 1.4, 1.5, 2.1, 2.2, 2.3, 2.4, 3.1, 3.2, 3.3, 3.4, 3.5,
#        4.1, 4.2, 4.3, 5.1, 5.2, 5.3, 5.4, 6.1
#
# Usage (inside pipeline container):
#   docker compose run --rm pipeline bash verification/REGRESSION_SUITE.sh
#
# Each block exits 1 on assertion failure, naming the failed task.
# Expected data state: historical Jan 01–07 + incremental Jan 06 + Jan 07 complete.
# Expected counts: bronze_txn=35, bronze_acc=20, silver_txn=28, quarantine=7,
#                  gold_daily=7, gold_weekly=3, watermark=2024-01-07.

set -uo pipefail

# =============================================================================
# Task 1.3 — Startup Validation Module (INV-08, R3)
# =============================================================================
echo "--- Task 1.3: Startup Validation ---"
PIPELINE_MODE=historical START_DATE=2024-01-01 END_DATE=2024-01-07 \
  DATA_DIR=/data SOURCE_DIR=/source \
  python -c "
from config import load_config
c = load_config()
assert c.mode == 'historical', f'mode={c.mode}'
print('Task 1.3 TC-1 PASS')
" || { echo "FAIL: Task 1.3 TC-1 (valid historical config)"; exit 1; }

python -c "
import os, tempfile, pathlib
os.environ.update({'PIPELINE_MODE':'historical', 'START_DATE':'2024-01-01',
                   'END_DATE':'2024-01-07', 'SOURCE_DIR':'/source'})
with tempfile.TemporaryDirectory() as tmp:
    os.environ['DATA_DIR'] = tmp
    os.makedirs(f'{tmp}/pipeline', exist_ok=True)
    stale = pathlib.Path(f'{tmp}/pipeline/dbt_catalog.duckdb')
    stale.touch()
    import importlib, config as cfg_mod
    importlib.reload(cfg_mod)
    cfg_mod.load_config()
    assert not stale.exists(), 'dbt_catalog.duckdb not deleted at startup'
    print('Task 1.3 TC-6 PASS')
" || { echo "FAIL: Task 1.3 TC-6 (stale catalog deletion — R3)"; exit 1; }

# =============================================================================
# Task 1.4 — Source File Pre-flight (INV-08, INV-13)
# =============================================================================
echo "--- Task 1.4: Source File Pre-flight ---"
PIPELINE_MODE=historical START_DATE=2024-01-01 END_DATE=2024-01-07 \
  DATA_DIR=/data SOURCE_DIR=/source \
  python -c "
from config import load_config
from pipeline import validate_source_files
c = load_config()
validate_source_files(c)
print('Task 1.4 TC-1 PASS')
" || { echo "FAIL: Task 1.4 TC-1 (all source files present)"; exit 1; }

# =============================================================================
# Task 1.5 — Run Log and Control Table Helpers (INV-06)
# =============================================================================
echo "--- Task 1.5: Run Log and Control Table ---"
python -c "
import os, tempfile, datetime, duckdb
from lake_io import read_watermark, write_watermark, append_run_log

with tempfile.TemporaryDirectory() as tmp:
    os.makedirs(f'{tmp}/pipeline')
    d = datetime.date(2024, 1, 7)
    write_watermark(tmp, d, 'run-001')
    result = read_watermark(tmp)
    assert result == d, f'TC-1 FAIL: expected {d}, got {result}'
    print('Task 1.5 TC-1 PASS')

    row = dict(run_id='r1', pipeline_type='HISTORICAL', model_name='test',
               layer='BRONZE', started_at=datetime.datetime.utcnow(),
               completed_at=datetime.datetime.utcnow(), status='SUCCESS',
               records_processed=10, records_written=10,
               records_rejected=None, error_message=None)
    append_run_log(tmp, row)
    append_run_log(tmp, {**row, 'run_id': 'r2'})
    append_run_log(tmp, {**row, 'run_id': 'r3'})
    count = duckdb.execute(f\"SELECT COUNT(*) FROM '{tmp}/pipeline/run_log.parquet'\").fetchone()[0]
    assert count == 3, f'TC-3/TC-5 FAIL: expected 3 rows, got {count}'
    print('Task 1.5 TC-3/TC-5 PASS')
" || { echo "FAIL: Task 1.5 (watermark/run_log helpers — INV-06)"; exit 1; }

# =============================================================================
# Task 2.1 — Bronze Transaction Codes Loader (INV-05, INV-07, INV-13)
# =============================================================================
echo "--- Task 2.1: Bronze Transaction Codes Loader ---"
python -c "
import duckdb, os, tempfile, datetime
from config import PipelineConfig
from bronze_loaders import load_bronze_transaction_codes

with tempfile.TemporaryDirectory() as tmp:
    os.makedirs(f'{tmp}/bronze/transaction_codes')
    os.makedirs(f'{tmp}/pipeline')
    cfg = PipelineConfig(mode='historical', data_dir=tmp, source_dir='/source',
                         start_date=datetime.date(2024,1,1), end_date=datetime.date(2024,1,7))
    result = load_bronze_transaction_codes(cfg, 'run-test-001')
    assert result['status'] == 'SUCCESS', f'TC-1 FAIL: {result}'
    pq = f'{tmp}/bronze/transaction_codes/data.parquet'
    count = duckdb.execute(f\"SELECT COUNT(*) FROM '{pq}'\").fetchone()[0]
    assert count == result['records_written'], f'TC-5 F2 FAIL: {count} != {result[\"records_written\"]}'
    nulls = duckdb.execute(f\"SELECT COUNT(*) FROM '{pq}' WHERE _pipeline_run_id IS NULL\").fetchone()[0]
    assert nulls == 0, f'TC-3 FAIL: {nulls} null run_ids'
    r2 = load_bronze_transaction_codes(cfg, 'run-test-002')
    assert r2['status'] == 'SKIPPED', f'TC-2 FAIL: expected SKIPPED, got {r2[\"status\"]}'
    print('Task 2.1 PASS')
" || { echo "FAIL: Task 2.1 (bronze transaction codes loader)"; exit 1; }

# =============================================================================
# Task 2.2 — Bronze Accounts Loader (INV-05, INV-07)
# =============================================================================
echo "--- Task 2.2: Bronze Accounts Loader ---"
python -c "
import duckdb, os, tempfile, datetime
from config import PipelineConfig
from bronze_loaders import load_bronze_accounts

with tempfile.TemporaryDirectory() as tmp:
    os.makedirs(f'{tmp}/bronze/accounts')
    os.makedirs(f'{tmp}/pipeline')
    cfg = PipelineConfig(mode='historical', data_dir=tmp, source_dir='/source',
                         start_date=datetime.date(2024,1,1), end_date=datetime.date(2024,1,7))
    d = datetime.date(2024, 1, 1)
    r = load_bronze_accounts(cfg, d, 'run-001')
    assert r['status'] == 'SUCCESS', f'TC-1 FAIL: {r}'
    part = f'{tmp}/bronze/accounts/date=2024-01-01/data.parquet'
    assert os.path.exists(part), 'TC-1 FAIL: partition not created'
    count = duckdb.execute(f\"SELECT COUNT(*) FROM '{part}'\").fetchone()[0]
    assert count == r['records_written'], f'TC-5 F2 FAIL: {count} != {r[\"records_written\"]}'
    nulls = duckdb.execute(f\"SELECT COUNT(*) FROM '{part}' WHERE _pipeline_run_id IS NULL\").fetchone()[0]
    assert nulls == 0, f'TC-3 FAIL: {nulls} null run_ids'
    r2 = load_bronze_accounts(cfg, d, 'run-002')
    assert r2['status'] == 'SKIPPED', f'TC-2 FAIL: expected SKIPPED, got {r2[\"status\"]}'
    print('Task 2.2 PASS')
" || { echo "FAIL: Task 2.2 (bronze accounts loader)"; exit 1; }

# =============================================================================
# Task 2.3 — Bronze Transactions Loader (INV-05, INV-07)
# =============================================================================
echo "--- Task 2.3: Bronze Transactions Loader ---"
python -c "
import duckdb, os, tempfile, datetime
from config import PipelineConfig
from bronze_loaders import load_bronze_transactions

with tempfile.TemporaryDirectory() as tmp:
    os.makedirs(f'{tmp}/bronze/transactions')
    os.makedirs(f'{tmp}/pipeline')
    cfg = PipelineConfig(mode='historical', data_dir=tmp, source_dir='/source',
                         start_date=datetime.date(2024,1,1), end_date=datetime.date(2024,1,7))
    for i in range(7):
        d = datetime.date(2024, 1, 1) + datetime.timedelta(days=i)
        r = load_bronze_transactions(cfg, d, f'run-00{i+1}')
        assert r['status'] in ('SUCCESS', 'WARNING'), f'Day {d} FAIL: {r}'
        part = f'{tmp}/bronze/transactions/date={d}/data.parquet'
        count = duckdb.execute(f\"SELECT COUNT(*) FROM '{part}'\").fetchone()[0]
        assert count == r['records_written'], f'TC-5 F2 FAIL day {d}'
    parts = [p for p in os.listdir(f'{tmp}/bronze/transactions') if p.startswith('date=')]
    assert len(parts) == 7, f'TC-4 FAIL: {len(parts)} partitions'
    r2 = load_bronze_transactions(cfg, datetime.date(2024,1,1), 'run-rerun')
    assert r2['status'] == 'SKIPPED', f'TC-2 FAIL: expected SKIPPED, got {r2[\"status\"]}'
    print('Task 2.3 PASS')
" || { echo "FAIL: Task 2.3 (bronze transactions loader)"; exit 1; }

# =============================================================================
# Task 2.4 — Bronze Phase Function (INV-06, INV-08)
# =============================================================================
echo "--- Task 2.4: Bronze Phase Function ---"
python -c "
import duckdb
count  = duckdb.execute(\"SELECT COUNT(*) FROM '/data/pipeline/run_log.parquet'\").fetchone()[0]
failed = duckdb.execute(\"SELECT COUNT(*) FROM '/data/pipeline/run_log.parquet' WHERE status='FAILED'\").fetchone()[0]
assert count > 0, 'run log empty — bronze phase not run'
assert failed == 0, f'{failed} FAILED entries in run log'
print(f'Task 2.4 PASS: run_log={count} entries, failed={failed}')
" || { echo "FAIL: Task 2.4 (bronze phase function — run log state)"; exit 1; }

# =============================================================================
# Task 3.1 — Silver Transaction Codes Model (INV-05, INV-11)
# =============================================================================
echo "--- Task 3.1: Silver Transaction Codes ---"
(cd dbt_project && dbt build --select silver_transaction_codes --quiet) \
  || { echo "FAIL: Task 3.1 dbt build silver_transaction_codes"; exit 1; }
python -c "
import duckdb
silver = duckdb.execute(\"SELECT COUNT(*) FROM '/data/silver/transaction_codes/data.parquet'\").fetchone()[0]
bronze = duckdb.execute(\"SELECT COUNT(*) FROM '/data/bronze/transaction_codes/data.parquet'\").fetchone()[0]
assert silver == bronze, f'TC-2 FAIL: silver={silver} bronze={bronze}'
nulls = duckdb.execute(\"SELECT COUNT(*) FROM '/data/silver/transaction_codes/data.parquet' WHERE _source_file IS NULL\").fetchone()[0]
assert nulls == 0, f'TC-4 FAIL: {nulls} null _source_file'
print(f'Task 3.1 PASS: silver_tc={silver}')
" || { echo "FAIL: Task 3.1 assertions"; exit 1; }

# =============================================================================
# Task 3.2 — Silver Accounts Model (INV-05, INV-10, INV-15)
# =============================================================================
echo "--- Task 3.2: Silver Accounts ---"
(cd dbt_project && dbt build --select silver_accounts --quiet) \
  || { echo "FAIL: Task 3.2 dbt build silver_accounts"; exit 1; }
python -c "
import duckdb
total     = duckdb.execute(\"SELECT COUNT(*) FROM '/data/silver/accounts/data.parquet'\").fetchone()[0]
unique_id = duckdb.execute(\"SELECT COUNT(DISTINCT account_id) FROM '/data/silver/accounts/data.parquet'\").fetchone()[0]
assert total == unique_id, f'TC-2 FAIL: {total} rows, {unique_id} distinct account_ids'
null_rvf  = duckdb.execute(\"SELECT COUNT(*) FROM '/data/silver/accounts/data.parquet' WHERE _record_valid_from IS NULL\").fetchone()[0]
assert null_rvf == 0, f'TC-6 FAIL: {null_rvf} null _record_valid_from'
print(f'Task 3.2 PASS: silver_accounts={total}')
" || { echo "FAIL: Task 3.2 assertions"; exit 1; }

# =============================================================================
# Task 3.3 — Silver Quarantine Model (INV-03, INV-04)
# =============================================================================
echo "--- Task 3.3: Silver Quarantine ---"
(cd dbt_project && dbt build --select silver_quarantine --quiet) \
  || { echo "FAIL: Task 3.3 dbt build silver_quarantine"; exit 1; }
python -c "
import duckdb
valid = {'NULL_REQUIRED_FIELD','INVALID_AMOUNT','DUPLICATE_TRANSACTION_ID',
         'INVALID_TRANSACTION_CODE','INVALID_CHANNEL','INVALID_ACCOUNT_STATUS'}
rows   = duckdb.execute(\"SELECT DISTINCT _rejection_reason FROM read_parquet('/data/silver/quarantine/*/*.parquet')\").fetchall()
actual = {r[0] for r in rows}
bad    = actual - valid
assert not bad, f'TC-6 FAIL: unexpected rejection codes {bad}'
print(f'Task 3.3 PASS: quarantine reasons={actual}')
" || { echo "FAIL: Task 3.3 assertions"; exit 1; }

# =============================================================================
# Task 3.4 — Silver Transactions Model (INV-01, INV-02, INV-04)
# =============================================================================
echo "--- Task 3.4: Silver Transactions ---"
(cd dbt_project && dbt build --select silver_transactions --quiet) \
  || { echo "FAIL: Task 3.4 dbt build silver_transactions"; exit 1; }
python -c "
import duckdb
# TC-1: conservation per date (glob form — handles transaction_date= partition naming)
for d in ['2024-01-01','2024-01-02','2024-01-03','2024-01-04','2024-01-05','2024-01-06','2024-01-07']:
    br  = duckdb.execute(f\"SELECT COUNT(*) FROM '/data/bronze/transactions/date={d}/data.parquet'\").fetchone()[0]
    try:
        sv = duckdb.execute(f\"SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/*/*.parquet') WHERE CAST(transaction_date AS VARCHAR) = '{d}'\").fetchone()[0]
    except:
        sv = 0
    try:
        qu = duckdb.execute(f\"SELECT COUNT(*) FROM read_parquet('/data/silver/quarantine/*/*.parquet') WHERE _source_file = 'transactions_{d}.csv'\").fetchone()[0]
    except:
        qu = 0
    assert br == sv + qu, f'TC-1 FAIL {d}: bronze={br} silver={sv} quar={qu}'
# TC-2: no duplicate transaction_id
total    = duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/*/*.parquet')\").fetchone()[0]
distinct = duckdb.execute(\"SELECT COUNT(DISTINCT transaction_id) FROM read_parquet('/data/silver/transactions/*/*.parquet')\").fetchone()[0]
assert total == distinct, f'TC-2 FAIL: {total} rows, {distinct} distinct ids'
print(f'Task 3.4 PASS: silver_txn={total}, conservation holds for all 7 dates')
" || { echo "FAIL: Task 3.4 assertions"; exit 1; }

# =============================================================================
# Task 3.5 — Silver Phase Function (INV-14, F-NEW-2)
# =============================================================================
echo "--- Task 3.5: Silver Phase Function ---"
# TC-6 (F-NEW-2): dbt build used, not dbt run
grep -q 'dbt build' /app/pipeline.py \
  || { echo "FAIL: Task 3.5 TC-6 — 'dbt build' not found in pipeline.py"; exit 1; }
python -c "
import duckdb
# INV-14: silver_transaction_codes must exist (pre-check gate active)
count = duckdb.execute(\"SELECT COUNT(*) FROM '/data/silver/transaction_codes/data.parquet'\").fetchone()[0]
assert count > 0, f'INV-14 FAIL: silver_transaction_codes empty ({count} rows)'
print(f'Task 3.5 PASS: silver_tc={count}, dbt build confirmed in pipeline.py')
" || { echo "FAIL: Task 3.5 assertions"; exit 1; }

# =============================================================================
# Task 4.1 — Gold Daily Summary Model (INV-04, INV-12, INV-16)
# =============================================================================
echo "--- Task 4.1: Gold Daily Summary ---"
(cd dbt_project && dbt build --select gold_daily_summary --quiet) \
  || { echo "FAIL: Task 4.1 dbt build gold_daily_summary"; exit 1; }
python -c "
import duckdb
total    = duckdb.execute(\"SELECT COUNT(*) FROM '/data/gold/daily_summary/data.parquet'\").fetchone()[0]
distinct = duckdb.execute(\"SELECT COUNT(DISTINCT transaction_date) FROM '/data/gold/daily_summary/data.parquet'\").fetchone()[0]
assert total == distinct, f'TC-1 FAIL: {total} rows, {distinct} distinct dates'
rows = duckdb.execute(\"SELECT transactions_by_type FROM '/data/gold/daily_summary/data.parquet'\").fetchall()
expected = {'PURCHASE', 'PAYMENT', 'FEE', 'INTEREST'}
for row in rows:
    struct = row[0]
    actual = set(struct.keys()) if hasattr(struct, 'keys') else set(struct._fields)
    assert actual == expected, f'TC-5 FAIL (INV-16): struct keys {actual}'
    assert 'REFUND' not in actual, 'TC-5 FAIL (INV-16): REFUND in struct'
print(f'Task 4.1 PASS: gold_daily={total}, struct shape correct on all rows')
" || { echo "FAIL: Task 4.1 assertions"; exit 1; }

# =============================================================================
# Task 4.2 — Gold Weekly Account Summary Model (INV-04, INV-12)
# =============================================================================
echo "--- Task 4.2: Gold Weekly Account Summary ---"
(cd dbt_project && dbt build --select gold_weekly_account_summary --quiet) \
  || { echo "FAIL: Task 4.2 dbt build gold_weekly_account_summary"; exit 1; }
python -c "
import duckdb
total    = duckdb.execute(\"SELECT COUNT(*) FROM '/data/gold/weekly_account_summary/data.parquet'\").fetchone()[0]
distinct = duckdb.execute(\"SELECT COUNT(*) FROM (SELECT DISTINCT week_start_date, account_id FROM '/data/gold/weekly_account_summary/data.parquet')\").fetchone()[0]
assert total == distinct, f'TC-1 FAIL: {total} rows, {distinct} distinct composite keys'
bad = duckdb.execute(\"SELECT COUNT(*) FROM '/data/gold/weekly_account_summary/data.parquet' WHERE week_end_date != week_start_date + INTERVAL 6 DAYS\").fetchone()[0]
assert bad == 0, f'TC-4 FAIL: {bad} rows with wrong week_end_date'
print(f'Task 4.2 PASS: gold_weekly={total}')
" || { echo "FAIL: Task 4.2 assertions"; exit 1; }

# =============================================================================
# Task 4.3 — Gold Phase Function (INV-08, INV-12, F-NEW-2)
# =============================================================================
echo "--- Task 4.3: Gold Phase Function ---"
python -c "
import duckdb
daily  = duckdb.execute(\"SELECT COUNT(*) FROM '/data/gold/daily_summary/data.parquet'\").fetchone()[0]
weekly = duckdb.execute(\"SELECT COUNT(*) FROM '/data/gold/weekly_account_summary/data.parquet'\").fetchone()[0]
assert daily  > 0, f'TC-1 FAIL: gold_daily empty'
assert weekly > 0, f'TC-1 FAIL: gold_weekly empty'
print(f'Task 4.3 PASS: gold_daily={daily}, gold_weekly={weekly}')
" || { echo "FAIL: Task 4.3 (gold phase output present)"; exit 1; }

# =============================================================================
# Task 5.1 — Historical Pipeline Orchestrator (INV-09)
# =============================================================================
echo "--- Task 5.1: Historical Pipeline Watermark ---"
python -c "
from lake_io import read_watermark
import datetime
wm = read_watermark('/data')
assert wm is not None, 'TC-1 FAIL: watermark is None — historical pipeline not run'
assert wm >= datetime.date(2024, 1, 5), f'TC-1 FAIL: watermark={wm} (expected >= 2024-01-05)'
print(f'Task 5.1 PASS: watermark={wm}')
" || { echo "FAIL: Task 5.1 (watermark present — INV-09)"; exit 1; }

# =============================================================================
# Task 5.2 — Incremental Pipeline Orchestrator (INV-09, F5)
# =============================================================================
echo "--- Task 5.2: Incremental Pre-check (F5) ---"
# F5 pre-check: silver_accounts must be non-empty before incremental run
python -c "
import duckdb
rows = duckdb.execute(\"SELECT COUNT(*) FROM '/data/silver/accounts/data.parquet'\").fetchone()[0]
assert rows > 0, f'F5 FAIL: silver_accounts is empty ({rows} rows) — incremental pre-check would block'
print(f'Task 5.2 PASS: silver_accounts={rows} rows (F5 pre-check would pass)')
" || { echo "FAIL: Task 5.2 (F5 silver_accounts pre-check)"; exit 1; }

# =============================================================================
# Task 5.3 — Idempotency Hardening (INV-10, INV-06, INV-09)
# =============================================================================
echo "--- Task 5.3: Idempotency State Check ---"
python -c "
import duckdb, datetime
from lake_io import read_watermark

bronze_txn  = duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/bronze/transactions/*/*.parquet')\").fetchone()[0]
bronze_acc  = duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/bronze/accounts/*/*.parquet')\").fetchone()[0]
silver_txn  = duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/*/*.parquet')\").fetchone()[0]
quarantine  = duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/silver/quarantine/*/*.parquet')\").fetchone()[0]
gold_daily  = duckdb.execute(\"SELECT COUNT(*) FROM '/data/gold/daily_summary/data.parquet'\").fetchone()[0]
gold_weekly = duckdb.execute(\"SELECT COUNT(*) FROM '/data/gold/weekly_account_summary/data.parquet'\").fetchone()[0]
run_log     = duckdb.execute(\"SELECT COUNT(*) FROM '/data/pipeline/run_log.parquet'\").fetchone()[0]
wm = read_watermark('/data')

assert bronze_txn  == 35, f'INV-10 FAIL: bronze_txn={bronze_txn} (expected 35)'
assert bronze_acc  == 20, f'INV-10 FAIL: bronze_acc={bronze_acc} (expected 20)'
assert silver_txn  == 28, f'INV-10 FAIL: silver_txn={silver_txn} (expected 28)'
assert quarantine  ==  7, f'INV-10 FAIL: quarantine={quarantine} (expected 7)'
assert gold_daily  ==  7, f'INV-10 FAIL: gold_daily={gold_daily} (expected 7)'
assert gold_weekly ==  3, f'INV-10 FAIL: gold_weekly={gold_weekly} (expected 3)'
assert run_log     >   0, f'INV-06 FAIL: run_log is empty'
assert wm == datetime.date(2024, 1, 7), f'INV-09 FAIL: watermark={wm} (expected 2024-01-07)'

print(f'Task 5.3 PASS: bronze={bronze_txn}/{bronze_acc} silver={silver_txn} quar={quarantine} gold={gold_daily}/{gold_weekly} wm={wm}')
" || { echo "FAIL: Task 5.3 (idempotency counts — INV-10/INV-06/INV-09)"; exit 1; }

# =============================================================================
# Task 5.4 — Audit Trail Verification (INV-05)
# =============================================================================
echo "--- Task 5.4: Audit Trail ---"
python -c "
import duckdb
checks = [
    (\"SELECT COUNT(*) FROM read_parquet('/data/bronze/transactions/*/*.parquet') WHERE _pipeline_run_id IS NULL\",   'A1 Bronze txn'),
    (\"SELECT COUNT(*) FROM read_parquet('/data/bronze/accounts/*/*.parquet') WHERE _pipeline_run_id IS NULL\",       'A1 Bronze acc'),
    (\"SELECT COUNT(*) FROM '/data/bronze/transaction_codes/data.parquet' WHERE _pipeline_run_id IS NULL\",           'A1 Bronze tc'),
    (\"SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/*/*.parquet') WHERE _pipeline_run_id IS NULL\",   'A2 Silver txn'),
    (\"SELECT COUNT(*) FROM '/data/silver/accounts/data.parquet' WHERE _pipeline_run_id IS NULL\",                   'A2 Silver acc'),
    (\"SELECT COUNT(*) FROM '/data/silver/transaction_codes/data.parquet' WHERE _pipeline_run_id IS NULL\",          'A2 Silver tc'),
    (\"SELECT COUNT(*) FROM '/data/gold/daily_summary/data.parquet' WHERE _pipeline_run_id IS NULL\",                'A3 Gold daily'),
    (\"SELECT COUNT(*) FROM '/data/gold/weekly_account_summary/data.parquet' WHERE _pipeline_run_id IS NULL\",       'A3 Gold weekly'),
]
for q, label in checks:
    n = duckdb.execute(q).fetchone()[0]
    assert n == 0, f'{label} FAIL: {n} null _pipeline_run_id rows'
untraceable = duckdb.execute(\"\"\"
    SELECT COUNT(DISTINCT _pipeline_run_id)
    FROM read_parquet('/data/silver/transactions/*/*.parquet')
    WHERE _pipeline_run_id NOT IN (
        SELECT run_id FROM '/data/pipeline/run_log.parquet' WHERE status = 'SUCCESS'
    )
\"\"\").fetchone()[0]
assert untraceable == 0, f'A4 FAIL: {untraceable} untraceable Silver run_ids'
print('Task 5.4 PASS: all audit checks PASS (INV-05)')
" || { echo "FAIL: Task 5.4 (audit trail — INV-05)"; exit 1; }

# =============================================================================
# Task 6.1 — Phase 8 Verification Suite (INV-01 through INV-16)
# =============================================================================
echo "--- Task 6.1: Phase 8 Verification Suite ---"
python -c "
import duckdb

# B1: Bronze transactions completeness
bronze_txn = duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/bronze/transactions/*/*.parquet')\").fetchone()[0]
source_txn = sum(duckdb.execute(f\"SELECT COUNT(*) FROM read_csv_auto('/source/transactions_2024-01-0{i}.csv')\").fetchone()[0] for i in range(1,8))
assert bronze_txn == source_txn, f'B1 FAIL: bronze={bronze_txn} source={source_txn}'
print(f'B1 PASS: bronze_txn={bronze_txn}')

# B2: Bronze accounts completeness
bronze_acc = duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/bronze/accounts/*/*.parquet')\").fetchone()[0]
source_acc = sum(duckdb.execute(f\"SELECT COUNT(*) FROM read_csv_auto('/source/accounts_2024-01-0{i}.csv')\").fetchone()[0] for i in range(1,8))
assert bronze_acc == source_acc, f'B2 FAIL: bronze={bronze_acc} source={source_acc}'
print(f'B2 PASS: bronze_acc={bronze_acc}')

# B3: Bronze transaction codes completeness
bronze_tc = duckdb.execute(\"SELECT COUNT(*) FROM '/data/bronze/transaction_codes/data.parquet'\").fetchone()[0]
source_tc  = duckdb.execute(\"SELECT COUNT(*) FROM read_csv_auto('/source/transaction_codes.csv')\").fetchone()[0]
assert bronze_tc == source_tc, f'B3 FAIL: bronze={bronze_tc} source={source_tc}'
print(f'B3 PASS: bronze_tc={bronze_tc}')

# S1: Conservation per date (uses glob form for silver — transaction_date= partition naming)
for d in ['2024-01-01','2024-01-02','2024-01-03','2024-01-04','2024-01-05','2024-01-06','2024-01-07']:
    br  = duckdb.execute(f\"SELECT COUNT(*) FROM '/data/bronze/transactions/date={d}/data.parquet'\").fetchone()[0]
    try:
        sv = duckdb.execute(f\"SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/*/*.parquet') WHERE CAST(transaction_date AS VARCHAR) = '{d}'\").fetchone()[0]
    except:
        sv = 0
    try:
        qu = duckdb.execute(f\"SELECT COUNT(*) FROM read_parquet('/data/silver/quarantine/*/*.parquet') WHERE _source_file = 'transactions_{d}.csv'\").fetchone()[0]
    except:
        qu = 0
    assert br == sv + qu, f'S1 FAIL {d}: bronze={br} silver={sv} quar={qu}'
print('S1 PASS')

# S2: No duplicate transaction_id in Silver
total    = duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/*/*.parquet')\").fetchone()[0]
distinct = duckdb.execute(\"SELECT COUNT(DISTINCT transaction_id) FROM read_parquet('/data/silver/transactions/*/*.parquet')\").fetchone()[0]
assert total == distinct, f'S2 FAIL: {total} rows, {distinct} distinct'
print(f'S2 PASS: silver_txn={total}')

# S3: No invalid transaction codes
inv = duckdb.execute(\"\"\"
    SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/*/*.parquet') st
    WHERE st.transaction_code NOT IN (
        SELECT transaction_code FROM '/data/silver/transaction_codes/data.parquet'
    )
\"\"\").fetchone()[0]
assert inv == 0, f'S3 FAIL: {inv} invalid codes'
print('S3 PASS')

# S4: No null _signed_amount
nulls = duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/*/*.parquet') WHERE _signed_amount IS NULL\").fetchone()[0]
assert nulls == 0, f'S4 FAIL: {nulls} null _signed_amount'
print('S4 PASS')

# S5: Quarantine rejection codes in valid set
valid_codes = {'NULL_REQUIRED_FIELD','INVALID_AMOUNT','DUPLICATE_TRANSACTION_ID',
               'INVALID_TRANSACTION_CODE','INVALID_CHANNEL','INVALID_ACCOUNT_STATUS'}
rows   = duckdb.execute(\"SELECT DISTINCT _rejection_reason FROM read_parquet('/data/silver/quarantine/*/*.parquet')\").fetchall()
actual = {r[0] for r in rows}
bad    = actual - valid_codes
assert not bad, f'S5 FAIL: unexpected codes {bad}'
print(f'S5 PASS: reasons={actual}')

# S6: INV-15 aggregate account conservation
bronze_da = duckdb.execute(\"SELECT COUNT(DISTINCT account_id) FROM read_parquet('/data/bronze/accounts/*/*.parquet')\").fetchone()[0]
silver_da = duckdb.execute(\"SELECT COUNT(*) FROM '/data/silver/accounts/data.parquet'\").fetchone()[0]
try:
    quar_acc = duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/silver/quarantine/*/*.parquet') WHERE _source_file LIKE 'accounts_%'\").fetchone()[0]
except:
    quar_acc = 0
assert bronze_da == silver_da + quar_acc, f'S6 FAIL: bronze_distinct={bronze_da} != silver({silver_da}) + quar({quar_acc})'
print(f'S6 PASS: bronze_distinct={bronze_da}, silver={silver_da}, quar={quar_acc}')

# G1: Gold daily — one row per resolvable date
gd = duckdb.execute(\"SELECT COUNT(*) FROM '/data/gold/daily_summary/data.parquet'\").fetchone()[0]
sd = duckdb.execute(\"SELECT COUNT(DISTINCT transaction_date) FROM read_parquet('/data/silver/transactions/*/*.parquet') WHERE _is_resolvable = true\").fetchone()[0]
assert gd == sd, f'G1 FAIL: gold={gd} silver={sd}'
print(f'G1 PASS: gold_daily={gd}')

# G2: Weekly total_purchases spot check
r = duckdb.execute(\"SELECT week_start_date, account_id, total_purchases FROM '/data/gold/weekly_account_summary/data.parquet' LIMIT 1\").fetchone()
wsd, acc, gold_tp = r
sc = duckdb.execute(f\"\"\"
    SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/*/*.parquet')
    WHERE account_id = '{acc}' AND DATE_TRUNC('week', transaction_date) = '{wsd}'
      AND transaction_type = 'PURCHASE' AND _is_resolvable = true
\"\"\").fetchone()[0]
assert sc == gold_tp, f'G2 FAIL: silver_count={sc} gold_total_purchases={gold_tp}'
print(f'G2 PASS: week={wsd}, account={acc}, silver={sc}=gold={gold_tp}')

# G3: total_signed_amount spot check for two dates
rows2 = duckdb.execute(\"SELECT transaction_date, total_signed_amount FROM '/data/gold/daily_summary/data.parquet' LIMIT 2\").fetchall()
for d, gold_sum in rows2:
    silver_sum = duckdb.execute(f\"SELECT SUM(_signed_amount) FROM read_parquet('/data/silver/transactions/*/*.parquet') WHERE transaction_date = '{d}' AND _is_resolvable = true\").fetchone()[0]
    assert abs(float(gold_sum) - float(silver_sum)) < 0.001, f'G3 FAIL for {d}: gold={gold_sum} silver={silver_sum}'
print('G3 PASS')

# G4: INV-16 struct shape integrity
rows_g4 = duckdb.execute(\"SELECT transactions_by_type FROM '/data/gold/daily_summary/data.parquet'\").fetchall()
expected = {'PURCHASE', 'PAYMENT', 'FEE', 'INTEREST'}
for row in rows_g4:
    struct = row[0]
    actual = set(struct.keys()) if hasattr(struct, 'keys') else set(struct._fields)
    assert actual == expected, f'G4 FAIL: keys={actual}'
    assert 'REFUND' not in actual, 'G4 FAIL: REFUND in struct'
print(f'G4 PASS: struct shape correct on all {len(rows_g4)} rows')

# I1-I4: Idempotency (compare against Session 6 verified values)
assert bronze_txn == 35, f'I1 FAIL: bronze_txn={bronze_txn}'
assert total      == 28, f'I2 FAIL: silver_txn={total}'
quarantine = duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/silver/quarantine/*/*.parquet')\").fetchone()[0]
assert quarantine ==  7, f'I3 FAIL: quarantine={quarantine}'
assert gd         ==  7, f'I4 FAIL: gold_daily={gd}'
gold_weekly = duckdb.execute(\"SELECT COUNT(*) FROM '/data/gold/weekly_account_summary/data.parquet'\").fetchone()[0]
assert gold_weekly == 3, f'I4 FAIL: gold_weekly={gold_weekly}'
from lake_io import read_watermark; import datetime
wm = read_watermark('/data')
assert wm == datetime.date(2024, 1, 7), f'I4 FAIL: watermark={wm}'
print(f'I1-I4 PASS')

# A1-A4: Audit trail
for label, q in [
    ('A1 Bronze txn',  \"SELECT COUNT(*) FROM read_parquet('/data/bronze/transactions/*/*.parquet') WHERE _pipeline_run_id IS NULL\"),
    ('A1 Bronze acc',  \"SELECT COUNT(*) FROM read_parquet('/data/bronze/accounts/*/*.parquet') WHERE _pipeline_run_id IS NULL\"),
    ('A1 Bronze tc',   \"SELECT COUNT(*) FROM '/data/bronze/transaction_codes/data.parquet' WHERE _pipeline_run_id IS NULL\"),
    ('A2 Silver txn',  \"SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/*/*.parquet') WHERE _pipeline_run_id IS NULL\"),
    ('A2 Silver acc',  \"SELECT COUNT(*) FROM '/data/silver/accounts/data.parquet' WHERE _pipeline_run_id IS NULL\"),
    ('A2 Silver tc',   \"SELECT COUNT(*) FROM '/data/silver/transaction_codes/data.parquet' WHERE _pipeline_run_id IS NULL\"),
    ('A3 Gold daily',  \"SELECT COUNT(*) FROM '/data/gold/daily_summary/data.parquet' WHERE _pipeline_run_id IS NULL\"),
    ('A3 Gold weekly', \"SELECT COUNT(*) FROM '/data/gold/weekly_account_summary/data.parquet' WHERE _pipeline_run_id IS NULL\"),
]:
    n = duckdb.execute(q).fetchone()[0]
    assert n == 0, f'{label} FAIL: {n} null _pipeline_run_id'
untraceable = duckdb.execute(\"\"\"
    SELECT COUNT(DISTINCT _pipeline_run_id)
    FROM read_parquet('/data/silver/transactions/*/*.parquet')
    WHERE _pipeline_run_id NOT IN (
        SELECT run_id FROM '/data/pipeline/run_log.parquet' WHERE status = 'SUCCESS'
    )
\"\"\").fetchone()[0]
assert untraceable == 0, f'A4 FAIL: {untraceable} untraceable run_ids'
print('A1-A4 PASS')
print('Task 6.1 PASS — all 21 Phase 8 checks PASS')
" || { echo "FAIL: Task 6.1 (Phase 8 verification suite)"; exit 1; }

# =============================================================================
# NON-PORTABLE COMMANDS — listed here, not executed
# =============================================================================
# The following verification commands from EXECUTION_PLAN.md are excluded because
# they modify system state or require external context not available inside the container.
#
# Task 1.4 TC-2 (missing source file test):
#   docker compose run --rm pipeline bash -c \
#     "mv /source/transactions_2024-01-03.csv /tmp/ && python pipeline.py; \
#      mv /tmp/transactions_2024-01-03.csv /source/"
#   Reason: moves a source file — breaks data state if interrupted.
#
# Task 2.4 full pipeline run (requires clean data directory):
#   docker compose run --rm pipeline python pipeline.py
#   Reason: changes watermark and run_log; not safe as an inline regression step.
#   Idempotency is verified by Task 5.3 block above against fixed expected counts.
#
# Task 5.1 full pipeline execution:
#   docker compose run --rm pipeline python pipeline.py && echo "Exit 0 PASS"
#   Reason: covered by Task 5.3 state inspection above.
#
# Task 5.2 TC-2 (mv control.parquet to test no-watermark path):
#   docker compose run --rm pipeline bash -c \
#     "mv /data/pipeline/control.parquet /tmp/ctrl_backup.parquet; \
#      PIPELINE_MODE=incremental python pipeline.py; echo exit:$?; \
#      mv /tmp/ctrl_backup.parquet /data/pipeline/control.parquet"
#   Reason: moves control.parquet — breaks watermark state if interrupted.
#
# Task 5.2 TC-4 (mv silver_accounts to test F5 pre-check):
#   docker compose run --rm pipeline bash -c \
#     "mv /data/silver/accounts/data.parquet /tmp/acc_backup.parquet; \
#      PIPELINE_MODE=incremental python pipeline.py; echo exit:$?; \
#      mv /tmp/acc_backup.parquet /data/silver/accounts/data.parquet"
#   Reason: moves silver_accounts — corrupts Gold aggregations if interrupted.
#
# Task 5.2 TC-3 (incremental run with 8th source file):
#   Reason: seed data covers Jan 01–07 only; no Jan 08 source files in the repository.
#
# Task 3.5 TC-1 (run_silver_phase() import test):
#   docker compose run --rm pipeline python -c \
#     "from config import load_config; from pipeline import run_silver_phase; ..."
#   Reason: runs full Silver dbt builds; covered by Tasks 3.1-3.4 dbt build blocks above.
#
# Task 4.3 TC-1 (run_gold_phase() import test):
#   Reason: runs full Gold dbt builds; covered by Tasks 4.1-4.2 dbt build blocks above.

echo ""
echo "REGRESSION SUITE PASS"
