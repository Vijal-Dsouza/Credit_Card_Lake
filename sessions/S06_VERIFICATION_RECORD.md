**Session:** S06 — End-to-End Integration and Phase 8 Preparation
**Date:** 2026-05-04
**Engineer:** Vijal Dsouza

---

## Task 6.1 — Phase 8 Verification Command Suite

**Status:** COMPLETE — all 21 checks PASS

**Execution context:** Docker (pipeline container). Checks run via
`docker compose run --rm pipeline python -c "..."` against bind-mounted `/data/` and `/source/`.

**Pipeline run sequence:**
1. Historical Jan 01–05 (`PIPELINE_MODE=historical`, `END_DATE=2024-01-05`) → watermark = 2024-01-05
2. Incremental Jan 06 (`PIPELINE_MODE=incremental`) → watermark = 2024-01-06
3. Incremental Jan 07 (`PIPELINE_MODE=incremental`) → watermark = 2024-01-07
4. Re-run historical Jan 01–05 (idempotency) → watermark = 2024-01-05
5. Re-run incremental Jan 06 + Jan 07 (idempotency) → watermark = 2024-01-07

**Data state after all runs:**
| Metric | Value |
|---|---|
| bronze_txn | 35 |
| bronze_acc | 20 |
| silver_txn | 28 |
| quarantine | 7 |
| gold_daily | 7 |
| gold_weekly | 3 |
| watermark | 2024-01-07 |

**Note on silver transaction partition naming:** `silver/transactions` uses the
`transaction_date=` partition prefix (not `date=`) and filename `data_0.parquet`
(not `data.parquet`). The S1 conservation check and Task 3.4 verification use
`read_parquet('/data/silver/transactions/*/*.parquet') WHERE CAST(transaction_date AS VARCHAR) = ...`
to handle the actual dbt-produced naming.

**Note on S6 amendment (v1.7):** S6 (INV-15 Account Promotion Conservation) was
revised from per-date `_source_file` methodology to aggregate form. See S06_LOG.md
EXECUTION_PLAN Amendment — v1.7 for full root cause and sign-off.

**Note on pipeline defect fixed in this session:** `run_silver_phase()` did not build
`silver_transaction_codes` on a fresh data directory. Fixed by adding
`_run_dbt_build("silver_transaction_codes", config)` when the file is absent.

---

### Check Results

| Check ID | Category | Command (abbreviated) | Result | Actual Value |
|---|---|---|---|---|
| B1 | Bronze Completeness | COUNT(*) bronze/transactions/*/*.parquet vs SUM 7 source CSVs | **PASS** | bronze_txn=35, source=35 |
| B2 | Bronze Completeness | COUNT(*) bronze/accounts/*/*.parquet vs SUM 7 source CSVs | **PASS** | bronze_acc=20, source=20 |
| B3 | Bronze Completeness | COUNT(*) bronze/transaction_codes/data.parquet vs source | **PASS** | bronze_tc=4, source_tc=4 |
| S1 | Silver Quality | Conservation: bronze_d == silver_d + quarantine_d for all 7 dates | **PASS** | All 7 dates: 5 = silver+quar (4 silver + 1 quar each) |
| S2 | Silver Quality | COUNT(*) vs COUNT(DISTINCT transaction_id) silver/transactions | **PASS** | total=28, distinct=28 |
| S3 | Silver Quality | Invalid transaction codes in silver/transactions | **PASS** | invalid=0 |
| S4 | Silver Quality | NULL _signed_amount in silver/transactions | **PASS** | nulls=0 |
| S5 | Silver Quality | DISTINCT _rejection_reason subset of valid set | **PASS** | reasons=['INVALID_CHANNEL'] |
| S6 | Silver Quality (INV-15) | bronze_distinct_accounts == silver_distinct + quarant_accts | **PASS** | bronze_distinct=3, silver=3, quar=0 |
| G1 | Gold Correctness | COUNT(*) gold/daily_summary vs COUNT(DISTINCT transaction_date) silver | **PASS** | gold_daily=7, silver_distinct_dates=7 |
| G2 | Gold Correctness | total_purchases spot check: ACC-001, week 2024-01-01 | **PASS** | silver=4=gold=4 |
| G3 | Gold Correctness | total_signed_amount spot check for 2024-01-01, 2024-01-02 | **PASS** | Jan-01: gold=-30.0=silver; Jan-02: gold=-170.0=silver |
| G4 | Gold Correctness (INV-16) | struct keys = {PURCHASE,PAYMENT,FEE,INTEREST}; REFUND absent | **PASS** | 7 rows checked; keys={FEE,INTEREST,PAYMENT,PURCHASE} |
| I1 | Idempotency | Bronze counts identical after second historical run + incremental re-runs | **PASS** | bronze_txn=35, bronze_acc=20 unchanged |
| I2 | Idempotency | Silver counts identical after re-run | **PASS** | silver_txn=28 unchanged |
| I3 | Idempotency | Quarantine counts identical after re-run | **PASS** | quarantine=7 unchanged |
| I4 | Idempotency | Gold counts + watermark identical after re-run | **PASS** | gold_daily=7, gold_weekly=3, watermark=2024-01-07 unchanged |
| A1 | Audit Trail | NULL _pipeline_run_id in Bronze (txn, acc, tc) | **PASS** | 0, 0, 0 |
| A2 | Audit Trail | NULL _pipeline_run_id in Silver (txn, acc, tc) | **PASS** | 0, 0, 0 |
| A3 | Audit Trail | NULL _pipeline_run_id in Gold (daily, weekly) | **PASS** | 0, 0 |
| A4 | Audit Trail | Silver _pipeline_run_id NOT IN run_log SUCCESS | **PASS** | untraceable_run_ids=0 |

---

### First-Attempt Failures and Root Cause (for audit completeness)

The first attempt at Task 6.1 (before data directory reset) returned BLOCKED on three checks.
These are documented here for completeness — all three were resolved before the final check run.

**B1/B2 FAIL (first attempt):** bronze_txn=40, bronze_acc=23. Root cause: S5 Task 5.2 TC-3
created stub Jan 08 source files, which loaded bronze partitions `date=2024-01-08`. These
partitions persisted per INV-07 (Bronze Immutability). Resolution: full data reset + historical
Jan 01–05 + incremental Jan 06–07, producing exactly 35 txns and 20 accounts matching 7 source CSVs.

**S6 FAIL (first attempt):** Per-date `_source_file` methodology returned silver_acc=0 for all
dates 01–07. Root cause: `silver_accounts` is a latest-wins model — all accounts carry
`_source_file` of the most recent ingestion date. The per-date check is structurally incompatible
with this architecture. Resolution: EXECUTION_PLAN.md v1.7 amendment — S6 revised to aggregate
form: `COUNT(DISTINCT account_id) FROM bronze == COUNT(*) FROM silver_accounts + quarant_accts`.

---

### Summary Verdict

**COMPLETE** — all 21 Phase 8 checks PASS. Tasks 6.2 and 6.3 complete.
System ready for engineer sign-off per `sessions/S06_LOG.md`.
