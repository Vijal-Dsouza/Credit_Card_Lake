#!/usr/bin/env bash
# HARNESS.sh — Live Invariant Assertion Harness
# Credit Card Transactions Lake — INV-05: Audit Chain Continuity
#
# Source: Task 5.4 HARNESS-CANDIDATE assertions (EXECUTION_PLAN.md v1.7)
#
# Usage:
#   DATA_DIR=/data bash verification/HARNESS.sh
#
# Requirements: DuckDB CLI binary (duckdb) + read access to DATA_DIR Parquet files.
# No pipeline code, Python, Docker, or build context required.
# DATA_DIR defaults to /data (docker bind-mount path); override for host runs:
#   DATA_DIR=./data bash verification/HARNESS.sh

set -uo pipefail

DATA_DIR="${DATA_DIR:-/data}"
PASS=0
FAIL=0
CRITICAL_FAIL=0
WARNING_FAIL=0

run_assertion() {
  local inv_id="$1"
  local severity="$2"
  local command="$3"
  if eval "$command" > /tmp/harness_out 2>&1; then
    echo "PASS | $inv_id | $severity | $command"
    PASS=$((PASS + 1))
  else
    local output
    output=$(cat /tmp/harness_out)
    echo "FAIL | $inv_id | $severity | $command | $output"
    FAIL=$((FAIL + 1))
    [ "$severity" = "CRITICAL" ] && CRITICAL_FAIL=$((CRITICAL_FAIL + 1)) || WARNING_FAIL=$((WARNING_FAIL + 1))
  fi
}

# --------------------------------------------------------------------------
# Helper: each assert_* function queries DuckDB CLI and exits 0 (PASS) or 1
# (FAIL). They print the result row so the harness output is human-readable.
# --------------------------------------------------------------------------

assert_bronze_null_run_ids() {
  local out
  out=$(duckdb -c "
SELECT 'Bronze txn null run_ids' AS check_name,
       COUNT(*) AS result,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS verdict
FROM read_parquet('${DATA_DIR}/bronze/transactions/*/*.parquet')
WHERE _pipeline_run_id IS NULL
UNION ALL
SELECT 'Bronze acc null run_ids',
       COUNT(*),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM read_parquet('${DATA_DIR}/bronze/accounts/*/*.parquet')
WHERE _pipeline_run_id IS NULL
UNION ALL
SELECT 'Bronze tc null run_ids',
       COUNT(*),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM '${DATA_DIR}/bronze/transaction_codes/data.parquet'
WHERE _pipeline_run_id IS NULL;
" 2>&1)
  echo "$out"
  ! echo "$out" | grep -q ' FAIL '
}

assert_silver_null_run_ids() {
  local out
  out=$(duckdb -c "
SELECT 'Silver txn null run_ids' AS check_name,
       COUNT(*) AS result,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS verdict
FROM read_parquet('${DATA_DIR}/silver/transactions/*/*.parquet')
WHERE _pipeline_run_id IS NULL
UNION ALL
SELECT 'Silver acc null run_ids',
       COUNT(*),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM '${DATA_DIR}/silver/accounts/data.parquet'
WHERE _pipeline_run_id IS NULL
UNION ALL
SELECT 'Silver tc null run_ids',
       COUNT(*),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM '${DATA_DIR}/silver/transaction_codes/data.parquet'
WHERE _pipeline_run_id IS NULL;
" 2>&1)
  echo "$out"
  ! echo "$out" | grep -q ' FAIL '
}

assert_gold_null_run_ids() {
  local out
  out=$(duckdb -c "
SELECT 'Gold daily null run_ids' AS check_name,
       COUNT(*) AS result,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS verdict
FROM '${DATA_DIR}/gold/daily_summary/data.parquet'
WHERE _pipeline_run_id IS NULL
UNION ALL
SELECT 'Gold weekly null run_ids',
       COUNT(*),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM '${DATA_DIR}/gold/weekly_account_summary/data.parquet'
WHERE _pipeline_run_id IS NULL;
" 2>&1)
  echo "$out"
  ! echo "$out" | grep -q ' FAIL '
}

assert_silver_run_id_lineage() {
  local out
  out=$(duckdb -c "
SELECT 'Silver run_id lineage' AS check_name,
       COUNT(DISTINCT _pipeline_run_id) AS untraceable_run_ids,
       CASE WHEN COUNT(DISTINCT _pipeline_run_id) = 0 THEN 'PASS' ELSE 'FAIL' END AS verdict
FROM read_parquet('${DATA_DIR}/silver/transactions/*/*.parquet')
WHERE _pipeline_run_id NOT IN (
    SELECT run_id
    FROM '${DATA_DIR}/pipeline/run_log.parquet'
    WHERE status = 'SUCCESS'
);
" 2>&1)
  echo "$out"
  ! echo "$out" | grep -q ' FAIL '
}

# ==========================================================================
# INV-05 — Audit Chain Continuity (TC-1): Bronze null _pipeline_run_id
# Severity: CRITICAL
# Expected outcome: all three Bronze entities return result = 0 (verdict PASS)
# ==========================================================================
run_assertion "INV-05-TC1" "CRITICAL" "assert_bronze_null_run_ids"

# ==========================================================================
# INV-05 — Audit Chain Continuity (TC-2): Silver null _pipeline_run_id
# Severity: CRITICAL
# Expected outcome: all three Silver entities return result = 0 (verdict PASS)
# ==========================================================================
run_assertion "INV-05-TC2" "CRITICAL" "assert_silver_null_run_ids"

# ==========================================================================
# INV-05 — Audit Chain Continuity (TC-3): Gold null _pipeline_run_id
# Severity: CRITICAL
# Expected outcome: both Gold files return result = 0 (verdict PASS)
# ==========================================================================
run_assertion "INV-05-TC3" "CRITICAL" "assert_gold_null_run_ids"

# ==========================================================================
# INV-05 — Audit Chain Continuity (TC-4): Silver run_id → run_log lineage
# Severity: CRITICAL
# Expected outcome: untraceable_run_ids = 0 (verdict PASS)
# Every Silver _pipeline_run_id must trace to a SUCCESS entry in run_log.
# ==========================================================================
run_assertion "INV-05-TC4" "CRITICAL" "assert_silver_run_id_lineage"

# HARNESS SUMMARY
TOTAL=$((PASS + FAIL))
echo ""
echo "HARNESS SUMMARY"
echo "Total: $TOTAL  Passed: $PASS  Failed: $FAIL  (CRITICAL: $CRITICAL_FAIL  WARNING: $WARNING_FAIL)"
[ "$CRITICAL_FAIL" -gt 0 ] && exit 2 || [ "$FAIL" -gt 0 ] && exit 1 || exit 0
