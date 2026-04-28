# S05 — Session Log
## Credit Card Transactions Lake — Session 5: Pipeline Orchestration

**Branch:** `session/s05_orchestration`
**Date:** 2026-04-28
**Engineer:** Vijal Dsouza

---

## Pre-session State

Sessions 1–4 complete and verified. All three phase functions (`run_bronze_phase`,
`run_silver_phase`, `run_gold_phase`) in place. `main()` was a non-functional stub.
No watermark existed prior to this session.

---

## Environment Fix — Dockerfile protobuf pin

**Issue discovered:** Rebuilding the Docker image (required to copy updated `pipeline.py`)
pulled `protobuf==7.34.1`, which removed the `including_default_value_fields` parameter
from `MessageToJson()`. dbt-core==1.7.0 uses that parameter — all dbt builds failed.

**Fix:** Added `"protobuf>=4.0.0,<5.0.0"` to the pip install line in `Dockerfile`.
`dbt-core==1.7.0` declares `protobuf>=4.0.0`; the 4.x series retains the legacy API.
This is a build environment fix, not a pipeline logic change.

**File modified:** `Dockerfile`

---

## Task 5.1 — Historical Pipeline Orchestrator

**Status:** COMPLETE

**Files modified:** `pipeline.py`, `Dockerfile`

**Key decisions:**
- `run_id = str(uuid.uuid4())` — per task spec; generates UUID before validate_source_files
- `pipeline_start` run log entry written (layer="BRONZE", status="SUCCESS") after
  validate_source_files passes, before any phase runs — marks run as started in log
- Each phase failure appends a `pipeline_failed` log entry at the failing layer, then
  sys.exit(1) — both data and orchestrator-level FAILED entries recorded
- Watermark write is the FINAL operation on the success path (INV-09 satisfied)
- Watermark verified by read-back assertion after write
- `_run_historical` is a dedicated function (single purpose: execute the historical mode
  sequence) — keeps `main()` to a single conditional dispatch
- Dockerfile protobuf fix committed in same commit as Task 5.1 (dependency on fix to run verification)

**Commit:** `[S5.1] — Historical Pipeline Orchestrator: main() historical mode with INV-09 watermark gate`

---

## Task 5.2 — Incremental Pipeline Orchestrator

**Status:** COMPLETE

**Files modified:** `pipeline.py`

**Key decisions:**
- `_check_silver_accounts` helper: single-purpose guard function — checks existence and
  non-emptiness of silver_accounts before any phase function is called (F5 requirement)
- `next_date = watermark + timedelta(days=1)` computed from `read_watermark` result;
  `validate_source_files(config)` called after (it re-reads watermark internally but
  results are consistent since control.parquet is not modified between calls)
- No `pipeline_start` log entry for incremental mode — not specified in Task 5.2 prompt;
  F5 failure path appends FAILED entry which establishes run_id in log
- Watermark advances to `next_date` (not `config.end_date`) — end_date is None for
  incremental mode; watermark write pattern identical to historical (INV-09)
- TC-4 and TC-5 required creating stub 2024-01-08 source files to bypass
  validate_source_files (which fails first due to no 8th day files in seed). Stub files
  copied from 2024-01-07 and deleted after each test. INV-13 applies to pipeline code
  only; engineer test setup is not a pipeline code path.
- TC-3 used same stub 8th-day files; duplicate transaction_ids quarantined in Silver
  (INV-01 conserved); pipeline succeeded and watermark advanced to 2024-01-08 as expected.
  Watermark restored to 2024-01-07 by subsequent historical re-run for Task 5.3.

**Commit:** `[S5.2] — Incremental Pipeline Orchestrator: _run_incremental with F5 silver_accounts pre-check`

---

## Task 5.3 — Idempotency Hardening

**Status:** COMPLETE (verification only — no code written)

**Baseline (before re-run):**
| Metric | Count |
|---|---|
| bronze_txn | 40 |
| bronze_acc | 23 |
| silver_txn | 28 |
| quarantine | 12 |
| gold_daily | 7 |
| gold_weekly | 3 |
| run_log | 172 |
| watermark | 2024-01-07 |

**After second historical run:**
| Metric | Count | Match? |
|---|---|---|
| bronze_txn | 40 | PASS |
| bronze_acc | 23 | PASS |
| silver_txn | 28 | PASS |
| quarantine | 12 | PASS |
| gold_daily | 7 | PASS |
| gold_weekly | 3 | PASS |
| run_log | 194 | PASS (grew by 22 — new run appended; INV-06) |
| watermark | 2024-01-07 | PASS (unchanged; INV-09) |

Note: run_log count is not "2x" because there were multiple historical runs during
S04/S05 testing. The delta of 22 entries matches one full successful historical run.
INV-10 and INV-06 are both satisfied.

---

## Task 5.4 — Audit Trail Verification

**Status:** COMPLETE (verification only — no code written)

| Query | Result |
|---|---|
| Bronze txn null _pipeline_run_id | 0 — PASS |
| Bronze acc null _pipeline_run_id | 0 — PASS |
| Bronze codes null _pipeline_run_id | 0 — PASS |
| Silver txn null _pipeline_run_id | 0 — PASS |
| Silver acc null _pipeline_run_id | 0 — PASS |
| Silver codes null _pipeline_run_id | 0 — PASS |
| Gold daily null _pipeline_run_id | 0 — PASS |
| Gold weekly null _pipeline_run_id | 0 — PASS |
| Silver run_ids untraceable to run_log SUCCESS | 0 — PASS |

All audit chain assertions pass. INV-05 satisfied end-to-end.

---

## Open Items

None. All S05 invariants satisfied. Dockerfile protobuf regression resolved.

---

## Session Integration Check

| Check | Expected | Result |
|---|---|---|
| `docker compose run --rm pipeline python pipeline.py` exit code | 0 | PASS |
| Watermark after historical run | 2024-01-07 | PASS |
| Idempotency: all layer counts identical on re-run | identical | PASS |
| Audit trail: 0 null run_ids at any layer | 0 | PASS |
| Audit trail: 0 untraceable run_ids | 0 | PASS |

---

## HUMAN GATE

Claude does not declare this session complete. Engineer sign-off required before PR is raised.

**Engineer sign-off:** Vijal Dsouza  Date: 2026-04-28
