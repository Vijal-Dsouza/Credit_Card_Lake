# S03 — Execution Prompt
## Credit Card Transactions Lake — Session 3: Silver dbt Models

**Path:** `sessions/S03_execution_prompt.md`
**Claude.md version:** v1.0
**Execution mode:** [ ] Manual | [ ] Autonomous

---

## AGENT IDENTITY

You are Claude Code operating under a frozen execution contract. Your authority comes
entirely from `docs/Claude.md` and the task prompts in this file. You may not make
decisions outside the scope of those documents.

---

## REPOSITORY CONTEXT

**Branch convention:** `session/s03_silver_models`

**Methodology version check:** Read `PROJECT_MANIFEST.md` and locate `METHODOLOGY_VERSION`.
Compare against the loaded skill frontmatter version. If they differ, output a
METHODOLOGY VERSION WARNING block — then continue without stopping.

**Planning artifacts:**
- `docs/Claude.md` — execution contract (FROZEN)
- `docs/ARCHITECTURE.md` — architecture decisions
- `docs/INVARIANTS.md` — system invariants
- `docs/EXECUTION_PLAN.md` — task prompts and verification commands

---

## WHAT HAS ALREADY BEEN BUILT

Sessions 1 and 2 delivered a working Bronze layer. The following is in place and verified:

- Full directory scaffold and Docker environment operational (Session 1)
- `bronze_loaders.py` — three loader functions: `load_bronze_transaction_codes()`,
  `load_bronze_accounts(config, date, run_id)`, `load_bronze_transactions(config, date, run_id)`
- `run_bronze_phase(config, run_id)` in `pipeline.py` — sequences loaders in correct order
  (transaction codes first, then accounts and transactions per date); returns `PhaseResult`
- All audit columns (`_source_file`, `_ingested_at`, `_pipeline_run_id`) non-null on every
  Bronze record (INV-05 enforced at Bronze write)
- Bronze partition existence check (INV-07) — skip if partition already present; no dedup
- Source files opened read-only (INV-13)
- F2 re-read integrity check on every partition write
- F3 WARNING run log entry on empty source CSV

At the start of this session: all 7 days of Bronze transactions, accounts, and the
transaction_codes reference are loaded to Parquet under `data/bronze/`. Silver models
are still stub SQL files (`SELECT 1 AS placeholder`).

**Pre-condition check before running any Silver verification command:**
```bash
docker compose run --rm pipeline python -c "
import duckdb, sys
try:
    bronze_txn = duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/bronze/transactions/*/*.parquet')\").fetchone()[0]
    assert bronze_txn > 0, 'Bronze transactions empty — run Session 2 before Session 3'
    print(f'Bronze fixture present: {bronze_txn} rows')
except Exception as e:
    print(f'FAIL — Bronze not loaded: {e}'); sys.exit(1)
"
```

---

## SCOPE BOUNDARY

Files CC may create or modify in this session:
- `dbt_project/models/silver/silver_transaction_codes.sql`
- `dbt_project/models/silver/silver_accounts.sql`
- `dbt_project/models/silver/silver_quarantine.sql`
- `dbt_project/models/silver/silver_transactions.sql`
- `dbt_project/models/silver/schema.yml`

CC must not write to any file under `source/`. CC must not modify `pipeline.py`,
`bronze_loaders.py`, `lake_io.py`, or `config.py` in this session. CC must not create
any file not listed above or not registered in `PROJECT_MANIFEST.md`. If a task prompt
conflicts with an invariant, the invariant wins — flag it, never resolve silently.

---

## SESSION GOAL

All four Silver dbt models operational and producing correct, validated output from Bronze.
Quarantine populated for invalid records. Conservation equation holds.

**Session integration check:**
```bash
# Pre-condition: Bronze must be present (see above)
docker compose run --rm pipeline python -c "
import duckdb
silver_txn = duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/*/*.parquet')\").fetchone()[0]
quarantine = duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/silver/quarantine/*/*.parquet')\").fetchone()[0]
bronze_txn = duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/bronze/transactions/*/*.parquet')\").fetchone()[0]
assert silver_txn + quarantine == bronze_txn, f'Conservation FAIL: {silver_txn} + {quarantine} != {bronze_txn}'
print(f'Conservation check PASS: {silver_txn} silver + {quarantine} quarantine = {bronze_txn} bronze')
"
```

---

## SESSION TASKS

Execute tasks in order. One commit per task.

### Task 3.1 — Silver Transaction Codes Model
Execute the CC prompt from `docs/EXECUTION_PLAN.md` Task 3.1 verbatim.

### Task 3.2 — Silver Accounts Model
Execute the CC prompt from `docs/EXECUTION_PLAN.md` Task 3.2 verbatim.

### Task 3.3 — Silver Quarantine Model
Execute the CC prompt from `docs/EXECUTION_PLAN.md` Task 3.3 verbatim.

### Task 3.4 — Silver Transactions Model
Execute the CC prompt from `docs/EXECUTION_PLAN.md` Task 3.4 verbatim.

### Task 3.5 — Silver Phase Function
Execute the CC prompt from `docs/EXECUTION_PLAN.md` Task 3.5 verbatim.

---

## ARTIFACT PATHS

- Session log: `sessions/S03_LOG.md`
- Verification record: `sessions/S03_VERIFICATION_RECORD.md`

---

## STOP CONDITIONS

**BLOCKED:** Stop immediately on any verification failure. Record BLOCKED in session log.
Output SESSION BLOCKED summary. Wait for engineer.

**SCOPE VIOLATION:** Stop immediately if a file boundary check fails or a pre-commit
declaration fails. Record SCOPE VIOLATION. Output SCOPE VIOLATION summary. Wait for
engineer disposition (ACCEPT or REVERT).

**INVARIANT CONFLICT:** If a task prompt conflicts with an invariant in `docs/Claude.md`,
flag the conflict explicitly. Do not resolve silently. Stop and wait for engineer.

**HUMAN GATE:** Claude never declares this session complete. The engineer signs off
`sessions/S03_LOG.md` before any PR is raised.
