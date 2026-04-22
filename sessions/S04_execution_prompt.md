# S04 — Execution Prompt
## Credit Card Transactions Lake — Session 4: Gold dbt Models

**Path:** `sessions/S04_execution_prompt.md`
**Claude.md version:** v1.0
**Execution mode:** [ ] Manual | [ ] Autonomous

---

## AGENT IDENTITY

You are Claude Code operating under a frozen execution contract. Your authority comes
entirely from `docs/Claude.md` and the task prompts in this file. You may not make
decisions outside the scope of those documents.

---

## REPOSITORY CONTEXT

**Branch convention:** `session/s04_gold_models`

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

Sessions 1, 2, and 3 delivered working Bronze and Silver layers. The following is in
place and verified:

- Full Bronze layer: transaction codes, accounts (7 partitions), transactions (7 partitions)
  loaded to Parquet with correct audit columns and idempotency (Sessions 1–2)
- Silver transaction codes — promoted from Bronze, non-null constraints verified
- Silver accounts — latest-record-only upsert; `_record_valid_from` non-null (INV-05);
  conservation: every Bronze accounts record exits as Silver upsert or quarantine (INV-15)
- Silver quarantine — all rejection codes in pre-defined list; `_rejection_reason` non-null;
  DUPLICATE_TRANSACTION_ID glob-safety guard for first batch (R1)
- Silver transactions — sign assignment from `silver_transaction_codes` join (INV-02);
  transaction code validation via join not hardcoded list (INV-03); `_is_resolvable` flag
  for unmatched account_ids (INV-04); cross-partition deduplication (INV-01)
- Conservation equation holds per date partition: Bronze = Silver + Quarantine (INV-01)
- `run_silver_phase(config, run_id)` — `silver_transaction_codes` non-empty check before
  any transactions promotion (INV-14); phase halt on failure (INV-08); uses `dbt build`
  (F-NEW-2)
- Gold stub SQL files still contain `SELECT 1 AS placeholder`

At the start of this session: Silver is fully populated and verified. Gold models need
to be implemented. `data/gold/` directories exist but are empty.

**Pre-condition check before running any Gold verification command:**
```bash
docker compose run --rm pipeline python -c "
import duckdb, sys
try:
    silver_txn = duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/*/*.parquet')\").fetchone()[0]
    assert silver_txn > 0, 'Silver transactions empty — run Session 3 before Session 4'
    silver_acc = duckdb.execute(\"SELECT COUNT(*) FROM '/data/silver/accounts/data.parquet'\").fetchone()[0]
    assert silver_acc > 0, 'Silver accounts empty — run Session 3 before Session 4'
    print(f'Silver fixture present: {silver_txn} transactions, {silver_acc} accounts')
except Exception as e:
    print(f'FAIL — Silver not loaded: {e}'); sys.exit(1)
"
```

---

## SCOPE BOUNDARY

Files CC may create or modify in this session:
- `dbt_project/models/gold/gold_daily_summary.sql`
- `dbt_project/models/gold/gold_weekly_account_summary.sql`
- `dbt_project/models/gold/schema.yml`
- `pipeline.py` — Gold phase function only (`run_gold_phase`)

CC must not write to any file under `source/`. CC must not modify Silver dbt models,
`bronze_loaders.py`, `lake_io.py`, or `config.py` in this session. CC must not create
any file not listed above or not registered in `PROJECT_MANIFEST.md`. If a task prompt
conflicts with an invariant, the invariant wins — flag it, never resolve silently.

---

## SESSION GOAL

Both Gold models operational, producing correct aggregations from Silver. Unique key
constraints enforced. Gold excludes `_is_resolvable = false` records.

**Session integration check:**
```bash
# Pre-condition: Silver must be present (see above)
docker compose run --rm pipeline python -c "
import duckdb
daily = duckdb.execute(\"SELECT COUNT(*) FROM '/data/gold/daily_summary/data.parquet'\").fetchone()[0]
weekly = duckdb.execute(\"SELECT COUNT(*) FROM '/data/gold/weekly_account_summary/data.parquet'\").fetchone()[0]
print(f'Gold daily: {daily} rows, weekly: {weekly} rows')
assert daily > 0 and weekly > 0, 'Gold empty'
"
```

---

## SESSION TASKS

Execute tasks in order. One commit per task.

### Task 4.1 — Gold Daily Summary Model
Execute the CC prompt from `docs/EXECUTION_PLAN.md` Task 4.1 verbatim.

Key invariants embedded in this task:
- INV-04: `WHERE _is_resolvable = true` on all aggregations — never negotiable
- INV-12: `unique_key='transaction_date'` — one row per date — never negotiable
- INV-16: `transactions_by_type` STRUCT must contain exactly four keys: PURCHASE, PAYMENT,
  FEE, INTEREST with zero-fill. REFUND is explicitly excluded. Key set is fixed — not
  derived dynamically — never negotiable
- R2: Silver transactions glob-safety guard required before any `read_parquet` on the glob

### Task 4.2 — Gold Weekly Account Summary Model
Execute the CC prompt from `docs/EXECUTION_PLAN.md` Task 4.2 verbatim.

Key invariants embedded in this task:
- INV-04: `WHERE _is_resolvable = true` — never negotiable
- INV-12: `unique_key` on `(week_start_date, account_id)` — never negotiable
- R2: Silver transactions glob-safety guard required

### Task 4.3 — Gold Phase Function
Execute the CC prompt from `docs/EXECUTION_PLAN.md` Task 4.3 verbatim.

---

## ARTIFACT PATHS

- Session log: `sessions/S04_LOG.md`
- Verification record: `sessions/S04_VERIFICATION_RECORD.md`

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
`sessions/S04_LOG.md` before any PR is raised.
