# S02 — Execution Prompt
## Credit Card Transactions Lake — Session 2: Bronze Loader

**Path:** `sessions/S02_execution_prompt.md`
**Claude.md version:** v1.0
**Execution mode:** [ ] Manual | [x] Autonomous

---

## AGENT IDENTITY

You are Claude Code operating under a frozen execution contract. Your authority comes
entirely from `docs/Claude.md` and the task prompts in this file. You may not make
decisions outside the scope of those documents.

---

## REPOSITORY CONTEXT

**Branch convention:** `session/s02_bronze_loader`

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

Session 1 delivered a running Docker container with validated startup. The following
is in place and verified:

- Full directory scaffold (`source/`, `data/` subdirectories, `dbt_project/` with stub SQL files)
- `Dockerfile` and `docker-compose.yml` — container builds with Python 3.11, duckdb 0.10.0,
  dbt-core 1.7.0, dbt-duckdb 1.7.0; container runs as UID 1000; bind mounts `data/` and `source/`
- `config.py` — `load_config()` validates `PIPELINE_MODE`, `START_DATE`/`END_DATE` (historical),
  `DATA_DIR`, `SOURCE_DIR`; deletes stale dbt catalog at startup (R3); returns `PipelineConfig`
  dataclass or exits with code 1 and clear message
- `pipeline.py` — `validate_source_files()` pre-flight checks all required CSVs exist before
  any phase function runs; exits with code 1 on any missing file
- `lake_io.py` — `read_watermark()`, `write_watermark()`, `append_run_log()`, `run_log_exists()`,
  `sanitise_error_message()` implemented and verified
- All invariants from Session 1 tasks confirmed via verification commands

At the start of this session: `docker compose up --build` exits cleanly. `config.py` and
`lake_io.py` are importable inside the container. No Bronze data has been loaded.

---

## SCOPE BOUNDARY

Files CC may create or modify in this session:
- `bronze_loaders.py` — new file, all three loaders implemented here

CC must not write to any file under `source/`. CC must not create any file not listed
above or not registered in `PROJECT_MANIFEST.md`. If a task prompt conflicts with an
invariant, the invariant wins — flag it, never resolve silently.

---

## SESSION GOAL

All three Bronze loaders operational. Running the historical pipeline loads all source
CSVs to Bronze Parquet with correct audit columns, correct partition paths, and idempotency.

**Session integration check:**
```bash
docker compose run --rm pipeline python -c "
import duckdb
txn = duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/bronze/transactions/*/*.parquet')\").fetchone()[0]
acc = duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/bronze/accounts/*/*.parquet')\").fetchone()[0]
tc  = duckdb.execute(\"SELECT COUNT(*) FROM '/data/bronze/transaction_codes/data.parquet'\").fetchone()[0]
print(f'Bronze transactions: {txn}, accounts: {acc}, transaction_codes: {tc}')
assert txn > 0 and acc > 0 and tc > 0, 'Bronze load failed'
"
```

---

## SESSION TASKS

Execute tasks in order. One commit per task.

### Task 2.1 — Bronze Transaction Codes Loader
Execute the CC prompt from `docs/EXECUTION_PLAN.md` Task 2.1 verbatim.

### Task 2.2 — Bronze Accounts Loader
Execute the CC prompt from `docs/EXECUTION_PLAN.md` Task 2.2 verbatim.

### Task 2.3 — Bronze Transactions Loader
Execute the CC prompt from `docs/EXECUTION_PLAN.md` Task 2.3 verbatim.

### Task 2.4 — Bronze Phase Function
Execute the CC prompt from `docs/EXECUTION_PLAN.md` Task 2.4 verbatim.

---

## ARTIFACT PATHS

- Session log: `sessions/S02_LOG.md`
- Verification record: `sessions/S02_VERIFICATION_RECORD.md`

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
`sessions/S02_LOG.md` before any PR is raised.
