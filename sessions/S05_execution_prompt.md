# S05 — Execution Prompt
## Credit Card Transactions Lake — Session 5: Pipeline Orchestration

**Path:** `sessions/S05_execution_prompt.md`
**Claude.md version:** v1.0
**Execution mode:** [ ] Manual | [ ] Autonomous

---

## AGENT IDENTITY

You are Claude Code operating under a frozen execution contract. Your authority comes
entirely from `docs/Claude.md` and the task prompts in this file. You may not make
decisions outside the scope of those documents.

---

## REPOSITORY CONTEXT

**Branch convention:** `session/s05_orchestration`

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

Sessions 1 through 4 delivered a fully operational Bronze–Silver–Gold data stack with all
three phase functions in place. The following is in place and verified:

- Docker environment, startup validation, source file pre-flight, `lake_io.py` helpers (S1)
- `run_bronze_phase(config, run_id)` — all three loaders, audit columns, idempotency (S2)
- All four Silver dbt models — conservation, sign assignment, code validation, referential
  isolation, quarantine, `_record_valid_from`; `run_silver_phase` with INV-14 pre-check (S3)
- Both Gold dbt models — `gold_daily_summary` (INV-04, INV-12, INV-16 fixed struct),
  `gold_weekly_account_summary` (INV-04, INV-12, closing_balance join); `run_gold_phase` (S4)
- `pipeline.py` currently contains: `PhaseResult` dataclass, `run_bronze_phase()`,
  `run_silver_phase()`, `run_gold_phase()`, `validate_source_files()`, and a stub `main()`
- The three phase functions have been individually verified. `main()` does not yet wire
  them together end-to-end or manage the watermark

At the start of this session: `docker compose run --rm pipeline python pipeline.py` exits
with a stub or no-op `main()`. No watermark has been written. The task in this session is
to complete `main()` for both pipeline modes and enforce watermark integrity end-to-end.

---

## SCOPE BOUNDARY

Files CC may create or modify in this session:
- `pipeline.py` — `main()` for historical and incremental modes; Tasks 5.3 and 5.4
  may add idempotency hardening and audit trail verification helpers

CC must not write to any file under `source/`. CC must not modify dbt SQL models,
`bronze_loaders.py`, `lake_io.py`, or `config.py` in this session (unless a gap in
a prior session's output surfaces — flag the gap; do not silently fix it). CC must not
create any file not listed above or not registered in `PROJECT_MANIFEST.md`. If a task
prompt conflicts with an invariant, the invariant wins — flag it, never resolve silently.

---

## SESSION GOAL

Complete `pipeline.py` — both historical and incremental modes fully wired. Watermark
advance gated on all three phases succeeding. End-to-end run completes for the 7-day
seed data.

**Session integration check:**
```bash
docker compose run --rm pipeline python pipeline.py
echo "Exit code: $?"
docker compose run --rm pipeline python -c "
from lake_io import read_watermark
import datetime
wm = read_watermark('/data')
print(f'Watermark: {wm}')
assert str(wm) == '2024-01-07', f'FAIL: watermark={wm}'
print('Watermark PASS')
"
```

---

## SESSION TASKS

Execute tasks in order. One commit per task.

### Task 5.1 — Historical Pipeline Orchestrator
Execute the CC prompt from `docs/EXECUTION_PLAN.md` Task 5.1 verbatim.

Critical invariants in this task:
- INV-09: Watermark write is the FINAL operation on the success path. No code path may
  advance the watermark before all three PhaseResult.success = True — never negotiable.
- INV-08: `sys.exit(1)` on any phase failure. Non-zero exit code — never negotiable.

### Task 5.2 — Incremental Pipeline Orchestrator
Execute the CC prompt from `docs/EXECUTION_PLAN.md` Task 5.2 verbatim.

Critical invariants:
- INV-09: Watermark read → derive next date → process → advance. Advance only on full
  three-phase success — never negotiable.
- F5: Silver accounts pre-check at incremental startup — verify non-empty before Bronze.

### Task 5.3 — Idempotency Hardening
Execute the CC prompt from `docs/EXECUTION_PLAN.md` Task 5.3 verbatim.

Critical invariants:
- INV-10: Second historical run on identical input must produce identical output — never negotiable.

### Task 5.4 — Audit Trail Verification
Execute the CC prompt from `docs/EXECUTION_PLAN.md` Task 5.4 verbatim.

Note: Task 5.4 is classified HARNESS-CANDIDATE — the DuckDB CLI audit assertions produced
here are the basis for `verification/HARNESS.sh` at Phase 8.

---

## ARTIFACT PATHS

- Session log: `sessions/S05_LOG.md`
- Verification record: `sessions/S05_VERIFICATION_RECORD.md`

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
`sessions/S05_LOG.md` before any PR is raised.
