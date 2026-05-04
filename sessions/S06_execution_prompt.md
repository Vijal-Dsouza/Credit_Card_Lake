# S06 — Execution Prompt
## Credit Card Transactions Lake — Session 6: End-to-End Integration and Phase 8 Preparation

**Path:** `sessions/S06_execution_prompt.md`
**Claude.md version:** v1.0
**Execution mode:** [ ] Manual | [x] Autonomous

---

## AGENT IDENTITY

You are Claude Code operating under a frozen execution contract. Your authority comes
entirely from `docs/Claude.md` and the task prompts in this file. You may not make
decisions outside the scope of those documents.

---

## REPOSITORY CONTEXT

**Branch convention:** `session/s06_integration`

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

Sessions 1 through 5 delivered a complete, end-to-end pipeline. The following is in place
and verified:

- Docker environment, config validation, source pre-flight, `lake_io.py` (S1)
- All three Bronze loaders in `bronze_loaders.py`; `run_bronze_phase()` (S2)
- All four Silver dbt models; `run_silver_phase()` with INV-14 pre-check (S3)
- Both Gold dbt models with INV-16 fixed struct; `run_gold_phase()` (S4)
- `main()` fully wired for historical and incremental modes; watermark gated on full
  three-phase success (INV-09); idempotency hardened (INV-10); audit trail verified
  end-to-end (INV-05) via Task 5.4 HARNESS-CANDIDATE assertions (S5)

At the start of this session: `docker compose run --rm pipeline python pipeline.py` with
`PIPELINE_MODE=historical` completes for all 7 days of seed data, watermark = 2024-01-07,
run log contains SUCCESS entries for all models, Gold Parquet files are present. The
system is functionally complete. This session runs the full Phase 8 verification suite
and produces the VERIFICATION_CHECKLIST.md and regression suite.

---

## SCOPE BOUNDARY

Files CC may create or modify in this session:
- `verification/VERIFICATION_CHECKLIST.md` — produced at Task 6.2
- `verification/REGRESSION_SUITE.sh` — assembled at Task 6.3
- `verification/HARNESS.sh` — assembled at Task 6.3 from HARNESS-CANDIDATE tasks

CC must not write to any file under `source/`. CC must not modify any pipeline code,
dbt models, or `docs/` planning artifacts in this session. If any verification command
fails, that is a BLOCKED stop — not a code fix opportunity. CC must not create any file
not listed above or not registered in `PROJECT_MANIFEST.md`. If a task prompt conflicts
with an invariant, the invariant wins — flag it, never resolve silently.

---

## SESSION GOAL

All Phase 8 verification expectations confirmed passing. System ready for Phase 8 sign-off.
Regression suite and live invariant harness committed.

**Session integration check:**
```bash
docker compose run --rm pipeline python -c "print('Session 6 integration check — run all Phase 8 verification queries below')"
```

---

## SESSION TASKS

Execute tasks in order. One commit per task.

### Task 6.1 — Phase 8 Verification Command Suite
Execute the CC prompt from `docs/EXECUTION_PLAN.md` Task 6.1 verbatim.

Run all canonical checks (B1–B3, S1–S6, G1–G4, I1–I4, A1–A4) and record PASS/FAIL
verdicts. Each check ID must produce an explicit verdict. If any check returns FAIL,
this is a BLOCKED stop — do not proceed to Task 6.2.

### Task 6.2 — VERIFICATION_CHECKLIST.md Production
Execute the CC prompt from `docs/EXECUTION_PLAN.md` Task 6.2 verbatim.

Produce `verification/VERIFICATION_CHECKLIST.md` referencing the canonical check IDs
from Task 6.1. Pre-populate check IDs and commands from the verified run. Leave
engineer sign-off fields blank.

### Task 6.3 — Regression Suite Assembly
Execute the CC prompt from `docs/EXECUTION_PLAN.md` Task 6.3 verbatim.

Collect all REGRESSION-RELEVANT portable verification commands from EXECUTION_PLAN.md
and consolidate into `verification/REGRESSION_SUITE.sh`. Non-portable commands noted
with reason, not silently omitted.

Assemble `verification/HARNESS.sh` from all HARNESS-CANDIDATE tasks — currently Task 5.4
(INV-05 audit chain, four DuckDB CLI assertions). One section per assertion per the
Template 9 format from `pbvi_templates.md`. This is not optional and not deferred.

---

## ARTIFACT PATHS

- Session log: `sessions/S06_LOG.md`
- Verification record: `sessions/S06_VERIFICATION_RECORD.md`
- Verification checklist: `verification/VERIFICATION_CHECKLIST.md`
- Regression suite: `verification/REGRESSION_SUITE.sh`
- Live invariant harness: `verification/HARNESS.sh`

---

## STOP CONDITIONS

**BLOCKED:** Stop immediately on any verification failure in Task 6.1. Record BLOCKED in
session log. Output SESSION BLOCKED summary with the specific check ID that failed.
Wait for engineer — this indicates a gap in a prior session that must be resolved via
loop before Phase 8 sign-off can proceed.

**SCOPE VIOLATION:** Stop immediately if a file boundary check fails or a pre-commit
declaration fails. Record SCOPE VIOLATION. Wait for engineer disposition.

**INVARIANT CONFLICT:** If a task prompt conflicts with an invariant in `docs/Claude.md`,
flag the conflict explicitly. Do not resolve silently. Stop and wait for engineer.

**HUMAN GATE:** Claude never declares Phase 8 complete or this session signed off.
The engineer reviews `verification/VERIFICATION_CHECKLIST.md`, answers the three Phase 8
gate questions without opening any document, and signs off `sessions/S06_LOG.md`.
Only after sign-off may Phase 8 Part 2 (BCE) begin.
