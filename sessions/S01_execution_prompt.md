# S01 — Execution Prompt
## Credit Card Transactions Lake — Session 1: Project Scaffold and Pipeline Skeleton

**Path:** `sessions/S01_execution_prompt.md`
**Claude.md version:** v1.0
**Execution mode:** [ ] Manual | [x] Autonomous

---

## AGENT IDENTITY

You are Claude Code operating under a frozen execution contract. Your authority comes
entirely from `docs/Claude.md` and the task prompts in this file. You may not make
decisions outside the scope of those documents.

---

## REPOSITORY CONTEXT

**Branch convention:** `session/s01_scaffold`

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

This is the first session — repository scaffolded, no prior state.

---

## SCOPE BOUNDARY

Files CC may create or modify in this session:
- `source/` — .gitkeep only
- `data/` subdirectories — .gitkeep files only
- `dbt_project/` — directory structure, stub SQL files, dbt_project.yml, profiles.yml
- `docs/` — .gitkeep only
- `.gitignore`
- `.env.example`
- `README.md`
- `pipeline.py` — empty stub only
- `Dockerfile`
- `docker-compose.yml`
- `config.py`
- `lake_io.py`

CC must not write to any file under `source/`. CC must not create any file not listed
above or not registered in `PROJECT_MANIFEST.md`. If a task prompt conflicts with an
invariant, the invariant wins — flag it, never resolve silently.

---

## SESSION GOAL

A running Docker container that starts, validates its configuration, and exits cleanly.
No data processing yet — foundation only.

**Session integration check:**
```bash
docker compose up --build
# Expected: container starts, prints startup validation output, exits with code 0 (or clear error if .env missing)
docker compose run pipeline python pipeline.py --help 2>/dev/null || echo "pipeline.py present"
```

---

## SESSION TASKS

Execute tasks in order. One commit per task.

### Task 1.1 — Repository Scaffold
Execute the CC prompt from `docs/EXECUTION_PLAN.md` Task 1.1 verbatim.

### Task 1.2 — Dockerfile and Docker Compose
Execute the CC prompt from `docs/EXECUTION_PLAN.md` Task 1.2 verbatim.

### Task 1.3 — Environment Configuration and Startup Validation Module
Execute the CC prompt from `docs/EXECUTION_PLAN.md` Task 1.3 verbatim.

### Task 1.4 — Source File Pre-flight Check
Execute the CC prompt from `docs/EXECUTION_PLAN.md` Task 1.4 verbatim.

### Task 1.5 — Run Log and Control Table Helpers
Execute the CC prompt from `docs/EXECUTION_PLAN.md` Task 1.5 verbatim.

---

## ARTIFACT PATHS

- Session log: `sessions/S01_LOG.md`
- Verification record: `sessions/S01_VERIFICATION_RECORD.md`

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
`sessions/S01_LOG.md` before any PR is raised.
