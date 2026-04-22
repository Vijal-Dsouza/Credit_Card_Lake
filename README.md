# Credit Card Lake

## What This Is

A financial services client processes credit card transactions across multiple channels daily. Data analysts and risk teams currently access raw extract files directly, bypassing quality control and producing inconsistent results with no audit trail. This system implements a Medallion architecture data lake (Bronze → Silver → Gold) that ingests daily transaction, account, and transaction code extract files, enforces defined quality rules at each layer boundary, and produces reliable Gold-layer aggregations that analysts can query with confidence via DuckDB. The pipeline does not compute risk, make credit decisions, or modify source system records — it ingests and surfaces pre-existing data in a controlled, auditable, and re-runnable way.

## Project Profile

Type: DATA_ACCELERATOR
Status: Phase 6 in progress — Session 1 complete, Session 2 not yet started.

## Where To Start

| If you want to... | Read this first |
|---|---|
| Understand the system | docs/ARCHITECTURE.md |
| Understand the constraints | docs/INVARIANTS.md |
| Understand the build history | sessions/ |
| Understand the current sign-off state | verification/ |
| Understand the system intelligence layer | discovery/INTAKE_SUMMARY.md |
| Work on an enhancement | enhancements/REGISTRY.md |

## Repository Structure

| Directory / File | Purpose |
|---|---|
| brief/ | Client inputs and requirements briefs — never modified after receipt |
| docs/ | PBVI trunk artifacts (ARCHITECTURE.md, INVARIANTS.md, EXECUTION_PLAN.md, Claude.md) |
| docs/prompts/ | CC execution prompts — methodology artifacts under version control |
| sessions/ | Working evidence — SESSION_LOG.md and VERIFICATION_RECORD.md |
| verification/ | Formal sign-off checklists — VERIFICATION_CHECKLIST.md per phase/enhancement |
| discovery/ | BCE SIL artifacts + discovery/components/ for component files |
| enhancements/ | REGISTRY.md + ENH-NNN subdirectory per enhancement |
| tools/ | Agentic build automation scripts — challenge.sh, resume_challenge.sh, resume_session.sh, monitor.sh, launch.sh (optional automation wrapper) |

## Rule Compliance

- Rule 1: All file references use full paths from repo root — never bare filenames.
- Rule 2: All files inside any enhancement package carry their ENH-NNN prefix — no exceptions.
- Rule 3: Any file not registered in PROJECT_MANIFEST.md must not be read by CC as authoritative input.

> **Host permission note:** Run `sudo chown -R 1000:1000 data/ source/` on the host before first run if you encounter permission errors writing to /data or /source.
