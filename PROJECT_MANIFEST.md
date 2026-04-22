# PROJECT_MANIFEST.md

## Project Details

| Field | Value |
|---|---|
| **Project Name** | Credit Card Financial Transactions Lake |
| **Profile** | DATA_ACCELERATOR |
| **METHODOLOGY_VERSION** | PBVI v4.4 / BCE v1.7 |
| **Classification** | Training Demo System — PBVI Data Engineering Training Vehicle |
| **Brief Version** | v1.0 |
| **Status** | Phase 5 complete — Phase 6 (Build) not yet started |

**Brief description:** A structured Medallion data lake (Bronze → Silver → Gold) that ingests daily credit card transaction extract files, enforces defined quality rules at each layer boundary, and produces Gold-layer aggregations queryable via DuckDB. The pipeline serves financial data analysts and risk teams who currently access raw extract files directly — bypassing quality control and producing inconsistent results. The system does not compute risk, make credit decisions, or modify source system records.

---

## Core Documents

| File | Status | Phase | Owner | Description |
|---|---|---|---|---|
| brief/Credit_Card_Transactions_Lake_Requirements_Brief_2.docx | PRESENT | Pre-Phase 1 | Engineer | Requirements brief v1.0 — never modified after receipt |
| docs/ARCHITECTURE.md | PRESENT | Phase 1 | Engineer | Architecture decisions and design rationale — v1.2, signed off by Engineer 21/04/2026 |
| docs/INVARIANTS.md | PRESENT | Phase 2 | Engineer | System invariants — v1.1, signed off by Vijal 21/04/2026. 16 invariants (INV-01 through INV-16); 6 GLOBAL, 10 TASK-SCOPED |
| docs/EXECUTION_PLAN.md | PRESENT | Phase 3 | Engineer | Task execution plan — v1.5, signed off by Vijal 21/04/2026. 6 sessions, 23 tasks. Frozen after Phase 4 gate |
| docs/PHASE4_GATE_RECORD.md | PRESENT | Phase 4 | Engineer | Design Gate record — verdict APPROVE, signed by Vijal 22/04/2026. Both RESOLVE findings addressed in v1.6 before sign-off |
| docs/Claude.md | PRESENT | Phase 5 | Engineer | AI execution contract — FROZEN. Produced 22/04/2026. 6 GLOBAL invariants embedded; scope boundary and tooling constraints locked |

---

## Non-Standard Registered Files

| File | Status | Phase | Owner | Description |
|---|---|---|---|---|
| source/accounts_20240101.csv | PRESENT | Pre-Phase 1 | Engineer | Seed data — accounts delta file for 2024-01-01. 2 records (ACC-001, ACC-002) |
| source/transaction_codes.csv | PRESENT | Pre-Phase 1 | Engineer | Seed data — static transaction codes reference. 4 codes (PURCH01, PAY01, FEE01, INT01) |
| source/transactions_20240101.csv | PRESENT | Pre-Phase 1 | Engineer | Seed data — transactions file for 2024-01-01. 5 records |
| docs/prompts/S1_execution_prompt.md | PRESENT | Phase 5 | Engineer | Session 1 execution prompt — Project scaffold, Docker environment, pipeline.py skeleton. 5 tasks |
| docs/prompts/S2_execution_prompt.md | PRESENT | Phase 5 | Engineer | Session 2 execution prompt — Bronze loader: transactions, accounts, transaction codes. 4 tasks |
| docs/prompts/S3_execution_prompt.md | PRESENT | Phase 5 | Engineer | Session 3 execution prompt — Silver dbt models: transaction codes, accounts, transactions, quarantine. 5 tasks |
| docs/prompts/S4_execution_prompt.md | PRESENT | Phase 5 | Engineer | Session 4 execution prompt — Gold dbt models: daily summary, weekly account summary. 3 tasks |
| docs/prompts/S5_execution_prompt.md | PRESENT | Phase 5 | Engineer | Session 5 execution prompt — Pipeline orchestration: historical, incremental, idempotency, audit trail. 4 tasks |
| docs/prompts/S6_execution_prompt.md | PRESENT | Phase 5 | Engineer | Session 6 execution prompt — End-to-end integration, Phase 8 sign-off preparation. 3 tasks |

---

## Non-Standard Registered Directories

| Directory | Status | Phase | Owner | Description |
|---|---|---|---|---|
| source/ | PRESENT | Pre-Phase 1 | Engineer | Static seed CSV files — read-only input to the pipeline. Never modified by build |
| docs/prompts/ | PRESENT | Phase 5 | Engineer | CC session execution prompt files — one per session (S1–S6). Methodology artifacts under version control |

---

## Session Logs

| File | Status | Phase | Owner | Description |
|---|---|---|---|---|
| *(populated as sessions run)* | | | | |

---

## Verification Records

| File | Status | Phase | Owner | Description |
|---|---|---|---|---|
| *(populated as sessions run)* | | | | |

---

## Verification Checklists

| File | Status | Phase | Owner | Description |
|---|---|---|---|---|
| verification/VERIFICATION_CHECKLIST.md | PENDING | Phase 8 | Engineer | Phase 8 system sign-off — 16 invariants + 21 canonical checks (B1–B3, S1–S6, G1–G4, I1–I4, A1–A4) |

---

## Discovery Artifacts

| File | Status | Phase | Owner | Description |
|---|---|---|---|---|
| discovery/INTAKE_SUMMARY.md | PENDING | Phase 8 | Engineer | BCE prerequisite artifact — Stage 1 |
| discovery/TOPOLOGY.md | PENDING | Phase 8 | Engineer | System topology — living extraction artifact |
| discovery/MODULE_CONTRACTS.md | PENDING | Phase 8 | Engineer | Module contracts — living extraction artifact |
| discovery/INTEGRATION_CONTRACTS.md | PENDING | Phase 8 | Engineer | Integration contracts — living extraction artifact |
| discovery/INVARIANT_CATALOGUE.md | PENDING | Phase 8 | Engineer | Invariant catalogue — living extraction artifact |
| discovery/RISK_REGISTER.md | PENDING | Phase 8 | Engineer | Risk register — living extraction artifact |
| discovery/ANNOTATION_CHECKLIST.md | PENDING | Phase 8 | Engineer | BCE attestation artifact — Stage 3 |

---

## Enhancement Registry

| File | Status | Phase | Owner | Description |
|---|---|---|---|---|
| enhancements/REGISTRY.md | PENDING | Post-Phase 8 | Sprint Lead | Enhancement registry |

---

## Structural Exceptions

| File | Location | Reason exempt from directory contracts |
|---|---|---|
| README.md | repo root | Universal repo convention — navigation and orientation only |
| PROJECT_MANIFEST.md | repo root | This file — registry cannot register itself |

---

## Open Items

| # | Item | Detail |
|---|---|---|
| OI-01 | ARCHITECTURE.md filename discrepancy | Project file is named `ARCHITECTURE_v1_2.md` — does not match the standard `docs/ARCHITECTURE.md` path required by Rule 1. Engineer to confirm canonical path and rename before first build session. |
| OI-02 | EXECUTION_PLAN version in Gate Record | PHASE4_GATE_RECORD references `EXECUTION_PLAN_v1.6.md` as the reviewed version; project file present is `EXECUTION_PLAN_v1_5.md`. v1.6 changes (HARNESS-CANDIDATE elevation for Task 5.4; INV-14 cross-reference addition) are recorded as RESOLVED in the Gate Record. Engineer to confirm v1.6 file exists and is committed, or amend the Gate Record reference. |
