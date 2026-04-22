# Claude.md — v1.0 · FROZEN · 2026-04-22

---

## 1. System Intent

This system is a Medallion data lake (Bronze → Silver → Gold) that ingests daily CSV
extract files for credit card transactions, accounts, and transaction codes; enforces
defined quality rules at each layer boundary; and produces Gold-layer aggregations
queryable via DuckDB CLI. It does not compute risk, make credit decisions, modify source
system records, or provide a serving API. Success is: an analyst runs a DuckDB query
against Gold Parquet files and receives consistent, auditable aggregations traceable
to a Bronze source file and a run log SUCCESS entry.

---

## 2. Hard Invariants

These invariants apply to every task without exception. TASK-SCOPED invariants are
embedded in task prompts in EXECUTION_PLAN.md and do not appear here.

**INVARIANT (methodology-mandated):** Each function, method, or handler must have a
single stateable purpose. Conditional nesting exceeding two levels is a structural
violation — refactor before proceeding. This is never negotiable.

**INVARIANT (INV-05):** Every record written to Bronze, Silver, and Gold must carry all
three non-null audit columns: `_pipeline_run_id`, `_source_file`, and `_ingested_at`
(or `_bronze_ingested_at` in Silver and Gold where carried forward from Bronze). Every
`_pipeline_run_id` appearing in any layer record must have a corresponding entry in
`pipeline/run_log.parquet` with `status = SUCCESS` for that model. No layer record may
exist whose `_pipeline_run_id` cannot be traced to a successful run log entry. This is
never negotiable.

**INVARIANT (INV-06):** `pipeline/run_log.parquet` must only ever be written to by
appending new rows. No pipeline code path may truncate, overwrite, or delete existing
rows in the run log under any circumstances — including re-runs, failure recovery, or
idempotency enforcement. This is never negotiable.

**INVARIANT (INV-08):** The watermark in `pipeline/control.parquet` must never advance
unless all three phase functions — `run_bronze_phase()`, `run_silver_phase()`,
`run_gold_phase()` — return success for that run. A partial run (one or two phases
succeeding before a failure) must not advance the watermark. This is never negotiable.

**INVARIANT (INV-10):** Running the pipeline twice on identical input must produce
identical output at every layer. Bronze: partition existence check prevents re-write.
Silver and Gold: dbt models must be replaceable without side effects. No layer may
produce different record counts or values on re-run for the same source data. This is
never negotiable.

**INVARIANT (INV-11):** No pipeline code path — in `pipeline.py`, `config.py`, any
helper module, or any dbt model — may make an external network call, import a network
library for live data retrieval, or reference any external service. DuckDB, dbt-duckdb,
and the Python standard library are the only permitted runtime dependencies. This is
never negotiable.

**INVARIANT (INV-13):** No pipeline code path — in `pipeline.py` or any dbt model —
may write to, modify, append to, or delete any file under the `source/` directory.
Source CSV files are read-only inputs. The pipeline may only open them in read mode.
This is never negotiable.

---

## 3. Scope Boundary

**CC may create or modify only these files and directories:**

| Path | Purpose |
|---|---|
| `pipeline.py` | Main orchestrator |
| `config.py` | Startup validation module |
| `bronze_loader.py` | Bronze ingestion logic |
| `Dockerfile` | Container definition |
| `docker-compose.yml` | Compose service definition |
| `.env.example` | Environment variable template |
| `.gitignore` | Repo hygiene |
| `README.md` | Project navigation |
| `requirements.txt` | Python dependencies (if produced separately) |
| `dbt_project/dbt_project.yml` | dbt project config |
| `dbt_project/profiles.yml` | dbt DuckDB profile |
| `dbt_project/models/silver/*.sql` | Silver dbt models |
| `dbt_project/models/gold/*.sql` | Gold dbt models |
| `dbt_project/models/silver/schema.yml` | Silver schema and dbt tests |
| `dbt_project/models/gold/schema.yml` | Gold schema and dbt tests |
| `data/` | All Bronze, Silver, Gold, and pipeline Parquet outputs (bind-mounted) |
| `sessions/S[N]_execution_prompt.md` | Session prompt files (Phase 5 output) |

**CC must not:**
- Write to or modify any file under `source/`
- Truncate, overwrite, or delete rows in `pipeline/run_log.parquet`
- Make external network calls or import network libraries
- Create files not listed above or not registered in `PROJECT_MANIFEST.md`

**Accepted limitations (not build defects):**
- A corrupted partial Bronze partition is silently skipped on re-run — Bronze self-repair
  is explicitly out of scope per brief (Decision 4 / F-03)
- `closing_balance` in Gold Weekly reflects Silver state at promotion time, not week-end
  date — SCD Type 2 is deferred (F-05)
- REFUND records from the source system would be quarantined as INVALID_TRANSACTION_CODE
  if seed data were extended — scope is fixed to the four transaction types in seed data
  for this exercise (F-04 / INV-16)

**Conflict rule:** If a task prompt conflicts with an invariant, the invariant wins —
flag it, never resolve silently.

---

## 4. Fixed Stack

| Component | Specification |
|---|---|
| Language | Python 3.11 |
| Container base | `python:3.11-slim` |
| DuckDB (Python package) | `duckdb==0.10.0` |
| dbt-core | `dbt-core==1.7.0` |
| dbt-duckdb adapter | `dbt-duckdb==1.7.0` |
| python-dotenv | latest compatible |
| pandas | latest compatible |
| pyarrow | latest compatible |
| dbt profile name | `cc_lake` |
| dbt target | `dev` |
| dbt catalog path | `{DATA_DIR}/pipeline/dbt_catalog.duckdb` (persistent, NOT :memory:) |
| Container UID | `1000:1000` |

**Environment variable names (from .env):**

| Variable | Purpose |
|---|---|
| `PIPELINE_MODE` | `historical` or `incremental` |
| `START_DATE` | Historical mode start (YYYY-MM-DD) |
| `END_DATE` | Historical mode end (YYYY-MM-DD) |
| `DATA_DIR` | Absolute path to data directory inside container (`/data`) |
| `SOURCE_DIR` | Absolute path to source directory inside container (`/source`) |

If a dependency or version is not listed here, CC must not choose its own — flag the gap.

---

## 5. Rules

**Rule 1:** All file references use full paths from repo root — never bare filenames.

**Rule 2:** All files inside any enhancement package carry their ENH-NNN prefix —
no exceptions.

**Rule 3:** Any file not in the mandatory set for its directory and not registered in
`PROJECT_MANIFEST.md` must not be read by CC as authoritative input. CC flags
unregistered files and reports them to the engineer before proceeding.
