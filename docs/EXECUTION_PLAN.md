# EXECUTION_PLAN.md — Credit Card Transactions Lake — v1.7

## Changelog
| Version | Date | Author | Change |
|---|---|---|---|
| v1.7 | 2026-05-04 | Vijal | S6 check (INV-15 Account Promotion Conservation) revised from per-date to aggregate form: `bronze_distinct_accounts == silver_distinct + quarantined_accounts`. Per-date methodology using `_source_file` was incompatible with `silver_accounts` latest-wins architecture — the model retains only the most recent record per account_id, so all accounts carry `_source_file` of the most recent ingestion date, making per-date attribution yield 0 for all but the final date. Aggregate form correctly verifies INV-15 (no account record silently dropped) without assuming per-date source file retention. Signed off by Vijal 2026-05-04. |
| v1.0 | 2026-04-16 | Vijal | Greenfield — Initial |
| v1.6 | 2026-04-21 | Vijal | Gap resolution pass — G8 and G9 applied: Task 3.4 added to INV-14 cross-reference row with note that enforcement is via reference JOIN in the model (halt mechanism remains in Task 3.5) (G8); Task 5.4 designated HARNESS-CANDIDATE — standalone DuckDB CLI harness form added covering all four audit assertions (TC-1 through TC-4) as stateless SQL runnable against any deployed lake instance without build context; regression classification updated from REGRESSION-RELEVANT to HARNESS-CANDIDATE (G9) |
| v1.5 | 2026-04-21 | Vijal | Gap resolution pass — G1 through G7 applied: INV-15 (Account Promotion Conservation) added to Task 3.2 CC prompt, test cases, verification command, and Invariant Cross-Reference table (G1); INV-16 (Gold Struct Shape Integrity) added to Task 4.1 CC prompt — REFUND removed from struct definition, fixed four-key set enforced with zero-fill, dbt struct integrity test added, Task 4.1 invariant enforcement updated (G2); Task 6.1 Phase 8 verification suite extended with checks S6 (INV-15 accounts conservation) and G4 (INV-16 struct shape), invariant enforcement note updated to INV-01 through INV-16, Task 6.2 verification command updated (G3); Task 3.1 verification command updated from dbt run+test to dbt build; Task 3.1 TC-1 label updated (G4); Task 3.2 CC prompt and schema.yml updated with _record_valid_from audit column per INV-05 update and ARCHITECTURE v1.2 (G5); Task 3.3 quarantine location config explicitly set to write rejected.parquet, consistent with all verification command references (G6); Regression Classification Summary table corrected — Tasks 5.3 and 5.4 added as REGRESSION-RELEVANT (G7) |
| v1.4 | 2026-04-21 | Vijal | Finding resolution pass — R1 through R5 applied: Task 3.3 CC prompt updated with glob-safety guard for DUPLICATE_TRANSACTION_ID check — if no Silver transaction partitions exist, skip the glob read and evaluate duplicate check as FALSE for all records (R1); Tasks 4.1 and 4.2 CC prompts updated with glob-safety guard before read_parquet on Silver transactions — if no partitions exist, use empty result set rather than raising file-not-found (R2); Task 1.3 CC prompt updated with startup dbt_catalog.duckdb deletion step — delete {data_dir}/pipeline/dbt_catalog.duckdb if present before any phase function runs (R3); Tasks 3.3 and 3.4 verification commands updated — dbt run + dbt test replaced with dbt build (R4); Resolved Decisions Table updated with dbt-duckdb persistent path decision (R5) |
| v1.3 | 2026-04-21 | Vijal | Finding resolution pass — F-NEW-1, F-NEW-2, F-NEW-3 applied: dbt-duckdb profile corrected from :memory: to persistent path, DATA_DIR var wired into dbt_project.yml vars block and profiles.yml, all Silver/Gold model tasks updated to use var('data_dir') for reads and location config for writes (F-NEW-1); Task 3.5 and 4.3 updated to use `dbt build` instead of `dbt run` + separate `dbt test`, ensuring schema tests run atomically with models (F-NEW-2); Task 1.5 updated with error_message sanitisation rule — filesystem paths redacted, stack traces forbidden in run log (F-NEW-3) |
| v1.2 | 2026-04-17 | Vijal | Finding resolution pass — F2 and F3 (second pass) applied: Task 6.1 I1–I4 and A1–A4 checks fully enumerated with explicit IDs and commands; Task 6.2 updated to reference canonical check IDs; Task 3.5 and 4.3 run log append behaviour made explicit for all code paths |
| v1.1 | 2026-04-17 | Vijal | Finding resolution pass — F1 through F7 applied: watermark init explicit in S5.1 CC prompt (F1); Bronze readability check added to S2.1/S2.2/S2.3 (F2); empty-source warning run log entry added to S2.1/S2.2/S2.3 (F3); S3/S4 verification commands confirmed against fixture Bronze Parquet (F4); Silver accounts pre-check added to S5.2 (F5); silver_quarantine ↔ silver_transactions inter-model dependency mechanism clarified in S3.4 (F6); Dockerfile USER/UID ordering fixed and verified in S1.2 (F7) |

---

## Resolved Decisions Table

| Open Question | Concrete Answer |
|---|---|
| Companion scaffold not available | Engineer owns all directory structure, dbt project layout, Docker Compose config, and pipeline.py structure per ARCHITECTURE.md Decision 6 |
| Pipeline mode switching | Driven by PIPELINE_MODE in .env file. Valid values: historical, incremental |
| Historical mode parameters | START_DATE and END_DATE read from .env |
| Incremental mode next date | watermark + 1 day, read from pipeline/control.parquet |
| Bronze idempotency | Partition existence check — skip if present, no dedup logic |
| Silver account history | Latest record only (no SCD Type 2) |
| Transaction codes timing | Loaded to Bronze and Silver before any transaction or account processing |
| Source file pre-flight | Existence check for all required files before any phase function runs |
| Orchestration structure | Three discrete phase functions returning PhaseResult — gated sequential execution |
| Docker setup | Single Python 3.11 container, DuckDB as Python package, bind-mounted data/ and source/ |
| dbt-duckdb catalog path | Persistent DuckDB file at {DATA_DIR}/pipeline/dbt_catalog.duckdb — not :memory:. Required so dbt resolves cross-model ref() dependencies within a single pipeline run. The DuckDB file is the dbt catalog/metadata store only — Silver and Gold Parquet outputs are written via location config, not through this file. The file is deleted at pipeline startup before any phase function runs to prevent stale catalog state across runs (R3). |

---

## Session Overview

| Session | Goal | Tasks | Est. Duration |
|---|---|---|---|
| S1 | Project scaffold, Docker environment, pipeline.py skeleton with startup validation | 5 | 60–90 min |
| S2 | Bronze loader — transactions, accounts, transaction codes | 4 | 60–90 min |
| S3 | Silver dbt models — transaction codes, accounts, transactions (including quarantine) | 5 | 90–120 min |
| S4 | Gold dbt models — daily summary and weekly account summary | 3 | 60 min |
| S5 | Pipeline orchestration — historical and incremental pipelines, watermark, run log | 4 | 90 min |
| S6 | End-to-end integration, idempotency verification, Phase 8 sign-off preparation | 3 | 60–90 min |

---

## Session 1 — Project Scaffold and Pipeline Skeleton

**Session goal:** A running Docker container that starts, validates its configuration, and exits cleanly. No data processing yet — foundation only.

**Integration check:**
```bash
docker compose up --build
# Expected: container starts, prints startup validation output, exits with code 0 (or clear error if .env missing)
docker compose run pipeline python pipeline.py --help 2>/dev/null || echo "pipeline.py present"
```

---

### Task 1.1 — Repository Scaffold

**Description:** Create the full directory structure, all stub files, and commit the scaffold. This is the first task of the first session — sets up everything subsequent tasks build on.

**CC Prompt:**
```
Create the following directory and file structure for the Credit Card Transactions Lake project.
All paths are from the repo root.

Directories to create (use .gitkeep for empty directories):
  source/
  data/bronze/transactions/
  data/bronze/accounts/
  data/bronze/transaction_codes/
  data/silver/transactions/
  data/silver/accounts/
  data/silver/transaction_codes/
  data/silver/quarantine/
  data/gold/daily_summary/
  data/gold/weekly_account_summary/
  data/pipeline/
  dbt_project/models/silver/
  dbt_project/models/gold/
  docs/

Files to create:
  .gitignore — include: .env, data/, __pycache__/, *.pyc, .dbt/
  .env.example — with these keys and placeholder values:
    PIPELINE_MODE=historical
    START_DATE=2024-01-01
    END_DATE=2024-01-07
    DATA_DIR=/data
    SOURCE_DIR=/source
  README.md — one paragraph describing the project as a training medallion architecture pipeline
  pipeline.py — empty stub with a main() function and if __name__ == "__main__": main()
  dbt_project/dbt_project.yml — minimal valid dbt project config (F-NEW-1):
    name: cc_transactions_lake
    version: "1.0.0"
    config-version: 2
    profile: cc_lake
    model-paths: ["models"]
    vars:
      data_dir: "{{ env_var('DATA_DIR', '/data') }}"
    models:
      cc_transactions_lake:
        silver:
          +materialized: table
        gold:
          +materialized: table

  The vars block is required (F-NEW-1). All Silver and Gold dbt models reference the data
  directory via var('data_dir') for read_parquet() source paths and via the location config
  for output paths. The env_var fallback '/data' matches the Docker bind-mount default.

  dbt_project/profiles.yml — DuckDB profile (F-NEW-1):
    cc_lake:
      target: dev
      outputs:
        dev:
          type: duckdb
          path: "{{ env_var('DATA_DIR', '/data') }}/pipeline/dbt_catalog.duckdb"

  IMPORTANT (F-NEW-1): The profile path must NOT be ":memory:". A persistent DuckDB file
  is required so that dbt can resolve cross-model dependencies during a single pipeline run.
  The file is written to the data/pipeline/ directory inside the bind-mounted volume.
  dbt models do NOT read or write via this DuckDB file for their Parquet output — they use
  read_parquet() source references and the location config to write directly to the
  data/silver/ and data/gold/ Parquet paths. The DuckDB file is the dbt catalog/metadata
  store only."
  dbt_project/models/silver/silver_transaction_codes.sql — empty stub (SELECT 1 AS placeholder)
  dbt_project/models/silver/silver_accounts.sql — empty stub
  dbt_project/models/silver/silver_transactions.sql — empty stub
  dbt_project/models/silver/silver_quarantine.sql — empty stub
  dbt_project/models/gold/gold_daily_summary.sql — empty stub
  dbt_project/models/gold/gold_weekly_account_summary.sql — empty stub

Use full paths from repo root for all file references. Do not create any files not listed above.
```

**Test cases:**

| Case | Scenario | Expected |
|---|---|---|
| TC-1 | All listed directories exist | `find . -type d` shows all 12 directories |
| TC-2 | All listed files exist | `find . -type f` shows all expected files |
| TC-3 | .gitignore excludes .env and data/ | `cat .gitignore` contains both entries |
| TC-4 | dbt_project.yml is valid YAML | `python -c "import yaml; yaml.safe_load(open('dbt_project/dbt_project.yml'))"` exits 0 |
| TC-5 (F-NEW-1) | vars.data_dir present in dbt_project.yml | `grep -q "data_dir" dbt_project/dbt_project.yml` exits 0 |
| TC-6 (F-NEW-1) | profiles.yml path is not :memory: | `grep -q ":memory:" dbt_project/profiles.yml` exits non-zero |

**Verification command:**
```bash
find . -type d | sort && find . -type f | grep -v __pycache__ | sort
```

**Invariant enforcement:** None — scaffold task only.

**Regression classification:** NOT-REGRESSION-RELEVANT — directory structure; subsequent tasks verify presence implicitly.

---

### Task 1.2 — Dockerfile and Docker Compose

**Description:** Create a working Dockerfile and docker-compose.yml that build a Python 3.11 container with DuckDB and dbt-duckdb installed. Container mounts data/ and source/ from the host.

**CC Prompt:**
```
Create the following two files at repo root. Use full paths from repo root.

File 1: Dockerfile

  FROM python:3.11-slim
  WORKDIR /app
  RUN pip install duckdb==0.10.0 dbt-core==1.7.0 dbt-duckdb==1.7.0 python-dotenv pandas pyarrow
  COPY . /app
  RUN chown -R 1000:1000 /app
  USER 1000:1000
  CMD ["python", "pipeline.py"]

CRITICAL ordering rules (F7):
  - RUN pip install must execute as root (before USER). Placing USER before pip install
    causes permission failures writing to system site-packages — the image will build
    but imports will fail at runtime.
  - COPY before chown ensures all app files are owned by UID 1000 before the USER switch.
  - USER 1000:1000 is the final instruction before CMD — the process runs as non-root.
  - The bind-mounted data/ and source/ directories on the host must be owned by UID 1000
    or be world-writable. Add a note in README.md:
      "Run `sudo chown -R 1000:1000 data/ source/` on the host before first run
       if you encounter permission errors writing to /data or /source."

File 2: docker-compose.yml
  version: "3.8"
  services:
    pipeline:
      build: .
      env_file: .env
      volumes:
        - ./data:/data
        - ./source:/source
        - ./dbt_project:/app/dbt_project
      working_dir: /app

Do not create any other files.
```

**Test cases:**

| Case | Scenario | Expected |
|---|---|---|
| TC-1 | Docker image builds successfully | `docker compose build` exits 0 |
| TC-2 | Container starts and python is available | `docker compose run --rm pipeline python --version` shows Python 3.11.x |
| TC-3 | duckdb importable in container | `docker compose run --rm pipeline python -c "import duckdb; print(duckdb.__version__)"` exits 0 |
| TC-4 | dbt available in container | `docker compose run --rm pipeline dbt --version` exits 0 |
| TC-5 (F7) | Container process runs as UID 1000 | `docker compose run --rm pipeline id -u` returns `1000` |
| TC-6 (F7) | Container can write to bind-mounted /data | `docker compose run --rm pipeline bash -c "touch /data/.write_test && rm /data/.write_test"` exits 0 |

**Verification command:**
```bash
docker compose build 2>&1 | tail -5
docker compose run --rm pipeline python -c "import duckdb, dbt; print('OK')"
# F7 — UID verification
docker compose run --rm pipeline id -u
# F7 — write permission verification (host data/ must be owned/writable by UID 1000)
docker compose run --rm pipeline bash -c "touch /data/.write_test && rm /data/.write_test && echo 'Write permission PASS'"
```

**Invariant enforcement:** None — infrastructure task. F7 note: incorrect USER ordering leaves Bronze partitions written to disk on a mid-run permission failure while the watermark is not advanced. This creates a state inconsistency that the existence-check idempotency logic (INV-07) then silently skips on re-run. Correct Dockerfile ordering eliminates this failure mode at its root.

**Regression classification:** NOT-REGRESSION-RELEVANT — Docker build; environment concern, not data pipeline logic.

---

### Task 1.3 — Environment Configuration and Startup Validation Module

**Description:** Create `config.py` — a module that loads and validates all `.env` values before any pipeline work begins. Returns a validated config object or exits with a clear error.

**CC Prompt:**
```
Create config.py at repo root (/app/config.py in the container).

This module must:
1. Load all environment variables from .env using python-dotenv.
2. Validate PIPELINE_MODE — must be "historical" or "incremental". Exit with code 1 and
   a clear message if invalid or missing.
3. If PIPELINE_MODE = "historical":
   - Validate START_DATE and END_DATE are present, are valid dates in YYYY-MM-DD format,
     and START_DATE <= END_DATE. Exit with code 1 and a clear message if any check fails.
4. If PIPELINE_MODE = "incremental":
   - Do not read START_DATE or END_DATE.
   - Record that watermark validation is deferred to pipeline startup (control table
     must exist — checked in pipeline.py, not here).
5. Validate DATA_DIR and SOURCE_DIR are set. Exit with code 1 if missing.
6. Return a dataclass named PipelineConfig with fields:
   - mode: str
   - data_dir: str
   - source_dir: str
   - start_date: date | None
   - end_date: date | None
7. Expose a single function: load_config() -> PipelineConfig

8. (R3) Delete the dbt catalog file if it exists before returning:
   Path(f"{data_dir}/pipeline/dbt_catalog.duckdb").unlink(missing_ok=True)
   This must execute after all validation passes and before load_config() returns.
   Rationale: dbt-duckdb uses a persistent DuckDB catalog file for cross-model dependency
   resolution. A stale catalog from a prior run can cause dbt to resolve models against
   out-of-date state. Deleting it at startup ensures each pipeline run starts with a clean
   catalog. This is safe because the catalog contains only dbt metadata — all data is in
   the Parquet files under data/silver/ and data/gold/ which are not touched by this step.
   The missing_ok=True flag means the deletion is a no-op on first run when the file does
   not yet exist.

Use the dataclasses module. Use pathlib where appropriate.
Use full paths from repo root for all imports and file references.
Do not create any other files.
```

**Test cases:**

| Case | Scenario | Expected |
|---|---|---|
| TC-1 | Valid historical config | `load_config()` returns PipelineConfig with mode="historical", start_date and end_date populated |
| TC-2 | Missing PIPELINE_MODE | `load_config()` exits with code 1 and prints a message containing "PIPELINE_MODE" |
| TC-3 | Invalid PIPELINE_MODE value | `load_config()` exits with code 1 |
| TC-4 | historical mode with END_DATE before START_DATE | exits with code 1 |
| TC-5 | Valid incremental config | returns PipelineConfig with mode="incremental", start_date=None, end_date=None |
| TC-6 (R3) | dbt_catalog.duckdb present at startup | file deleted before load_config() returns; absent file produces no error |

**Verification command:**
```bash
# TC-1
PIPELINE_MODE=historical START_DATE=2024-01-01 END_DATE=2024-01-07 DATA_DIR=/data SOURCE_DIR=/source python -c "from config import load_config; c = load_config(); assert c.mode == 'historical'; print('TC-1 PASS')"
# TC-2
python -c "from config import load_config; load_config()" 2>&1; echo "exit: $?"
# TC-4
PIPELINE_MODE=historical START_DATE=2024-01-07 END_DATE=2024-01-01 DATA_DIR=/data SOURCE_DIR=/source python -c "from config import load_config; load_config()" 2>&1; echo "exit: $?"
# TC-6 (R3) — stale catalog deleted at startup
python -c "
import os, tempfile, pathlib
os.environ['PIPELINE_MODE']='historical'
os.environ['START_DATE']='2024-01-01'
os.environ['END_DATE']='2024-01-07'
os.environ['SOURCE_DIR']='source'
with tempfile.TemporaryDirectory() as tmp:
    os.environ['DATA_DIR']=tmp
    os.makedirs(f'{tmp}/pipeline', exist_ok=True)
    stale = pathlib.Path(f'{tmp}/pipeline/dbt_catalog.duckdb')
    stale.touch()
    from config import load_config
    load_config()
    assert not stale.exists(), 'TC-6 FAIL — dbt_catalog.duckdb not deleted'
    print('TC-6 PASS')
"
```

**Invariant enforcement:**
- INV-08 (Atomic Pipeline Execution): Startup validation must exit with non-zero code before any phase function runs if configuration is invalid. `load_config()` must use `sys.exit(1)` on any validation failure — not raise an exception that a caller might catch.
- R3: dbt_catalog.duckdb deletion executes unconditionally at the end of load_config(), after all validation passes. It is a startup guard, not a phase function — it runs before any Bronze, Silver, or Gold work begins.

**Regression classification:** REGRESSION-RELEVANT — startup validation is a core guard; portable command above.

---

### Task 1.4 — Source File Pre-flight Check

**Description:** Add source file existence validation to `pipeline.py`. Before any phase function runs, check that all required source files for the target date or date range are present.

**CC Prompt:**
```
Update pipeline.py at repo root.

Add a function: validate_source_files(config: PipelineConfig) -> None

This function must:
1. Import PipelineConfig from config.py.
2. For historical mode:
   - Iterate every date from config.start_date to config.end_date inclusive.
   - For each date, check that the following files exist under config.source_dir:
       transactions_YYYY-MM-DD.csv
       accounts_YYYY-MM-DD.csv
   - Also check that transaction_codes.csv exists under config.source_dir (once, not per date).
3. For incremental mode:
   - Determine the next date as watermark + 1 day. The watermark is read from
     {config.data_dir}/pipeline/control.parquet using DuckDB.
     If control.parquet does not exist, print a clear error and call sys.exit(1).
   - Check that the following files exist under config.source_dir for the next date:
       transactions_YYYY-MM-DD.csv
       accounts_YYYY-MM-DD.csv
4. If any required file is absent:
   - Print a clear error message listing every missing file.
   - Call sys.exit(1).
5. If all files are present: print "Source file pre-flight: PASS" and return.

Update the main() function stub to:
  - Call load_config() from config.py
  - Call validate_source_files(config)
  - Print "Startup validation complete. Pipeline mode: {config.mode}"
  - Then return (remaining pipeline phases are stubs for now)

Use pathlib.Path.exists() for all file existence checks — read mode only, no open().
Use full paths from repo root for all file references. Do not modify config.py.
Do not create any other files.
```

**Test cases:**

| Case | Scenario | Expected |
|---|---|---|
| TC-1 | All source files present (historical, 7-day range) | prints "Source file pre-flight: PASS", exits 0 |
| TC-2 | One transactions file missing | prints the missing filename, exits 1 |
| TC-3 | transaction_codes.csv missing | prints "transaction_codes.csv", exits 1 |
| TC-4 | Incremental mode, control.parquet absent | prints clear error about control table, exits 1 |

**Verification command:**
```bash
# TC-1 (run from container with source/ mounted)
docker compose run --rm pipeline python pipeline.py
# TC-2
docker compose run --rm pipeline bash -c "mv /source/transactions_2024-01-03.csv /tmp/ && python pipeline.py; mv /tmp/transactions_2024-01-03.csv /source/"
```

**Invariant enforcement:**
- INV-08: Source file presence is a precondition of pipeline execution. Missing file triggers sys.exit(1) before any phase function is invoked.
- INV-13: validate_source_files reads source files in existence-check mode only — no open(), no read(). Use pathlib.Path.exists().

**Regression classification:** REGRESSION-RELEVANT — source file pre-flight is a named invariant enforcement point (INV-08).

---

### Task 1.5 — Run Log and Control Table Initialisation Helpers

**Description:** Create `lake_io.py` — a module with low-level read/write helpers for `pipeline/control.parquet` and `pipeline/run_log.parquet`. These helpers are called by `pipeline.py` throughout sessions 2–5.

**CC Prompt:**
```
Create lake_io.py at repo root.

This module provides four functions for reading and writing pipeline state files.
All paths are constructed from a data_dir parameter — never hardcoded.

Function 1: read_watermark(data_dir: str) -> date | None
  - Read {data_dir}/pipeline/control.parquet using DuckDB.
  - Return last_processed_date as a Python date object.
  - If the file does not exist, return None.

Function 2: write_watermark(data_dir: str, processed_date: date, run_id: str) -> None
  - Write a single-row Parquet file to {data_dir}/pipeline/control.parquet with columns:
      last_processed_date: DATE
      updated_at: TIMESTAMP (current UTC time)
      updated_by_run_id: STRING
  - This is a full overwrite of the control file — the file is single-row by design.

Function 3: append_run_log(data_dir: str, row: dict) -> None
  - If {data_dir}/pipeline/run_log.parquet does not exist: create it with the row as the only record.
  - If it exists: read existing records, append the new row, write the combined result.
  - Column schema (all must be present — use None for nullable fields):
      run_id: STRING
      pipeline_type: STRING  (HISTORICAL or INCREMENTAL)
      model_name: STRING
      layer: STRING  (BRONZE, SILVER, or GOLD)
      started_at: TIMESTAMP
      completed_at: TIMESTAMP
      status: STRING  (SUCCESS, FAILED, or SKIPPED)
      records_processed: INTEGER
      records_written: INTEGER
      records_rejected: INTEGER  (nullable)
      error_message: STRING  (nullable)
  - INV-06 enforcement: this function must never truncate or overwrite existing rows.
    Read → append → write is the only permitted write pattern.

Function 4: run_log_exists(data_dir: str) -> bool
  - Return True if {data_dir}/pipeline/run_log.parquet exists and has at least one row.

Function 5: sanitise_error_message(raw: str) -> str  (F-NEW-3)
  - Called by append_run_log before writing the error_message field. Never call
    append_run_log with a raw exception string or traceback directly.
  - Rules:
      a. Replace any filesystem path (absolute or relative, starting with / or ./ or ../)
         with the literal string [path redacted]. Use a regex: r'(/|\./|\.\./)[\w./\-]+'
      b. Truncate the result to 500 characters maximum.
      c. Never write Python exception tracebacks. If the input contains the string
         "Traceback (most recent call last)" strip everything from that point onward
         before applying rule (a).
      d. If the sanitised result is empty after all rules, write "error detail redacted".
  - Returns the sanitised string.
  - append_run_log must call sanitise_error_message on any non-None error_message value
    before writing. This applies to every call site — callers do NOT pre-sanitise.

Use DuckDB for all Parquet reads and writes.
Import date and datetime from the datetime module.
Use full paths from repo root. Do not create any other files.
```

**Test cases:**

| Case | Scenario | Expected |
|---|---|---|
| TC-1 | write_watermark then read_watermark | read returns the same date that was written |
| TC-2 | append_run_log to non-existent file | file created with 1 row |
| TC-3 | append_run_log twice | file has 2 rows, first row unchanged |
| TC-4 | read_watermark on non-existent file | returns None |
| TC-5 | append_run_log never truncates | after 3 appends, row count = 3 |
| TC-6 (F-NEW-3) | sanitise_error_message strips filesystem path | input "/data/bronze/tx.parquet not found" → output contains "[path redacted]", not the path |
| TC-7 (F-NEW-3) | sanitise_error_message strips traceback | input containing "Traceback (most recent call last)" → traceback content absent in output |
| TC-8 (F-NEW-3) | sanitise_error_message truncates to 500 chars | 600-char input → output length <= 500 |
| TC-9 (F-NEW-3) | append_run_log auto-sanitises error_message | raw path string passed as error_message → stored value contains [path redacted] |

**Verification command:**
```bash
python -c "
import os, tempfile, datetime
from lake_io import read_watermark, write_watermark, append_run_log

with tempfile.TemporaryDirectory() as tmp:
    os.makedirs(f'{tmp}/pipeline')
    d = datetime.date(2024, 1, 7)
    write_watermark(tmp, d, 'run-001')
    assert read_watermark(tmp) == d, 'TC-1 FAIL'
    print('TC-1 PASS')

    row = dict(run_id='r1', pipeline_type='HISTORICAL', model_name='bronze_transactions',
               layer='BRONZE', started_at=datetime.datetime.utcnow(),
               completed_at=datetime.datetime.utcnow(), status='SUCCESS',
               records_processed=10, records_written=10, records_rejected=None, error_message=None)
    append_run_log(tmp, row)
    append_run_log(tmp, {**row, 'run_id': 'r2'})
    append_run_log(tmp, {**row, 'run_id': 'r3'})
    import duckdb
    count = duckdb.execute(f\"SELECT COUNT(*) FROM '{tmp}/pipeline/run_log.parquet'\").fetchone()[0]
    assert count == 3, f'TC-3/TC-5 FAIL: got {count}'
    print('TC-3 and TC-5 PASS')
"
```

**Invariant enforcement:**
- INV-06 (Run Log Append-Only — GLOBAL): `append_run_log` must use read → append → write pattern. No truncate, no overwrite, no DELETE. This is never negotiable.
- F-NEW-3: `sanitise_error_message` is called inside `append_run_log` on every non-None error_message before write. No caller bypasses this. No filesystem path or traceback is ever stored in run_log.parquet.

**Regression classification:** REGRESSION-RELEVANT — INV-06 GLOBAL invariant; F-NEW-3 error_message sanitisation.

---

## Session 2 — Bronze Loader

**Session goal:** All three Bronze loaders operational. Running the historical pipeline loads all source CSVs to Bronze Parquet with correct audit columns, correct partition paths, and idempotency.

**Integration check:**
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

### Task 2.1 — Bronze Transaction Codes Loader

**Description:** Implement `bronze_loaders.py` with a function that loads `transaction_codes.csv` to Bronze. This is the first loader because transaction codes must exist in Bronze before any transaction processing.

**CC Prompt:**
```
Create bronze_loaders.py at repo root.

Implement function: load_bronze_transaction_codes(config: PipelineConfig, run_id: str) -> dict

This function must:
1. Check if {config.data_dir}/bronze/transaction_codes/data.parquet already exists.
   If it does: print "Bronze transaction_codes already loaded — skipping." Return:
     {"status": "SKIPPED", "records_processed": 0, "records_written": 0}
2. If it does not exist:
   a. Read {config.source_dir}/transaction_codes.csv using DuckDB.
   b. Add these three audit columns to every row:
      _source_file: STRING — set to "transaction_codes.csv"
      _ingested_at: TIMESTAMP — set to current UTC timestamp at time of ingestion
      _pipeline_run_id: STRING — set to the run_id parameter
   c. Write the result to {config.data_dir}/bronze/transaction_codes/data.parquet using DuckDB.
   d. (F2) Verify the written file is readable and its row count equals the source CSV row count.
      Perform this check by reading the written Parquet file back with DuckDB after writing:
        SELECT COUNT(*) FROM '{written_path}'
      If the read fails (file corrupt/unreadable) or count differs from source: print an error
      and call sys.exit(1). Do NOT rely on the DuckDB write operation returning without error
      as proof of file integrity — always verify by re-reading.
   e. (F3) If the source CSV row count is 0:
      - Still write the zero-row Parquet file (preserves partition existence for idempotency).
      - Append a WARNING run log entry (status="WARNING") with error_message=
        "transaction_codes.csv contained 0 rows — Bronze partition written but empty.
         Analyst review required."
      - Return: {"status": "WARNING", "records_processed": 0, "records_written": 0}
   f. Return on success: {"status": "SUCCESS", "records_processed": <source_row_count>, "records_written": <verified_row_count>}

Rules:
- Use DuckDB for all reads and writes.
- All three audit columns must be non-null on every written record.
- Do not apply any filtering, transformation, or deduplication to source records.
- Do not write to any path outside {config.data_dir}/bronze/.
- Use full paths from repo root for all file references.
- Do not create any other files.
```

**Test cases:**

| Case | Scenario | Expected |
|---|---|---|
| TC-1 | First run — transaction_codes.csv present | data.parquet written, row count matches CSV, all audit columns non-null |
| TC-2 | Second run — partition already exists | prints skip message, returns SKIPPED, existing file untouched |
| TC-3 | All audit columns non-null | `SELECT COUNT(*) FROM data.parquet WHERE _pipeline_run_id IS NULL` = 0 |
| TC-4 | _source_file value | all rows have _source_file = "transaction_codes.csv" |
| TC-5 (F2) | Written Parquet is re-read for integrity | verification SELECT runs after write; corrupt file triggers sys.exit(1) |
| TC-6 (F3) | Empty transaction_codes.csv | returns WARNING, zero-row parquet written, WARNING run log entry appended |

**Verification command:**
```bash
python -c "
import duckdb, os, tempfile
from config import PipelineConfig
from bronze_loaders import load_bronze_transaction_codes
import datetime

with tempfile.TemporaryDirectory() as tmp:
    os.makedirs(f'{tmp}/bronze/transaction_codes')
    cfg = PipelineConfig(mode='historical', data_dir=tmp, source_dir='source',
                         start_date=datetime.date(2024,1,1), end_date=datetime.date(2024,1,7))
    result = load_bronze_transaction_codes(cfg, 'run-test-001')
    assert result['status'] == 'SUCCESS', f'TC-1 FAIL: {result}'
    # F2 — verify parquet is readable post-write
    count = duckdb.execute(f\"SELECT COUNT(*) FROM '{tmp}/bronze/transaction_codes/data.parquet'\").fetchone()[0]
    assert count == result['records_written'], 'TC-5 (F2) FAIL — count mismatch after re-read'
    null_count = duckdb.execute(f\"SELECT COUNT(*) FROM '{tmp}/bronze/transaction_codes/data.parquet' WHERE _pipeline_run_id IS NULL\").fetchone()[0]
    assert null_count == 0, 'TC-3 FAIL'
    result2 = load_bronze_transaction_codes(cfg, 'run-test-002')
    assert result2['status'] == 'SKIPPED', 'TC-2 FAIL'
    print('All TC PASS')
"
```

**Invariant enforcement:**
- INV-05: All three audit columns (_source_file, _ingested_at, _pipeline_run_id) must be non-null on every written record. This is never negotiable.
- INV-07: If Bronze partition already exists, skip entirely — no read, no write, no dedup. This is never negotiable.
- INV-13: Source CSV opened in read mode only via DuckDB SELECT. No write operations on any path under source/.
- INV-11: Bronze ingestion implemented in Python + DuckDB only. No dbt reference here.
- F2: Readability verification re-read is mandatory — existence of the file alone is not sufficient proof of integrity.
- F3: Zero-row source file must produce a WARNING run log entry — silent success on empty input is a conservation equation violation risk.

**Regression classification:** REGRESSION-RELEVANT — INV-05, INV-07, INV-13 enforcement; F2 readability check; F3 empty-source warning.

---

### Task 2.2 — Bronze Accounts Loader

**Description:** Add `load_bronze_accounts(config, date, run_id)` to `bronze_loaders.py`. Loads a single day's accounts delta CSV to its Bronze partition.

**CC Prompt:**
```
Update bronze_loaders.py at repo root. Add the following function.

Implement function: load_bronze_accounts(config: PipelineConfig, date: datetime.date, run_id: str) -> dict

This function must:
1. Determine the partition path: {config.data_dir}/bronze/accounts/date={date.strftime('%Y-%m-%d')}/data.parquet
2. Check if the partition path already exists.
   If it does: print "Bronze accounts {date} already loaded — skipping." Return:
     {"status": "SKIPPED", "records_processed": 0, "records_written": 0}
3. If it does not exist:
   a. Determine source file: {config.source_dir}/accounts_{date.strftime('%Y-%m-%d')}.csv
   b. Read the source CSV using DuckDB.
   c. Add three audit columns to every row:
      _source_file: STRING — set to "accounts_{date.strftime('%Y-%m-%d')}.csv"
      _ingested_at: TIMESTAMP — current UTC timestamp at ingestion time
      _pipeline_run_id: STRING — set to the run_id parameter
   d. Write to the partition path using DuckDB.
   e. (F2) Verify the written partition is readable and its row count equals the source CSV row count.
      Re-read the written Parquet file with DuckDB after writing:
        SELECT COUNT(*) FROM '{partition_path}'
      If the read fails or count differs: print an error and call sys.exit(1).
      Do NOT rely on the DuckDB write returning without error as proof of file integrity.
   f. (F3) If the source CSV row count is 0:
      - Still write the zero-row Parquet partition (preserves partition existence for idempotency).
      - Append a WARNING run log entry with error_message=
        "accounts_{date}.csv contained 0 rows — Bronze partition written but empty.
         Analyst review required."
      - Return: {"status": "WARNING", "records_processed": 0, "records_written": 0}
   g. Return on success: {"status": "SUCCESS", "records_processed": <source_count>, "records_written": <verified_count>}

Rules: no filtering, no transformation, no dedup. All three audit columns non-null.
Write only to {config.data_dir}/bronze/accounts/ paths.
Use full paths from repo root. Do not create any other files.
```

**Test cases:**

| Case | Scenario | Expected |
|---|---|---|
| TC-1 | First load for 2024-01-01 | partition written at correct path, row count matches CSV |
| TC-2 | Second load for same date | returns SKIPPED, partition unchanged |
| TC-3 | All audit columns non-null | zero null _pipeline_run_id rows |
| TC-4 | _source_file matches date | "accounts_2024-01-01.csv" for date 2024-01-01 |
| TC-5 (F2) | Written Parquet re-read for integrity | verification SELECT runs after write; mismatch triggers sys.exit(1) |
| TC-6 (F3) | Empty accounts CSV for a date | returns WARNING, zero-row partition written, WARNING run log entry appended |

**Verification command:**
```bash
python -c "
import duckdb, os, tempfile, datetime
from config import PipelineConfig
from bronze_loaders import load_bronze_accounts

with tempfile.TemporaryDirectory() as tmp:
    os.makedirs(f'{tmp}/bronze/accounts')
    cfg = PipelineConfig(mode='historical', data_dir=tmp, source_dir='source',
                         start_date=datetime.date(2024,1,1), end_date=datetime.date(2024,1,7))
    d = datetime.date(2024, 1, 1)
    r = load_bronze_accounts(cfg, d, 'run-001')
    assert r['status'] == 'SUCCESS', f'TC-1 FAIL: {r}'
    part = f'{tmp}/bronze/accounts/date=2024-01-01/data.parquet'
    assert os.path.exists(part), 'TC-1 FAIL — partition not created'
    # F2 — re-read integrity check
    count = duckdb.execute(f\"SELECT COUNT(*) FROM '{part}\'").fetchone()[0]
    assert count == r['records_written'], 'TC-5 (F2) FAIL — count mismatch after re-read'
    null_count = duckdb.execute(f\"SELECT COUNT(*) FROM '{part}' WHERE _pipeline_run_id IS NULL\").fetchone()[0]
    assert null_count == 0, 'TC-3 FAIL'
    r2 = load_bronze_accounts(cfg, d, 'run-002')
    assert r2['status'] == 'SKIPPED', 'TC-2 FAIL'
    print('All TC PASS')
"
```

**Invariant enforcement:**
- INV-05: All three audit columns non-null. This is never negotiable.
- INV-07: Partition existence check is unconditional. Skip if present. This is never negotiable.
- INV-13: Source CSV opened read-only.
- F2: Re-read integrity check is mandatory. Corrupt partition silently skipped on re-run (INV-07) causes conservation gap.
- F3: Zero-row source file must produce a WARNING run log entry — silent success on empty input masks a potential data gap from the analyst.

**Regression classification:** REGRESSION-RELEVANT — INV-05, INV-07; F2 readability check; F3 empty-source warning.

---

### Task 2.3 — Bronze Transactions Loader

**Description:** Add `load_bronze_transactions(config, date, run_id)` to `bronze_loaders.py`. Identical pattern to accounts loader but for daily transaction files.

**CC Prompt:**
```
Update bronze_loaders.py at repo root. Add the following function.

Implement function: load_bronze_transactions(config: PipelineConfig, date: datetime.date, run_id: str) -> dict

This function must follow the exact same pattern as load_bronze_accounts, with these differences:
- Partition path: {config.data_dir}/bronze/transactions/date={date.strftime('%Y-%m-%d')}/data.parquet
- Source file: {config.source_dir}/transactions_{date.strftime('%Y-%m-%d')}.csv
- _source_file audit column: "transactions_{date.strftime('%Y-%m-%d')}.csv"
- Skip message: "Bronze transactions {date} already loaded — skipping."

All other rules are identical: no filtering, no transformation, no dedup, all audit
columns non-null, write only to data/bronze/transactions/ paths.

Additional rules from findings (F2, F3 — same as Task 2.2):
- (F2) After writing each partition, re-read with DuckDB and verify row count matches
  source CSV row count. If read fails or count differs: sys.exit(1).
- (F3) If source CSV row count is 0: write zero-row partition, append WARNING run log
  entry with error_message="transactions_{date}.csv contained 0 rows — Bronze partition
  written but empty. Analyst review required.", return {"status": "WARNING", ...}.

Use full paths from repo root. Do not create any other files.
```

**Test cases:**

| Case | Scenario | Expected |
|---|---|---|
| TC-1 | First load for 2024-01-01 | partition at correct path, row count matches CSV |
| TC-2 | Second load for same date | SKIPPED, partition unchanged |
| TC-3 | All audit columns non-null | zero null _pipeline_run_id rows |
| TC-4 | 7-day historical load | 7 partitions created under bronze/transactions/ |
| TC-5 (F2) | Written Parquet re-read for integrity | verification SELECT runs after each write; mismatch triggers sys.exit(1) |
| TC-6 (F3) | Empty transactions CSV for a date | returns WARNING, zero-row partition written, WARNING run log entry appended |

**Verification command:**
```bash
python -c "
import duckdb, os, tempfile, datetime
from config import PipelineConfig
from bronze_loaders import load_bronze_transactions

with tempfile.TemporaryDirectory() as tmp:
    os.makedirs(f'{tmp}/bronze/transactions')
    cfg = PipelineConfig(mode='historical', data_dir=tmp, source_dir='source',
                         start_date=datetime.date(2024,1,1), end_date=datetime.date(2024,1,7))
    for i in range(7):
        d = datetime.date(2024, 1, 1) + datetime.timedelta(days=i)
        r = load_bronze_transactions(cfg, d, f'run-00{i+1}')
        assert r['status'] in ('SUCCESS', 'WARNING'), f'Day {d} FAIL: {r}'
        part = f'{tmp}/bronze/transactions/date={d}/data.parquet'
        # F2 — re-read integrity check
        count = duckdb.execute(f\"SELECT COUNT(*) FROM '{part}'\").fetchone()[0]
        assert count == r['records_written'], f'TC-5 (F2) FAIL day {d}'
    parts = [p for p in os.listdir(f'{tmp}/bronze/transactions') if p.startswith('date=')]
    assert len(parts) == 7, f'TC-4 FAIL: {len(parts)} partitions'
    r2 = load_bronze_transactions(cfg, datetime.date(2024,1,1), 'run-rerun')
    assert r2['status'] == 'SKIPPED', 'TC-2 FAIL'
    print('All TC PASS')
"
```

**Invariant enforcement:**
- INV-05, INV-07, INV-13: Same as Task 2.2.
- F2: Re-read integrity check mandatory on every partition write. Corrupt partition silently skipped on re-run produces a conservation gap that is invisible to the run log.
- F3: Zero-row source file must produce a WARNING run log entry. Silent success on an empty day's transactions is an analyst blind spot.

**Regression classification:** REGRESSION-RELEVANT — INV-05, INV-07; F2 readability check; F3 empty-source warning.

---

### Task 2.4 — Bronze Phase Function

**Description:** Add `run_bronze_phase(config, run_id)` to `pipeline.py`. Sequences the three Bronze loaders in correct order and returns a `PhaseResult`.

**CC Prompt:**
```
Update pipeline.py at repo root.

Add a dataclass PhaseResult with fields:
  success: bool
  records_processed: int
  records_written: int
  error: str | None

Add function: run_bronze_phase(config: PipelineConfig, run_id: str) -> PhaseResult

For historical mode this function must:
1. Call load_bronze_transaction_codes(config, run_id). Append a run log entry via append_run_log.
2. For each date from config.start_date to config.end_date inclusive, in date order:
   a. Call load_bronze_accounts(config, date, run_id). Append run log entry.
   b. Call load_bronze_transactions(config, date, run_id). Append run log entry.
3. If any loader call raises an exception or returns status="FAILED":
   - Append a run log entry with status=FAILED and the error_message.
   - Return PhaseResult(success=False, ..., error=<message>).
4. If all loaders succeed or are SKIPPED:
   - Return PhaseResult(success=True, records_processed=<total>, records_written=<total>, error=None).

For incremental mode this function must:
1. Determine next_date = watermark + 1 day (read via read_watermark from lake_io).
2. Call load_bronze_transaction_codes (will SKIP as reference already loaded).
3. Call load_bronze_accounts(config, next_date, run_id). Append run log entry.
4. Call load_bronze_transactions(config, next_date, run_id). Append run log entry.
5. Return PhaseResult per same success/failure logic as historical.

layer = "BRONZE" for all run log entries.
started_at = timestamp before loader call. completed_at = timestamp after.

Update main() to call run_bronze_phase after startup validation and print the result.

Use full paths from repo root. Do not create any other files.
```

**Test cases:**

| Case | Scenario | Expected |
|---|---|---|
| TC-1 | Historical 7-day run | PhaseResult(success=True), 15 run log entries (1 tx_codes + 7 accounts + 7 transactions) |
| TC-2 | Re-run historical | All SKIPPED, PhaseResult(success=True), no new Bronze partitions |
| TC-3 | One loader raises exception | PhaseResult(success=False), run log entry with status=FAILED |

**Verification command:**
```bash
docker compose run --rm pipeline python pipeline.py
docker compose run --rm pipeline python -c "
import duckdb
count = duckdb.execute(\"SELECT COUNT(*) FROM '/data/pipeline/run_log.parquet'\").fetchone()[0]
failed = duckdb.execute(\"SELECT COUNT(*) FROM '/data/pipeline/run_log.parquet' WHERE status='FAILED'\").fetchone()[0]
print(f'Run log entries: {count}, failed: {failed}')
assert failed == 0, 'FAIL entries in run log'
"
```

**Invariant enforcement:**
- INV-06: append_run_log used for all run log writes — never overwrite.
- INV-08: If any loader returns failure, run_bronze_phase returns PhaseResult(success=False). The caller (main) must not invoke run_silver_phase.

**Regression classification:** REGRESSION-RELEVANT — orchestration gating is INV-08.

---

## Session 3 — Silver dbt Models

**Session goal:** All four Silver dbt models operational and producing correct, validated output from Bronze. Quarantine populated for invalid records. Conservation equation holds.

**Integration check:**
```bash
# F4 PRE-CONDITION: Session 3 verification commands run dbt against actual Bronze Parquet
# fixture data, not in compile-only mode. Before running any S3 verification command,
# confirm Bronze partitions are present:
docker compose run --rm pipeline python -c "
import duckdb, sys
try:
    bronze_txn = duckdb.execute("SELECT COUNT(*) FROM read_parquet('/data/bronze/transactions/*/*.parquet')").fetchone()[0]
    assert bronze_txn > 0, 'Bronze transactions empty — run Session 2 before Session 3'
    print(f'Bronze fixture present: {bronze_txn} rows')
except Exception as e:
    print(f'FAIL — Bronze not loaded: {e}'); sys.exit(1)
"
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

### Task 3.1 — Silver Transaction Codes Model

**Description:** Implement `dbt_project/models/silver/silver_transaction_codes.sql`. Promotes transaction codes from Bronze to Silver reference file.

**CC Prompt:**
```
Replace the stub at dbt_project/models/silver/silver_transaction_codes.sql with a complete dbt model.

This model must:
1. Read from the Bronze transaction codes Parquet file using var('data_dir') (F-NEW-1):
   read_parquet('{{ var("data_dir") }}/bronze/transaction_codes/data.parquet')
   var('data_dir') is resolved from dbt_project.yml vars block, which in turn reads the
   DATA_DIR environment variable. Do NOT use env_var() directly in model SQL — use var().
2. Select all source columns plus carry forward audit columns from Bronze:
   _source_file, _bronze_ingested_at (renamed from _ingested_at in Bronze), _pipeline_run_id
3. Write output to: {{ var("data_dir") }}/silver/transaction_codes/data.parquet
   Use the location config to specify the output Parquet path directly on the filesystem.
   dbt-duckdb writes the materialized result to this path. The DuckDB catalog file in
   profiles.yml is the metadata store only — it does not contain the Parquet data.
4. No filtering — all Bronze records promoted. This reference has no rejection path.

Model config block at top:
  {{ config(materialized='table', location=var('data_dir') ~ '/silver/transaction_codes/data.parquet') }}

Create dbt_project/models/silver/schema.yml if it does not exist and add:
  - not_null tests for transaction_code, debit_credit_indicator, transaction_type, affects_balance
  - unique test for transaction_code

Use full paths from repo root. Do not modify any other files.
```

**Test cases:**

| Case | Scenario | Expected |
|---|---|---|
| TC-1 | dbt build executes without error | exit code 0 |
| TC-2 | Output row count matches Bronze | SELECT COUNT(*) Silver = SELECT COUNT(*) Bronze transaction_codes |
| TC-3 | transaction_code uniqueness | no duplicate transaction_codes in Silver |
| TC-4 | _source_file non-null | zero null _source_file rows |

**Verification command:**
```bash
docker compose run --rm pipeline bash -c "cd dbt_project && dbt build --select silver_transaction_codes"  # G4: dbt build runs model+tests atomically (F-NEW-2)
docker compose run --rm pipeline python -c "
import duckdb
silver = duckdb.execute(\"SELECT COUNT(*) FROM '/data/silver/transaction_codes/data.parquet'\").fetchone()[0]
bronze = duckdb.execute(\"SELECT COUNT(*) FROM '/data/bronze/transaction_codes/data.parquet'\").fetchone()[0]
assert silver == bronze, f'TC-2 FAIL: silver={silver} bronze={bronze}'
null_sf = duckdb.execute(\"SELECT COUNT(*) FROM '/data/silver/transaction_codes/data.parquet' WHERE _source_file IS NULL\").fetchone()[0]
assert null_sf == 0, 'TC-4 FAIL'
print('All TC PASS')
"
```

**Invariant enforcement:**
- INV-05: _source_file, _bronze_ingested_at, _pipeline_run_id non-null on all Silver records.
- INV-11: Model reads only from data/bronze/ paths. No reference to source/ directory.

**Regression classification:** REGRESSION-RELEVANT — INV-05, INV-11.

---

### Task 3.2 — Silver Accounts Model

**Description:** Implement `dbt_project/models/silver/silver_accounts.sql`. Upserts account delta records — latest record per account_id wins. Applies account quality rules.

**CC Prompt:**
```
Replace the stub at dbt_project/models/silver/silver_accounts.sql with a complete dbt model.

This model must:
1. Read from all Bronze accounts partitions:
   read_parquet('{{ var("data_dir") }}/bronze/accounts/*/*.parquet')  {# F-NEW-1: use var(), not env_var() directly #}
2. Rejection rules — records failing any check are excluded from this model (handled in silver_quarantine.sql).
   Valid means:
   a. account_id, open_date, credit_limit, current_balance, billing_cycle_start,
      billing_cycle_end, account_status are all non-null and non-empty string.
   b. account_status is one of: ACTIVE, SUSPENDED, CLOSED.
3. For valid records: upsert on account_id — keep only the latest record per account_id,
   determined by _ingested_at DESC from Bronze.
4. Silver audit columns:
   _source_file: carried forward from Bronze
   _bronze_ingested_at: renamed from Bronze _ingested_at
   _pipeline_run_id: run context
   _record_valid_from: timestamp when this version became current in Silver
5. Write as a non-partitioned table:
   location: var('data_dir') ~ '/silver/accounts/data.parquet'  {# F-NEW-1 #}

Add dbt schema tests in schema.yml:
  - unique + not_null on account_id
  - not_null on _pipeline_run_id
  - not_null on _record_valid_from  {# G5: INV-05 update per ARCHITECTURE v1.2 #}

_record_valid_from population rule (G5):
  On initial insert: set to _ingested_at from Bronze.
  On upsert (existing account_id): set to current pipeline promotion timestamp.
  This column records when this version became current in Silver — it does NOT
  reconstruct historical account state. This is consistent with the SCD Type 2
  deferral documented in ARCHITECTURE.md Section 7.

Conservation check (INV-15): after writing Silver accounts, the sum of records
promoted to Silver (per date partition) plus records written to quarantine for
that date must equal the Bronze accounts row count for that date. This model
is responsible for the promotion side; silver_quarantine.sql covers the
rejection side. Post-model verification must confirm the equation holds.

Use full paths from repo root. Do not modify any other files.
```

**Test cases:**

| Case | Scenario | Expected |
|---|---|---|
| TC-1 | Valid accounts promoted | Silver accounts row count = distinct valid account_ids across all Bronze partitions |
| TC-2 | account_id uniqueness | SELECT COUNT(DISTINCT account_id) = SELECT COUNT(*) from Silver accounts |
| TC-3 | Invalid account_status excluded | record with status "INVALID" not in Silver accounts |
| TC-4 | Null required field excluded | record with null open_date not in Silver accounts |
| TC-5 | Latest record wins on upsert | if account_id appears twice, Silver has the one with later _ingested_at |
| TC-6 (G5) | _record_valid_from non-null | zero null _record_valid_from rows in Silver accounts |
| TC-7 (INV-15) | Account conservation per date | Bronze accounts row count for each date = Silver upserted (distinct account_id from that date) + Quarantine rejected for that date |

**Verification command:**
```bash
docker compose run --rm pipeline bash -c "cd dbt_project && dbt build --select silver_accounts"  # F-NEW-2: dbt build runs model+tests atomically
docker compose run --rm pipeline python -c "
import duckdb
total = duckdb.execute(\"SELECT COUNT(*) FROM '/data/silver/accounts/data.parquet'\").fetchone()[0]
unique_ids = duckdb.execute(\"SELECT COUNT(DISTINCT account_id) FROM '/data/silver/accounts/data.parquet'\").fetchone()[0]
assert total == unique_ids, f'TC-2 FAIL: {total} rows, {unique_ids} distinct ids'
print(f'TC-2 PASS: {total} accounts, all unique')
null_rvf = duckdb.execute(\"SELECT COUNT(*) FROM '/data/silver/accounts/data.parquet' WHERE _record_valid_from IS NULL\").fetchone()[0]
assert null_rvf == 0, f'TC-6 FAIL: {null_rvf} null _record_valid_from rows'
print('TC-6 PASS')
# INV-15 conservation check per date partition
for i in range(1, 8):
    d = f'2024-01-0{i}'
    bronze = duckdb.execute(f\"SELECT COUNT(*) FROM '/data/bronze/accounts/date={d}/data.parquet'\").fetchone()[0]
    try:
        quarantine = duckdb.execute(f\"SELECT COUNT(*) FROM read_parquet('/data/silver/quarantine/*/*.parquet') WHERE _source_file = 'accounts_{d}.csv'\").fetchone()[0]
    except:
        quarantine = 0
    silver_for_date = duckdb.execute(f\"SELECT COUNT(DISTINCT account_id) FROM '/data/silver/accounts/data.parquet' WHERE _source_file = 'accounts_{d}.csv'\").fetchone()[0]
    assert bronze == silver_for_date + quarantine, f'TC-7 FAIL {d}: bronze={bronze} silver={silver_for_date} quar={quarantine}'
print('TC-7 PASS — INV-15 account conservation holds for all dates')
"
```

**Invariant enforcement:**
- INV-05: _source_file, _bronze_ingested_at, _pipeline_run_id, and _record_valid_from non-null on all Silver records. _record_valid_from is a v1.1 addition per ARCHITECTURE v1.2. This is never negotiable.
- INV-10: Re-running produces identical output for identical Bronze input.
- INV-11: Model reads only from data/bronze/ paths.
- INV-15: Account promotion conservation — Bronze accounts row count per date = Silver upserted + Quarantine rejected. No account record may be silently dropped. This is never negotiable.

**Regression classification:** REGRESSION-RELEVANT — INV-05, INV-10, INV-15, account upsert logic, _record_valid_from enforcement.

---

### Task 3.3 — Silver Quarantine Model

**Description:** Implement `dbt_project/models/silver/silver_quarantine.sql`. Captures all records rejected from Silver transactions and Silver accounts.

**CC Prompt:**
```
Replace the stub at dbt_project/models/silver/silver_quarantine.sql with a complete dbt model.

This model captures two rejection streams and unions them.

Stream 1 — Rejected transactions from Bronze transactions partitions:
Read from: read_parquet('{{ var("data_dir") }}/bronze/transactions/*/*.parquet')  {# F-NEW-1 #}
Apply rejection rules in order. A record is rejected at the FIRST rule it fails.

Rejection rules (ordered):
  NULL_REQUIRED_FIELD: transaction_id IS NULL OR account_id IS NULL OR
    transaction_date IS NULL OR amount IS NULL OR transaction_code IS NULL OR
    channel IS NULL OR TRIM(transaction_id) = '' OR TRIM(account_id) = '' OR
    TRIM(transaction_code) = '' OR TRIM(channel) = ''
  INVALID_AMOUNT: amount <= 0 OR non-numeric
  DUPLICATE_TRANSACTION_ID: transaction_id already exists in
    read_parquet('{{ var("data_dir") }}/silver/transactions/*/*.parquet')  {# F-NEW-1 #}
    Use LEFT ANTI JOIN or NOT EXISTS. Handle empty partition case with COALESCE.
    IMPORTANT (R1): Before reading the Silver transactions glob, check whether any Silver
    transaction partitions exist using DuckDB's glob() function or a try/except block.
    If no partitions exist (clean system, first run), evaluate DUPLICATE_TRANSACTION_ID
    as FALSE for all records — there are no prior Silver transactions to duplicate.
    Do NOT attempt to read a non-existent glob path; this raises a DuckDB file-not-found
    error that aborts the model. Pattern:
      {% set silver_txn_path = var("data_dir") ~ "/silver/transactions/*/*.parquet" %}
      {# Check partition existence before joining #}
      WITH silver_existing AS (
        SELECT transaction_id
        FROM read_parquet('{{ silver_txn_path }}')  {# only reached if glob resolves #}
        ...
      )
      Use a CASE or subquery that returns an empty set when no partitions exist.
      Acceptable patterns: glob() existence check in a macro, or a try/except in the
      Silver phase function that catches the file-not-found and passes an empty CTE.
  INVALID_TRANSACTION_CODE: transaction_code NOT IN
    (SELECT transaction_code FROM '{{ var("data_dir") }}/silver/transaction_codes/data.parquet')  {# F-NEW-1 #}
    This must be a JOIN — not a hardcoded list. (INV-03)
  INVALID_CHANNEL: channel NOT IN ('ONLINE', 'IN_STORE')

Note: UNRESOLVABLE_ACCOUNT_ID is NOT a quarantine rule — it is a flag in silver_transactions.

Stream 2 — Rejected accounts from Bronze accounts partitions:
Read from: read_parquet('{{ var("data_dir") }}/bronze/accounts/*/*.parquet')  {# F-NEW-1 #}
Rejection rules:
  NULL_REQUIRED_FIELD: any required field null or empty
  INVALID_ACCOUNT_STATUS: account_status NOT IN ('ACTIVE', 'SUSPENDED', 'CLOSED')

Output columns for all quarantine records:
  _source_file, _pipeline_run_id, _rejected_at (current timestamp),
  _rejection_reason (the code string), plus all original source columns
  (union compatible — use NULL for columns absent in one stream)

Partition by the date embedded in _source_file filename.

Model config (G6 — explicit output filename required for consistency with all verification commands):
  {{ config(materialized='table',
            location=var('data_dir') ~ '/silver/quarantine/date={partition_date}/rejected.parquet')  }}
  The output filename must be `rejected.parquet` — all verification commands across Tasks 3.4,
  6.1 (S1), and the Session 3 integration check reference this exact filename. Using dbt's
  default naming would produce a mismatched path that breaks every post-model check.
  Note: the partition_date macro variable must be set from the _source_file date at model
  execution time, or the model written to iterate per-date. If dbt-duckdb does not support
  dynamic location paths natively, use a Python post-step or partition the output using
  DuckDB COPY ... PARTITION BY syntax in the model SQL, ensuring the filename is `rejected.parquet`
  in each partition directory.

Add dbt schema test in schema.yml:
  - not_null on _rejection_reason
  - accepted_values on _rejection_reason matching the pre-defined code list

Use full paths from repo root. Do not modify any other files.
```

**Test cases:**

| Case | Scenario | Expected |
|---|---|---|
| TC-1 | Record with null transaction_id | in quarantine with NULL_REQUIRED_FIELD |
| TC-2 | Record with amount = 0 | in quarantine with INVALID_AMOUNT |
| TC-3 | Duplicate transaction_id | second occurrence in quarantine with DUPLICATE_TRANSACTION_ID |
| TC-4 | Invalid transaction_code | in quarantine with INVALID_TRANSACTION_CODE |
| TC-5 | Invalid channel value | in quarantine with INVALID_CHANNEL |
| TC-6 | All _rejection_reason values in pre-defined list | dbt accepted_values test passes |

**Verification command:**
```bash
docker compose run --rm pipeline bash -c "cd dbt_project && dbt build --select silver_quarantine"  # R4: dbt build runs model+tests atomically
docker compose run --rm pipeline python -c "
import duckdb
codes = {'NULL_REQUIRED_FIELD','INVALID_AMOUNT','DUPLICATE_TRANSACTION_ID','INVALID_TRANSACTION_CODE','INVALID_CHANNEL','INVALID_ACCOUNT_STATUS'}
rows = duckdb.execute(\"SELECT DISTINCT _rejection_reason FROM read_parquet('/data/silver/quarantine/*/*.parquet')\").fetchall()
actual = {r[0] for r in rows}
invalid = actual - codes
assert not invalid, f'TC-6 FAIL: unexpected codes {invalid}'
print('TC-6 PASS')
"
```

**Invariant enforcement:**
- INV-03: INVALID_TRANSACTION_CODE rejection uses JOIN to silver_transaction_codes — not a hardcoded list. This is never negotiable.
- INV-04: UNRESOLVABLE_ACCOUNT_ID is NOT in the quarantine rejection rules. This is never negotiable.
- INV-05: _source_file, _pipeline_run_id non-null on all quarantine records.
- R1: DUPLICATE_TRANSACTION_ID check must guard against a non-existent Silver transactions glob path. On a clean system (first run, no prior Silver partitions), the duplicate check evaluates FALSE for all records — no glob read is attempted. Attempting to read a non-existent glob raises a DuckDB file-not-found that aborts the model and violates the conservation equation (INV-01) silently.

**Regression classification:** REGRESSION-RELEVANT — INV-03, INV-04 boundary logic.

---

### Task 3.4 — Silver Transactions Model

**Description:** Implement `dbt_project/models/silver/silver_transactions.sql`. Applies sign assignment, referential isolation flag, cross-partition deduplication, and all quality checks.

**CC Prompt:**
```
Replace the stub at dbt_project/models/silver/silver_transactions.sql with a complete dbt model.

Source: read_parquet('{{ var("data_dir") }}/bronze/transactions/*/*.parquet')  {# F-NEW-1 #}

IMPORTANT — inter-model dependency mechanism (F6):
silver_transactions and silver_quarantine are INDEPENDENT dbt models. They do NOT call
each other. Both models read directly from the same Bronze source. Each independently
applies the same rejection rule definitions to determine which records it processes.
  - silver_quarantine: selects records that match a rejection rule and writes them to
    the quarantine path.
  - silver_transactions: selects records that do NOT match any rejection rule and
    promotes them to Silver.
There is no shared CTE, no ref() dependency between these two models, and no shared
intermediate table. The conservation equation (INV-01) is verified post-hoc by querying
both output paths — it is not enforced by model code.

Step 1 — Filter to promotable records only. A record is promotable if it does NOT match
any of these conditions (same definitions as in silver_quarantine — both models must use
identical rule logic or the conservation equation will not hold):
  NULL_REQUIRED_FIELD, INVALID_AMOUNT, DUPLICATE_TRANSACTION_ID,
  INVALID_TRANSACTION_CODE (JOIN to silver_transaction_codes — INV-03),
  INVALID_CHANNEL.

IMPORTANT — DUPLICATE_TRANSACTION_ID glob-safety (R1):
Before reading the Silver transactions glob to check for existing transaction_ids, confirm
that Silver transaction partitions exist. On a clean system (first historical run), no
Silver partitions exist yet and any attempt to read the glob path raises a DuckDB
file-not-found error that aborts the model. Use the same guard as silver_quarantine:
check glob() or use a try/except in the Silver phase function that passes an empty CTE
when no partitions exist. When no prior Silver partitions exist, every record in this
run is non-duplicate — evaluate DUPLICATE_TRANSACTION_ID as FALSE for all records.

Step 2 — Sign assignment:
  _signed_amount = amount * CASE
    WHEN tc.debit_credit_indicator = 'DR' THEN 1
    WHEN tc.debit_credit_indicator = 'CR' THEN -1
  END
  Requires JOIN to silver_transaction_codes on transaction_code.
  The debit_credit_indicator field is the ONLY source of sign. (INV-02)

Step 3 — Referential isolation flag:
  _is_resolvable = CASE WHEN sa.account_id IS NOT NULL THEN TRUE ELSE FALSE END
  Requires LEFT JOIN to silver_accounts on account_id.
  Records with no matching account_id get _is_resolvable = FALSE.
  These records ARE INCLUDED in Silver — NOT sent to quarantine. (INV-04)

Step 4 — Output columns:
  All original source columns plus:
    _source_file, _bronze_ingested_at, _pipeline_run_id, _promoted_at,
    _is_resolvable: BOOLEAN, _signed_amount: DECIMAL

Step 5 — Partition by transaction_date.

Add dbt schema tests in schema.yml:
  - unique + not_null on transaction_id
  - not_null on _signed_amount, _is_resolvable, _pipeline_run_id

Use full paths from repo root. Do not modify any other files.
```

**Test cases:**

| Case | Scenario | Expected |
|---|---|---|
| TC-1 | Conservation equation | bronze_count = silver_count + quarantine_count for each date partition |
| TC-2 | No duplicate transaction_id across all Silver partitions | COUNT(*) = COUNT(DISTINCT transaction_id) |
| TC-3 | _signed_amount non-null | zero null _signed_amount rows |
| TC-4 | Sign from transaction_codes | DR transaction has positive _signed_amount; CR has negative |
| TC-5 | UNRESOLVABLE_ACCOUNT_ID in Silver with _is_resolvable = false | record present in Silver, _is_resolvable = false |
| TC-6 | UNRESOLVABLE_ACCOUNT_ID not in quarantine | no record in quarantine with UNRESOLVABLE_ACCOUNT_ID code |

**Verification command:**
```bash
docker compose run --rm pipeline bash -c "cd dbt_project && dbt build --select silver_transactions"  # R4: dbt build runs model+tests atomically
docker compose run --rm pipeline python -c "
import duckdb
for d in ['2024-01-01','2024-01-02','2024-01-03','2024-01-04','2024-01-05','2024-01-06','2024-01-07']:
    bronze = duckdb.execute(f\"SELECT COUNT(*) FROM '/data/bronze/transactions/date={d}/data.parquet'\").fetchone()[0]
    try: silver = duckdb.execute(f\"SELECT COUNT(*) FROM '/data/silver/transactions/date={d}/data.parquet'\").fetchone()[0]
    except: silver = 0
    try: quar = duckdb.execute(f\"SELECT COUNT(*) FROM '/data/silver/quarantine/date={d}/rejected.parquet' WHERE _source_file LIKE 'transactions%'\").fetchone()[0]
    except: quar = 0
    assert bronze == silver + quar, f'TC-1 FAIL {d}: bronze={bronze} silver={silver} quar={quar}'
print('TC-1 PASS')
total = duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/*/*.parquet')\").fetchone()[0]
distinct = duckdb.execute(\"SELECT COUNT(DISTINCT transaction_id) FROM read_parquet('/data/silver/transactions/*/*.parquet')\").fetchone()[0]
assert total == distinct, f'TC-2 FAIL: {total} rows, {distinct} distinct ids'
print('TC-2 PASS')
"
```

**Invariant enforcement:**
- INV-01: Conservation equation — Bronze = Silver + Quarantine per date. No silent drops. This is never negotiable.
- INV-02: _signed_amount derived exclusively from debit_credit_indicator JOIN. No hardcoded CASE on amount sign. This is never negotiable.
- INV-03: INVALID_TRANSACTION_CODE check uses JOIN to silver_transaction_codes. This is never negotiable.
- INV-04: Unresolvable account_id → _is_resolvable = false in Silver, NOT quarantine. This is never negotiable.
- INV-05: All audit columns non-null.
- R1: DUPLICATE_TRANSACTION_ID glob-safety guard identical to Task 3.3. If the guard logic differs between silver_quarantine and silver_transactions, the conservation equation will not hold — a record could be double-counted or silently dropped when the glob path does not yet exist.

**Regression classification:** REGRESSION-RELEVANT — conservation equation (INV-01), sign assignment (INV-02), referential isolation (INV-04).

---

### Task 3.5 — Silver Phase Function

**Description:** Add `run_silver_phase(config, run_id)` to `pipeline.py`. Runs all Silver dbt models in correct dependency order and returns a `PhaseResult`.

**CC Prompt:**
```
Update pipeline.py at repo root.

Add function: run_silver_phase(config: PipelineConfig, run_id: str) -> PhaseResult

This function must:

1. Check the run log for WARNING entries for this run_id with layer=BRONZE. If any exist:
   - Do NOT abort — WARNING is not a failure.
   - Append a WARNING run log entry (model_name="silver_phase_start", layer="SILVER") with
     error_message="One or more Bronze partitions have zero rows — Silver will process empty
     input for those dates. Analyst review recommended."
   - Continue.

2. Check silver_transaction_codes before running any transaction models:
   Path: {config.data_dir}/silver/transaction_codes/data.parquet
   a. File absent or empty → append FAILED run log entry (model_name="silver_transaction_codes",
      status=FAILED, error_message="silver_transaction_codes absent or empty — cannot promote
      transactions") and return PhaseResult(success=False). (INV-14)
   b. File present and non-empty → append SKIPPED run log entry (model_name=
      "silver_transaction_codes", status=SKIPPED, records_processed=0, records_written=0).
      Do NOT re-run the dbt model. Continue.

3. Run the remaining three Silver models using `dbt build` (not `dbt run`).
   `dbt build` runs model + schema tests atomically in dependency order. Use it for all
   Silver and Gold model execution. Never use `dbt run` followed by a separate `dbt test`
   call — this is the pattern that risks writing corrupt output before tests can catch it.
   (F-NEW-2)

   Run in this order:
     dbt build --select silver_accounts
     dbt build --select silver_quarantine
     dbt build --select silver_transactions

   For each model:
     - Record started_at before subprocess call.
     - Run: dbt build --select <model_name> from the dbt_project/ directory.
     - Record completed_at after subprocess returns.
     - If return code != 0: append FAILED run log entry (status=FAILED, error_message=stderr).
       Return PhaseResult(success=False).
     - If return code == 0: append SUCCESS run log entry.

4. Return PhaseResult(success=True) if all steps pass.

Run log entry rules — every code path appends exactly one entry:
  - Bronze WARNING detected:                      status=WARNING, model_name="silver_phase_start"
  - silver_transaction_codes SKIPPED:             status=SKIPPED, records_written=0
  - silver_transaction_codes FAILED (absent):     status=FAILED, error_message set
  - Each dbt build SUCCESS:                       status=SUCCESS
  - Each dbt build FAILED:                        status=FAILED, error_message=stderr

layer = "SILVER" for all run log entries from this function.
Use full paths from repo root. Do not create any other files.
```

**Test cases:**

| Case | Scenario | Expected |
|---|---|---|
| TC-1 | Silver phase runs after Bronze | PhaseResult(success=True), all Silver files present |
| TC-2 | silver_transaction_codes absent | PhaseResult(success=False), FAILED run log entry for model_name="silver_transaction_codes" |
| TC-3 | dbt build failure on a Silver model | PhaseResult(success=False), FAILED entry in run log |
| TC-4 (F3) | silver_transaction_codes present and non-empty (re-run) | SKIPPED run log entry appended, dbt build NOT re-run |
| TC-5 (F3) | Bronze WARNING entries exist for this run_id | WARNING run log entry for model_name="silver_phase_start" appended before any dbt build |
| TC-6 (F-NEW-2) | dbt build used not dbt run | subprocess call contains "dbt build", not "dbt run" |

**Verification command:**
```bash
docker compose run --rm pipeline python -c "
from config import load_config
from pipeline import run_silver_phase
import uuid
cfg = load_config()
result = run_silver_phase(cfg, str(uuid.uuid4()))
assert result.success, f'TC-1 FAIL: {result.error}'
print('TC-1 PASS')
"
# TC-6 (F-NEW-2): confirm dbt build is used, not dbt run
grep -n 'dbt build\|dbt run' pipeline.py | grep 'silver'
```

**Invariant enforcement:**
- INV-14: silver_transaction_codes presence check before any transaction promotion. This is never negotiable.
- INV-08: PhaseResult(success=False) returned — caller must not proceed to Gold phase.
- F-NEW-2: `dbt build` is mandatory — never `dbt run`. This ensures schema tests run atomically with model execution, preventing corrupt Silver output from being written without a failure signal.

**Regression classification:** REGRESSION-RELEVANT — INV-14 enforcement point; F-NEW-2 dbt build enforcement.

---

## Session 4 — Gold dbt Models

**Session goal:** Both Gold models operational, producing correct aggregations from Silver. Unique key constraints enforced. Gold excludes _is_resolvable = false records.

**Integration check:**
```bash
# F4 PRE-CONDITION: Session 4 verification commands execute dbt against actual Silver Parquet
# fixture data. Before running any S4 verification command, confirm Silver is populated:
docker compose run --rm pipeline python -c "
import duckdb, sys
try:
    silver_txn = duckdb.execute("SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/*/*.parquet')").fetchone()[0]
    assert silver_txn > 0, 'Silver transactions empty — run Session 3 before Session 4'
    silver_acc = duckdb.execute("SELECT COUNT(*) FROM '/data/silver/accounts/data.parquet'").fetchone()[0]
    assert silver_acc > 0, 'Silver accounts empty — run Session 3 before Session 4'
    print(f'Silver fixture present: {silver_txn} transactions, {silver_acc} accounts')
except Exception as e:
    print(f'FAIL — Silver not loaded: {e}'); sys.exit(1)
"
docker compose run --rm pipeline python -c "
import duckdb
daily = duckdb.execute(\"SELECT COUNT(*) FROM '/data/gold/daily_summary/data.parquet'\").fetchone()[0]
weekly = duckdb.execute(\"SELECT COUNT(*) FROM '/data/gold/weekly_account_summary/data.parquet'\").fetchone()[0]
print(f'Gold daily: {daily} rows, weekly: {weekly} rows')
assert daily > 0 and weekly > 0, 'Gold empty'
"
```

---

### Task 4.1 — Gold Daily Summary Model

**Description:** Implement `dbt_project/models/gold/gold_daily_summary.sql`. One row per calendar day, aggregated from Silver transactions where _is_resolvable = true.

**CC Prompt:**
```
Replace the stub at dbt_project/models/gold/gold_daily_summary.sql with a complete dbt model.

Source: read_parquet('{{ var("data_dir") }}/silver/transactions/*/*.parquet')  {# F-NEW-1 #}
Filter: WHERE _is_resolvable = true (INV-04 — exclude unresolvable records from all Gold)

IMPORTANT — Silver transactions glob-safety (R2):
Before executing read_parquet on the Silver transactions glob, confirm that Silver
transaction partitions exist. If no partitions exist, the glob read raises a DuckDB
file-not-found error. Use a guard — either a glob() existence check or a try/except
in the Gold phase function — that returns an empty result set when no Silver transaction
partitions are present. In that case the model produces zero Gold rows, which is correct
and expected. The model must not raise an error on a clean system.

Produce one row per transaction_date with these columns:
  transaction_date: DATE
  total_transactions: COUNT(*) of resolvable transactions
  total_signed_amount: SUM(_signed_amount)
  transactions_by_type: STRUCT with exactly four keys — PURCHASE, PAYMENT, FEE, INTEREST.
    REFUND is explicitly excluded (INV-16): it is defined in the brief as a valid source
    transaction type but is absent from the seed data and is out of scope for the struct
    definition in this exercise. The key set must NOT be derived dynamically from distinct
    values in silver_transaction_codes — it must be the fixed four-key definition below.
    Each entry contains: count: INTEGER and signed_amount_sum: DECIMAL.
    Keys with no transactions on a given day must carry count = 0 and signed_amount_sum = 0.00
    — never omit a key or leave it null. A row with a missing or variable key set must not
    be written to Gold (INV-16).
    Construction pattern:
      STRUCT_PACK(
        PURCHASE := STRUCT_PACK(
          count := COALESCE(SUM(CASE WHEN transaction_type='PURCHASE' THEN 1 END), 0),
          signed_amount_sum := COALESCE(SUM(CASE WHEN transaction_type='PURCHASE' THEN _signed_amount END), 0.00)),
        PAYMENT := STRUCT_PACK(
          count := COALESCE(SUM(CASE WHEN transaction_type='PAYMENT' THEN 1 END), 0),
          signed_amount_sum := COALESCE(SUM(CASE WHEN transaction_type='PAYMENT' THEN _signed_amount END), 0.00)),
        FEE := STRUCT_PACK(
          count := COALESCE(SUM(CASE WHEN transaction_type='FEE' THEN 1 END), 0),
          signed_amount_sum := COALESCE(SUM(CASE WHEN transaction_type='FEE' THEN _signed_amount END), 0.00)),
        INTEREST := STRUCT_PACK(
          count := COALESCE(SUM(CASE WHEN transaction_type='INTEREST' THEN 1 END), 0),
          signed_amount_sum := COALESCE(SUM(CASE WHEN transaction_type='INTEREST' THEN _signed_amount END), 0.00))
      )
  online_transactions: COUNT(*) WHERE channel = 'ONLINE'
  instore_transactions: COUNT(*) WHERE channel = 'IN_STORE'
  _computed_at: TIMESTAMP
  _pipeline_run_id: STRING
  _source_period_start: DATE — MIN(transaction_date) from source Silver records
  _source_period_end: DATE — MAX(transaction_date) from source Silver records

Model config:
  {{ config(materialized='table',
            location=var('data_dir') ~ '/gold/daily_summary/data.parquet'  {# F-NEW-1 #},
            unique_key='transaction_date') }}

Create dbt_project/models/gold/schema.yml and add:
  - unique + not_null on transaction_date
  - not_null on _pipeline_run_id
  - custom dbt test asserting transactions_by_type struct contains exactly the keys
    PURCHASE, PAYMENT, FEE, INTEREST on every row (INV-16 struct shape integrity test)

Use full paths from repo root. Do not modify any other files.
```

**Test cases:**

| Case | Scenario | Expected |
|---|---|---|
| TC-1 | One row per distinct transaction_date | COUNT(*) = COUNT(DISTINCT transaction_date) |
| TC-2 | total_signed_amount matches Silver | SUM(_signed_amount) from Silver (resolvable) for each date = total_signed_amount in Gold |
| TC-3 | _is_resolvable=false excluded | total_transactions excludes unresolvable records |
| TC-4 | online + instore = total | online_transactions + instore_transactions = total_transactions for each row |
| TC-5 (INV-16) | transactions_by_type has exactly four keys on every row | PURCHASE, PAYMENT, FEE, INTEREST present; REFUND absent; no missing or null keys |
| TC-6 (INV-16) | Zero-transaction-type day uses zero-fill | day with no FEE transactions has FEE.count = 0 and FEE.signed_amount_sum = 0.00 |

**Verification command:**
```bash
docker compose run --rm pipeline bash -c "cd dbt_project && dbt build --select gold_daily_summary"  # F-NEW-2
docker compose run --rm pipeline python -c "
import duckdb
total = duckdb.execute(\"SELECT COUNT(*) FROM '/data/gold/daily_summary/data.parquet'\").fetchone()[0]
distinct = duckdb.execute(\"SELECT COUNT(DISTINCT transaction_date) FROM '/data/gold/daily_summary/data.parquet'\").fetchone()[0]
assert total == distinct, f'TC-1 FAIL: {total} rows, {distinct} distinct dates'
print('TC-1 PASS')
rows = duckdb.execute(\"SELECT transaction_date, total_signed_amount FROM '/data/gold/daily_summary/data.parquet' LIMIT 1\").fetchall()
d, gold_sum = rows[0]
silver_sum = duckdb.execute(f\"SELECT SUM(_signed_amount) FROM read_parquet('/data/silver/transactions/*/*.parquet') WHERE transaction_date = '{d}' AND _is_resolvable = true\").fetchone()[0]
assert abs(float(gold_sum) - float(silver_sum)) < 0.001, f'TC-2 FAIL for {d}'
print('TC-2 PASS')
# INV-16 struct shape check — PURCHASE, PAYMENT, FEE, INTEREST present; REFUND absent
rows_all = duckdb.execute(\"SELECT transactions_by_type FROM '/data/gold/daily_summary/data.parquet'\").fetchall()
expected_keys = {'PURCHASE', 'PAYMENT', 'FEE', 'INTEREST'}
for row in rows_all:
    struct = row[0]
    actual_keys = set(struct.keys()) if hasattr(struct, 'keys') else set(struct._fields)
    assert actual_keys == expected_keys, f'TC-5 FAIL: struct keys {actual_keys} != {expected_keys}'
    assert 'REFUND' not in actual_keys, 'TC-5 FAIL: REFUND must not be in struct'
print('TC-5 PASS — INV-16 struct shape correct on all rows')
"
```

**Invariant enforcement:**
- INV-04: WHERE _is_resolvable = true on all aggregations. This is never negotiable.
- INV-12: unique_key='transaction_date' — dbt enforces one row per date. This is never negotiable.
- INV-05: _pipeline_run_id non-null.
- INV-16: transactions_by_type STRUCT must contain exactly four keys: PURCHASE, PAYMENT, FEE, INTEREST. REFUND is explicitly excluded from this exercise. Keys must always be present with zero-fill — never omitted, never variable across rows. A dynamic derivation from silver_transaction_codes at runtime is not permitted. This is never negotiable.
- R2: Silver transactions glob-safety guard required. A file-not-found from a non-existent glob aborts the Gold phase and leaves the watermark unadvanced — violating INV-08 (Atomic Pipeline Execution) by producing a partial failure that cannot be cleanly retried.

**Regression classification:** REGRESSION-RELEVANT — INV-04 exclusion, INV-12 unique key, INV-16 struct shape integrity.

---

### Task 4.2 — Gold Weekly Account Summary Model

**Description:** Implement `dbt_project/models/gold/gold_weekly_account_summary.sql`. One row per account per ISO calendar week, for accounts with at least one resolvable transaction.

**CC Prompt:**
```
Replace the stub at dbt_project/models/gold/gold_weekly_account_summary.sql with a complete dbt model.

Sources:
  Transactions: read_parquet('{{ var("data_dir") }}/silver/transactions/*/*.parquet')  {# F-NEW-1 #}
    Filter: WHERE _is_resolvable = true (INV-04)
  Accounts: '{{ var("data_dir") }}/silver/accounts/data.parquet'  {# F-NEW-1 #}

IMPORTANT — Silver transactions glob-safety (R2):
Before executing read_parquet on the Silver transactions glob, confirm that Silver
transaction partitions exist. If no partitions exist, the glob read raises a DuckDB
file-not-found error. Use a guard — either a glob() existence check or a try/except
in the Gold phase function — that returns an empty result set when no Silver transaction
partitions are present. In that case the model produces zero Gold rows, which is correct
and expected. The model must not raise an error on a clean system.

Week definition: ISO week, Monday start.
  week_start_date = DATE_TRUNC('week', transaction_date)
  week_end_date = DATE_TRUNC('week', transaction_date) + INTERVAL 6 DAYS

Produce one row per (week_start_date, account_id):
  week_start_date: DATE
  week_end_date: DATE
  account_id: STRING
  total_purchases: COUNT(*) WHERE transaction_type = 'PURCHASE'
  avg_purchase_amount: AVG(_signed_amount) WHERE transaction_type = 'PURCHASE' (NULL if no purchases)
  total_payments: COALESCE(SUM(_signed_amount) WHERE transaction_type = 'PAYMENT', 0)
  total_fees: COALESCE(SUM(_signed_amount) WHERE transaction_type = 'FEE', 0)
  total_interest: COALESCE(SUM(_signed_amount) WHERE transaction_type = 'INTEREST', 0)
  closing_balance: current_balance from Silver Accounts (JOIN on account_id)
  _computed_at: TIMESTAMP
  _pipeline_run_id: STRING

Only include accounts with at least one resolvable transaction in the week.

Model config:
  {{ config(materialized='table',
            location=var('data_dir') ~ '/gold/weekly_account_summary/data.parquet'  {# F-NEW-1 #},
            unique_key=['week_start_date', 'account_id']) }}

Add dbt schema tests in gold/schema.yml:
  - unique (composite key: week_start_date + account_id)
  - not_null on week_start_date, account_id, _pipeline_run_id

Use full paths from repo root. Do not modify any other files.
```

**Test cases:**

| Case | Scenario | Expected |
|---|---|---|
| TC-1 | One row per (week_start_date, account_id) | dbt unique test passes |
| TC-2 | total_purchases count matches Silver | COUNT from Silver for that week/account/type = total_purchases |
| TC-3 | _is_resolvable=false excluded | account with only unresolvable transactions not in weekly summary |
| TC-4 | week_end_date = week_start_date + 6 days | for all rows |

**Verification command:**
```bash
docker compose run --rm pipeline bash -c "cd dbt_project && dbt build --select gold_weekly_account_summary"  # F-NEW-2
docker compose run --rm pipeline python -c "
import duckdb
total = duckdb.execute(\"SELECT COUNT(*) FROM '/data/gold/weekly_account_summary/data.parquet'\").fetchone()[0]
distinct = duckdb.execute(\"SELECT COUNT(*) FROM (SELECT DISTINCT week_start_date, account_id FROM '/data/gold/weekly_account_summary/data.parquet')\").fetchone()[0]
assert total == distinct, f'TC-1 FAIL: {total} rows, {distinct} distinct keys'
print('TC-1 PASS')
bad = duckdb.execute(\"SELECT COUNT(*) FROM '/data/gold/weekly_account_summary/data.parquet' WHERE week_end_date != week_start_date + INTERVAL 6 DAYS\").fetchone()[0]
assert bad == 0, f'TC-4 FAIL: {bad} rows with wrong week_end_date'
print('TC-4 PASS')
"
```

**Invariant enforcement:**
- INV-04: WHERE _is_resolvable = true on all transaction source queries. This is never negotiable.
- INV-12: unique_key=['week_start_date', 'account_id']. This is never negotiable.
- INV-05: _pipeline_run_id non-null.
- R2: Silver transactions glob-safety guard required — same rationale as Task 4.1. Model must return zero rows cleanly when Silver transactions glob resolves to no files, rather than raising a file-not-found that aborts the Gold phase.

**Regression classification:** REGRESSION-RELEVANT — INV-04, INV-12.

---

### Task 4.3 — Gold Phase Function

**Description:** Add `run_gold_phase(config, run_id)` to `pipeline.py`. Runs both Gold dbt models and returns a `PhaseResult`.

**CC Prompt:**
```
Update pipeline.py at repo root.

Add function: run_gold_phase(config: PipelineConfig, run_id: str) -> PhaseResult

This function must:

1. Check the run log for WARNING entries for this run_id with layer=BRONZE or layer=SILVER.
   If any exist:
   - Do NOT abort.
   - Append a WARNING run log entry (model_name="gold_phase_start", layer="GOLD") with
     error_message="Upstream WARNING entries detected — Gold aggregations may reflect empty
     Bronze partitions. Check run_log for Bronze/Silver WARNING entries."
   - Continue.

2. Run Gold models using `dbt build` (not `dbt run`). `dbt build` runs model + schema tests
   atomically, ensuring the unique_key constraints (INV-12) are enforced before the Gold
   Parquet files are written. Never use `dbt run` followed by a separate `dbt test`. (F-NEW-2)

   Run in this order:
     dbt build --select gold_daily_summary
     dbt build --select gold_weekly_account_summary

   For each model:
     - Record started_at before subprocess call.
     - Run: dbt build --select <model_name> from the dbt_project/ directory.
     - Record completed_at after subprocess returns.
     - If return code != 0: append FAILED run log entry (status=FAILED, error_message=stderr).
       Return PhaseResult(success=False).
     - If return code == 0: append SUCCESS run log entry.

3. Return PhaseResult(success=True) if all steps pass.

Run log entry rules — every code path appends exactly one entry:
  - Upstream WARNING detected:          status=WARNING, model_name="gold_phase_start"
  - Each dbt build SUCCESS:             status=SUCCESS
  - Each dbt build FAILED:              status=FAILED, error_message=stderr

layer = "GOLD" for all run log entries from this function.
Use full paths from repo root. Do not create any other files.
```

**Test cases:**

| Case | Scenario | Expected |
|---|---|---|
| TC-1 | Gold phase runs after Silver — no upstream WARNINGs | PhaseResult(success=True), both Gold files present |
| TC-2 | dbt build fails on unique key violation | PhaseResult(success=False), FAILED entry in run log |
| TC-3 (F3) | Upstream Bronze WARNING exists for this run_id | WARNING run log entry for model_name="gold_phase_start" appended first |
| TC-4 (F-NEW-2) | dbt build used not dbt run | subprocess call contains "dbt build", not "dbt run" |

**Verification command:**
```bash
docker compose run --rm pipeline python -c "
from config import load_config
from pipeline import run_gold_phase
import uuid, duckdb
cfg = load_config()
result = run_gold_phase(cfg, str(uuid.uuid4()))
assert result.success, f'TC-1 FAIL: {result.error}'
assert duckdb.execute(\"SELECT COUNT(*) FROM '/data/gold/daily_summary/data.parquet'\").fetchone()[0] > 0
assert duckdb.execute(\"SELECT COUNT(*) FROM '/data/gold/weekly_account_summary/data.parquet'\").fetchone()[0] > 0
print('TC-1 PASS')
"
```

**Invariant enforcement:**
- INV-08: PhaseResult returned; caller must not advance watermark on failure.
- INV-12 (F-NEW-2): `dbt build` enforces unique_key schema tests atomically with model execution — corrupt Gold output cannot be written without a failure signal. This is never negotiable.

**Regression classification:** REGRESSION-RELEVANT — Gold phase gating.

---

## Session 5 — Pipeline Orchestration

**Session goal:** Complete `pipeline.py` — both historical and incremental modes fully wired. Watermark advance gated on all three phases succeeding. End-to-end run completes for the 7-day seed data.

**Integration check:**
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

### Task 5.1 — Historical Pipeline Orchestrator

**Description:** Wire `main()` to call the three phase functions in sequence for historical mode, advance the watermark only on full success.

**CC Prompt:**
```
Update pipeline.py at repo root. Replace the main() stub with a complete implementation.

main() for HISTORICAL mode must:

1. Generate a unique run_id using uuid.uuid4().
2. Call load_config(). If it exits (sys.exit), the process terminates.
3. Call validate_source_files(config). If it exits, the process terminates.
4. Write an initial run log entry with status=SUCCESS for model_name="pipeline_start",
   layer="BRONZE", records_processed=0, records_written=0.
5. Call run_bronze_phase(config, run_id).
   If PhaseResult.success is False: append FAILED run log entry, print error, sys.exit(1).
6. Call run_silver_phase(config, run_id).
   If PhaseResult.success is False: append FAILED run log entry, print error, sys.exit(1).
7. Call run_gold_phase(config, run_id).
   If PhaseResult.success is False: append FAILED run log entry, print error, sys.exit(1).
8. (F1) Advance the watermark — EXPLICIT INITIALISATION STEP:
   This is the first time the watermark is written for a historical run.
   Call: write_watermark(config.data_dir, config.end_date, run_id)
   This writes {config.data_dir}/pipeline/control.parquet with:
     last_processed_date = config.end_date
     updated_at = current UTC timestamp
     updated_by_run_id = run_id
   This file did not exist before this call. Creating it is an explicit,
   named action — not a side-effect. If write_watermark raises an exception:
     - Append a FAILED run log entry with error_message="Watermark write failed: {e}"
     - Print the error
     - sys.exit(1)
   The watermark write is the FINAL operation on the success path. (INV-09)
9. Verify the watermark was written correctly by reading it back:
   wm = read_watermark(config.data_dir)
   assert wm == config.end_date, f"Watermark verify FAIL: wrote {config.end_date}, read {wm}"
   If assertion fails: print error, sys.exit(1).
10. Print "Pipeline complete. Watermark advanced to {config.end_date}."
11. sys.exit(0)

Known gap: startup validation failures before run_id generation produce no run log entry.
This is a documented limitation — not a violation of INV-06.

Use full paths from repo root. Do not create any other files.
```

**Test cases:**

| Case | Scenario | Expected |
|---|---|---|
| TC-1 | Full 7-day historical run | exits 0, watermark = 2024-01-07 |
| TC-2 | Re-run identical historical | exits 0, watermark unchanged, no duplicates at any layer |
| TC-3 | Silver phase failure | exits 1, watermark NOT advanced |

**Verification command:**
```bash
docker compose run --rm pipeline python pipeline.py && echo "Exit 0 PASS"
docker compose run --rm pipeline python -c "
from lake_io import read_watermark
import datetime
wm = read_watermark('/data')
assert wm == datetime.date(2024, 1, 7), f'TC-1 FAIL: {wm}'
print('TC-1 PASS')
"
```

**Invariant enforcement:**
- INV-09: Watermark write is structurally the last operation on the success path. No code path advances watermark before all three PhaseResult.success = True. This is never negotiable.
- INV-08: sys.exit(1) on any phase failure. Non-zero exit code. This is never negotiable.

**Regression classification:** REGRESSION-RELEVANT — watermark integrity is INV-09.

---

### Task 5.2 — Incremental Pipeline Orchestrator

**Description:** Add incremental mode handling to `main()`. Reads watermark, determines next date, gates on all three phases, advances watermark only on full success.

**CC Prompt:**
```
Update pipeline.py at repo root. Extend main() to handle INCREMENTAL mode.

When config.mode == "incremental":

1. Generate run_id.
2. Read watermark via read_watermark(config.data_dir).
   If None: print "No watermark found. Run historical pipeline first." sys.exit(1).
3. Determine next_date = watermark + datetime.timedelta(days=1).
4. Validate source files for next_date (reuse validate_source_files logic).
5. (F5) Silver accounts integrity pre-check — before invoking any phase function:
   Check that {config.data_dir}/silver/accounts/data.parquet exists and is non-empty:
     SELECT COUNT(*) FROM '{config.data_dir}/silver/accounts/data.parquet'
   If the file does not exist or returns 0 rows:
     - Append a FAILED run log entry with error_message=
       "silver_accounts absent or empty — incremental Silver phase requires a baseline
        accounts snapshot from the historical run. Re-run historical pipeline first."
     - Print the error
     - sys.exit(1)
   Rationale: incremental Silver transactions JOIN to silver_accounts for _is_resolvable
   flagging. A missing or empty accounts snapshot produces all _is_resolvable = FALSE,
   silently corrupting Gold aggregations without triggering any other guard.
6. Call run_bronze_phase(config, run_id).
7. Call run_silver_phase(config, run_id).
8. Call run_gold_phase(config, run_id).
9. On all three success: advance watermark to next_date.
   Call write_watermark(config.data_dir, next_date, run_id).
   Verify by reading back: assert read_watermark(config.data_dir) == next_date (F1 pattern).
10. Print "Incremental pipeline complete. Watermark advanced to {next_date}."
11. sys.exit(0)

On any phase failure: sys.exit(1), no watermark advance.

Note: Gold models use full table materialization — fully recomputed on each incremental run.

Use full paths from repo root. Do not create any other files.
```

**Test cases:**

| Case | Scenario | Expected |
|---|---|---|
| TC-1 | Incremental run, next file absent | exits 1, clear "file not found" error, watermark unchanged |
| TC-2 | No watermark (control.parquet absent) | exits 1, "Run historical pipeline first" message |
| TC-3 | Valid incremental run (8th file present) | exits 0, watermark = 2024-01-08, verified by read-back |
| TC-4 (F5) | silver_accounts absent before incremental run | exits 1, error message references silver_accounts, watermark unchanged |
| TC-5 (F5) | silver_accounts present but empty | exits 1, error message references silver_accounts |

**Verification command:**
```bash
# TC-2 — no watermark
docker compose run --rm pipeline bash -c "mv /data/pipeline/control.parquet /tmp/ctrl_backup.parquet; PIPELINE_MODE=incremental python pipeline.py; echo 'exit: '$?; mv /tmp/ctrl_backup.parquet /data/pipeline/control.parquet"
# TC-4 (F5) — silver_accounts absent
docker compose run --rm pipeline bash -c "mv /data/silver/accounts/data.parquet /tmp/acc_backup.parquet; PIPELINE_MODE=incremental python pipeline.py; echo 'exit: '$?; mv /tmp/acc_backup.parquet /data/silver/accounts/data.parquet"
# TC-3 — watermark read-back after valid incremental run (requires 8th source file)
docker compose run --rm pipeline python -c "
from lake_io import read_watermark
import datetime
wm = read_watermark('/data')
assert wm == datetime.date(2024, 1, 8), f'TC-3 FAIL: watermark={wm}'
print('TC-3 PASS')
"
```

**Invariant enforcement:**
- INV-09: Watermark advance only after all three phases succeed. Watermark verified by read-back (F1 pattern).
- INV-08: sys.exit(1) on any failure.
- F5: silver_accounts integrity pre-check fires before any phase function is called, preventing silent Gold corruption.

**Regression classification:** REGRESSION-RELEVANT — INV-09, incremental mode gating; F5 accounts pre-check.

---

### Task 5.3 — Idempotency Hardening

**Description:** Verification-only task. Run the full pipeline twice on identical input and verify all layer outputs are identical.

**CC Prompt:**
```
This is a verification task — do not write any new code.

Run the following sequence and record results:

Step 1: Record current state after first successful historical run:
  - Bronze transactions row count
  - Silver transactions row count
  - Quarantine row count
  - Gold daily_summary row count
  - Gold weekly_account_summary row count
  - Watermark
  - Run log entry count

Step 2: Re-run the historical pipeline:
  docker compose run --rm pipeline python pipeline.py

Step 3: Record the same counts again.

Step 4: Assert all counts are identical between Step 1 and Step 3.
  Assert watermark is unchanged.
  Assert run_log has exactly 2x the model entries (two runs recorded, second run appended).

Record all counts and assertions in the Verification Record.
```

**Test cases:**

| Case | Scenario | Expected |
|---|---|---|
| TC-1 | Bronze counts after re-run | identical to first run |
| TC-2 | Silver counts after re-run | identical |
| TC-3 | Quarantine counts after re-run | identical |
| TC-4 | Gold counts after re-run | identical |
| TC-5 | Watermark after re-run | unchanged from first run |
| TC-6 | Run log row count | doubled (second run entries appended) |

**Verification command:**
```bash
docker compose run --rm pipeline python -c "
import duckdb
print('bronze_txn:', duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/bronze/transactions/*/*.parquet')\").fetchone()[0])
print('silver_txn:', duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/*/*.parquet')\").fetchone()[0])
print('quarantine:', duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/silver/quarantine/*/*.parquet')\").fetchone()[0])
print('gold_daily:', duckdb.execute(\"SELECT COUNT(*) FROM '/data/gold/daily_summary/data.parquet'\").fetchone()[0])
print('gold_weekly:', duckdb.execute(\"SELECT COUNT(*) FROM '/data/gold/weekly_account_summary/data.parquet'\").fetchone()[0])
print('run_log:', duckdb.execute(\"SELECT COUNT(*) FROM '/data/pipeline/run_log.parquet'\").fetchone()[0])
from lake_io import read_watermark; print('watermark:', read_watermark('/data'))
"
```

**Invariant enforcement:**
- INV-10: Idempotency — re-execution produces identical output. This is never negotiable.
- INV-06: Run log row count doubles (entries appended, not overwritten). This is never negotiable.

**Regression classification:** REGRESSION-RELEVANT — INV-10 full-pipeline idempotency test.

---

### Task 5.4 — Audit Trail Verification

**Description:** Verification-only task. Confirm every layer record's _pipeline_run_id traces to a SUCCESS entry in the run log.

**CC Prompt:**
```
This is a verification task — do not write any new code.

Run the following DuckDB queries and record results:

Query 1 — Bronze audit completeness:
  SELECT COUNT(*) FROM read_parquet('/data/bronze/transactions/*/*.parquet') WHERE _pipeline_run_id IS NULL
  SELECT COUNT(*) FROM read_parquet('/data/bronze/accounts/*/*.parquet') WHERE _pipeline_run_id IS NULL
  SELECT COUNT(*) FROM '/data/bronze/transaction_codes/data.parquet' WHERE _pipeline_run_id IS NULL
  Expected: all three return 0.

Query 2 — Silver audit completeness:
  SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/*/*.parquet') WHERE _pipeline_run_id IS NULL
  SELECT COUNT(*) FROM '/data/silver/accounts/data.parquet' WHERE _pipeline_run_id IS NULL
  SELECT COUNT(*) FROM '/data/silver/transaction_codes/data.parquet' WHERE _pipeline_run_id IS NULL
  Expected: all three return 0.

Query 3 — Gold audit completeness:
  SELECT COUNT(*) FROM '/data/gold/daily_summary/data.parquet' WHERE _pipeline_run_id IS NULL
  SELECT COUNT(*) FROM '/data/gold/weekly_account_summary/data.parquet' WHERE _pipeline_run_id IS NULL
  Expected: both return 0.

Query 4 — Run log traceability:
  SELECT DISTINCT _pipeline_run_id FROM read_parquet('/data/silver/transactions/*/*.parquet')
    WHERE _pipeline_run_id NOT IN (SELECT run_id FROM '/data/pipeline/run_log.parquet' WHERE status = 'SUCCESS')
  Expected: returns 0 rows.

Record all results in the Verification Record.
```

**Test cases:**

| Case | Scenario | Expected |
|---|---|---|
| TC-1 | Bronze null _pipeline_run_id | 0 for all three entities |
| TC-2 | Silver null _pipeline_run_id | 0 for all three entities |
| TC-3 | Gold null _pipeline_run_id | 0 for both Gold files |
| TC-4 | Silver run_ids traceable to run log | 0 untraceable run_ids |

**Verification command:**
```bash
docker compose run --rm pipeline python -c "
import duckdb
checks = [
    (\"SELECT COUNT(*) FROM read_parquet('/data/bronze/transactions/*/*.parquet') WHERE _pipeline_run_id IS NULL\", 'Bronze txn'),
    (\"SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/*/*.parquet') WHERE _pipeline_run_id IS NULL\", 'Silver txn'),
    (\"SELECT COUNT(*) FROM '/data/gold/daily_summary/data.parquet' WHERE _pipeline_run_id IS NULL\", 'Gold daily'),
]
for q, label in checks:
    n = duckdb.execute(q).fetchone()[0]
    assert n == 0, f'{label} FAIL: {n} null run_ids'
    print(f'{label} PASS')
untraceable = duckdb.execute(\"SELECT COUNT(DISTINCT _pipeline_run_id) FROM read_parquet('/data/silver/transactions/*/*.parquet') WHERE _pipeline_run_id NOT IN (SELECT run_id FROM '/data/pipeline/run_log.parquet' WHERE status='SUCCESS')\").fetchone()[0]
assert untraceable == 0, f'TC-4 FAIL: {untraceable} untraceable run_ids'
print('TC-4 PASS')
"
```

**Harness form (HARNESS-CANDIDATE — stateless, no build context required):**

The following four SQL assertions can be executed directly against any deployed instance of
the lake using the DuckDB CLI. They require only read access to the mounted data/ directory.
No Docker build context, no pipeline.py, no Python imports. DATA_DIR must be set to the
mount path of the data/ volume on the target system (default: /data).

```bash
# Set DATA_DIR to the data volume mount path on the target system
DATA_DIR=/data

duckdb -c "
-- TC-1/TC-2: Non-null _pipeline_run_id across Bronze (spot-check transactions + accounts + tx codes)
SELECT 'Bronze txn null run_ids' AS check_name,
       COUNT(*) AS result,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS verdict
FROM read_parquet('${DATA_DIR}/bronze/transactions/*/*.parquet')
WHERE _pipeline_run_id IS NULL
UNION ALL
SELECT 'Bronze acc null run_ids',
       COUNT(*),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM read_parquet('${DATA_DIR}/bronze/accounts/*/*.parquet')
WHERE _pipeline_run_id IS NULL
UNION ALL
SELECT 'Bronze tc null run_ids',
       COUNT(*),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM '${DATA_DIR}/bronze/transaction_codes/data.parquet'
WHERE _pipeline_run_id IS NULL;

-- TC-2: Non-null _pipeline_run_id across Silver
SELECT 'Silver txn null run_ids' AS check_name,
       COUNT(*) AS result,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS verdict
FROM read_parquet('${DATA_DIR}/silver/transactions/*/*.parquet')
WHERE _pipeline_run_id IS NULL
UNION ALL
SELECT 'Silver acc null run_ids',
       COUNT(*),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM '${DATA_DIR}/silver/accounts/data.parquet'
WHERE _pipeline_run_id IS NULL
UNION ALL
SELECT 'Silver tc null run_ids',
       COUNT(*),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM '${DATA_DIR}/silver/transaction_codes/data.parquet'
WHERE _pipeline_run_id IS NULL;

-- TC-3: Non-null _pipeline_run_id across Gold
SELECT 'Gold daily null run_ids' AS check_name,
       COUNT(*) AS result,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS verdict
FROM '${DATA_DIR}/gold/daily_summary/data.parquet'
WHERE _pipeline_run_id IS NULL
UNION ALL
SELECT 'Gold weekly null run_ids',
       COUNT(*),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM '${DATA_DIR}/gold/weekly_account_summary/data.parquet'
WHERE _pipeline_run_id IS NULL;

-- TC-4: Every Silver _pipeline_run_id traces to a SUCCESS run log entry (source-to-destination lineage)
SELECT 'Silver run_id lineage' AS check_name,
       COUNT(DISTINCT _pipeline_run_id) AS untraceable_run_ids,
       CASE WHEN COUNT(DISTINCT _pipeline_run_id) = 0 THEN 'PASS' ELSE 'FAIL' END AS verdict
FROM read_parquet('${DATA_DIR}/silver/transactions/*/*.parquet')
WHERE _pipeline_run_id NOT IN (
    SELECT run_id
    FROM '${DATA_DIR}/pipeline/run_log.parquet'
    WHERE status = 'SUCCESS'
);
"
```

All four assertions must return verdict = PASS. Any FAIL result indicates an INV-05 violation
on the deployed system. This harness form is designed to run without pipeline code, Docker,
or build context — it operates solely on the Parquet files produced by the pipeline.

**Invariant enforcement:**
- INV-05: Audit chain continuity — full chain verified end-to-end. This is never negotiable.
  The harness form above is the canonical stateless assertion for INV-05 on any deployed
  instance of the lake. It covers both audit column completeness (non-null _pipeline_run_id
  at every layer) and source-to-destination lineage (every Silver run_id traces to a SUCCESS
  run log entry). These are the two properties named in INV-05.

**Regression classification:** HARNESS-CANDIDATE — INV-05 full audit chain verification.
Qualifies because: (1) stateless — queries Parquet files only, no pipeline state or imports;
(2) no build context required — DuckDB CLI plus read access to data/ volume is sufficient;
(3) directly tied to a named GLOBAL invariant (INV-05 Audit Chain Continuity).
The harness form above is the deployable assertion. The docker compose form above it is
retained for convenience during development sessions.

---

## Session 6 — End-to-End Integration and Phase 8 Preparation

**Session goal:** All Phase 8 verification expectations (Brief Section 10) confirmed passing. System ready for Phase 8 sign-off.

**Integration check:**
```bash
docker compose run --rm pipeline python -c "print('Session 6 integration check — run all Phase 8 verification queries below')"
```

---

### Task 6.1 — Phase 8 Verification Command Suite

**Description:** Produce and run all Phase 8 verification commands from Brief Section 10. Each command must produce a PASS/FAIL verdict.

**Canonical check table (Task 6.2 references these IDs verbatim):**

| Check ID | Category | What it verifies |
|---|---|---|
| B1 | Bronze Completeness | Bronze transactions row count = sum of 7 source CSV row counts |
| B2 | Bronze Completeness | Bronze accounts row count = sum of 7 accounts CSV row counts |
| B3 | Bronze Completeness | Bronze transaction_codes row count = transaction_codes.csv row count |
| S1 | Silver Quality | Silver txn + quarantine = Bronze txn per date partition (conservation) |
| S2 | Silver Quality | No duplicate transaction_id across all Silver transaction partitions |
| S3 | Silver Quality | Every Silver transaction has a valid transaction_code in silver_transaction_codes |
| S4 | Silver Quality | No null _signed_amount in silver_transactions |
| S5 | Silver Quality | Every quarantine record has a non-null _rejection_reason from the pre-defined code list |
| G1 | Gold Correctness | gold_daily_summary has one row per distinct resolvable transaction_date in Silver |
| G2 | Gold Correctness | gold_weekly_account_summary total_purchases matches Silver COUNT (spot check one week/account) |
| G3 | Gold Correctness | gold_daily_summary total_signed_amount matches Silver SUM for two spot-checked dates |
| I1 | Idempotency | Bronze row counts identical after second historical run |
| I2 | Idempotency | Silver row counts identical after second historical run |
| I3 | Idempotency | Quarantine row counts identical after second historical run |
| I4 | Idempotency | Gold row counts identical after second historical run; watermark unchanged |
| A1 | Audit Trail | Zero null _pipeline_run_id rows across all Bronze entities |
| A2 | Audit Trail | Zero null _pipeline_run_id rows across all Silver entities |
| A3 | Audit Trail | Zero null _pipeline_run_id rows across both Gold files |
| A4 | Audit Trail | All Silver _pipeline_run_id values trace to a SUCCESS entry in run_log.parquet |
| S6 | Silver Quality | INV-15: Bronze accounts row count per date = Silver upserted + Quarantine rejected for accounts |
| G4 | Gold Correctness | INV-16: transactions_by_type STRUCT contains exactly PURCHASE, PAYMENT, FEE, INTEREST on every row; REFUND absent |

**CC Prompt:**
```
This is a verification and documentation task. Do not write any new code.

Run the following checks using the canonical IDs from the table above. For each: record the
check ID, the exact command, the result (PASS or FAIL), and the actual value. If FAIL, record
the root cause.

B1: bronze_txn = SELECT COUNT(*) FROM read_parquet('/data/bronze/transactions/*/*.parquet')
    source_txn = SUM of SELECT COUNT(*) FROM read_csv_auto('/source/transactions_2024-01-0{N}.csv') for N in 1..7
    PASS if bronze_txn == source_txn

B2: bronze_acc = SELECT COUNT(*) FROM read_parquet('/data/bronze/accounts/*/*.parquet')
    source_acc = SUM of SELECT COUNT(*) FROM read_csv_auto('/source/accounts_2024-01-0{N}.csv') for N in 1..7
    PASS if bronze_acc == source_acc

B3: bronze_tc = SELECT COUNT(*) FROM '/data/bronze/transaction_codes/data.parquet'
    source_tc  = SELECT COUNT(*) FROM read_csv_auto('/source/transaction_codes.csv')
    PASS if bronze_tc == source_tc

S1: For each date d in 2024-01-01 through 2024-01-07:
    bronze_d  = SELECT COUNT(*) FROM '/data/bronze/transactions/date={d}/data.parquet'
    silver_d  = SELECT COUNT(*) FROM '/data/silver/transactions/date={d}/data.parquet' (0 if absent)
    quarant_d = SELECT COUNT(*) FROM '/data/silver/quarantine/date={d}/rejected.parquet'
                WHERE _source_file LIKE 'transactions%' (0 if absent)
    PASS if bronze_d == silver_d + quarant_d for every date

S2: total    = SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/*/*.parquet')
    distinct = SELECT COUNT(DISTINCT transaction_id) FROM read_parquet('/data/silver/transactions/*/*.parquet')
    PASS if total == distinct

S3: invalid = SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/*/*.parquet') st
              WHERE st.transaction_code NOT IN
                (SELECT transaction_code FROM '/data/silver/transaction_codes/data.parquet')
    PASS if invalid == 0

S4: nulls = SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/*/*.parquet')
            WHERE _signed_amount IS NULL
    PASS if nulls == 0

S5: SELECT DISTINCT _rejection_reason FROM read_parquet('/data/silver/quarantine/*/*.parquet')
    PASS if result set is a subset of:
    {NULL_REQUIRED_FIELD, INVALID_AMOUNT, DUPLICATE_TRANSACTION_ID,
     INVALID_TRANSACTION_CODE, INVALID_CHANNEL, INVALID_ACCOUNT_STATUS}

G1: gold_dates   = SELECT COUNT(*) FROM '/data/gold/daily_summary/data.parquet'
    silver_dates = SELECT COUNT(DISTINCT transaction_date) FROM
                   read_parquet('/data/silver/transactions/*/*.parquet') WHERE _is_resolvable = true
    PASS if gold_dates == silver_dates

G2: Pick any (week_start_date, account_id) row from gold_weekly_account_summary.
    silver_count = SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/*/*.parquet')
                   WHERE account_id = '{account_id}'
                   AND DATE_TRUNC('week', transaction_date) = '{week_start_date}'
                   AND transaction_code IN (SELECT transaction_code FROM
                     '/data/silver/transaction_codes/data.parquet' WHERE transaction_type = 'PURCHASE')
                   AND _is_resolvable = true
    PASS if silver_count == gold_total_purchases for that row

G3: For two distinct dates d1, d2 from gold_daily_summary:
    gold_sum   = total_signed_amount WHERE transaction_date = d
    silver_sum = SELECT SUM(_signed_amount) FROM read_parquet('/data/silver/transactions/*/*.parquet')
                 WHERE transaction_date = d AND _is_resolvable = true
    PASS if ABS(gold_sum - silver_sum) < 0.001 for both dates

I1: Record bronze_txn, bronze_acc, bronze_tc counts. Re-run historical pipeline. PASS if identical.
    (Reference Task 5.3 recorded values if already run.)

I2: Record silver_txn count. Re-run. PASS if identical.

I3: Record quarantine row count. Re-run. PASS if identical.

I4: Record gold_daily and gold_weekly row counts and watermark. Re-run.
    PASS if all three are identical after re-run.

A1: SELECT COUNT(*) WHERE _pipeline_run_id IS NULL from each Bronze entity → must all be 0

A2: SELECT COUNT(*) WHERE _pipeline_run_id IS NULL from each Silver entity → must all be 0

A3: SELECT COUNT(*) WHERE _pipeline_run_id IS NULL from both Gold files → must both be 0

A4: SELECT COUNT(DISTINCT _pipeline_run_id) FROM read_parquet('/data/silver/transactions/*/*.parquet')
    WHERE _pipeline_run_id NOT IN
      (SELECT run_id FROM '/data/pipeline/run_log.parquet' WHERE status = 'SUCCESS')
    PASS if result is 0

S6 (INV-15 — Account Promotion Conservation):
    bronze_distinct = SELECT COUNT(DISTINCT account_id)
                      FROM read_parquet('/data/bronze/accounts/*/*.parquet')
    silver_distinct = SELECT COUNT(*) FROM '/data/silver/accounts/data.parquet'
    quarant_accts   = SELECT COUNT(*) FROM read_parquet('/data/silver/quarantine/*/*.parquet')
                      WHERE _source_file LIKE 'accounts_%' (0 if absent)
    PASS if bronze_distinct == silver_distinct + quarant_accts

G4 (INV-16 — Gold Struct Shape Integrity):
    SELECT DISTINCT transactions_by_type FROM '/data/gold/daily_summary/data.parquet'
    For every row: struct must contain exactly the keys PURCHASE, PAYMENT, FEE, INTEREST.
    REFUND must be absent. No null or missing key on any row.
    PASS if all rows have the four expected keys and no other keys.

Record all results using the canonical check IDs above. These IDs are used verbatim in
VERIFICATION_CHECKLIST.md (Task 6.2). Do not rename or omit any check.
```

**Test cases:**

| Case | Scenario | Expected |
|---|---|---|
| B1 | Bronze transactions completeness | count matches CSV sum |
| B2 | Bronze accounts completeness | count matches CSV sum |
| B3 | Bronze transaction_codes completeness | count matches CSV |
| S1 | Conservation equation | bronze_d == silver_d + quarant_d for all 7 dates |
| S2 | No duplicate transaction_id | 0 duplicates |
| S3 | All Silver txn codes valid | 0 invalid codes |
| S4 | No null _signed_amount | 0 nulls |
| S5 | All quarantine reason codes valid | result set subset of pre-defined list |
| G1 | Gold daily — one row per resolvable date | count matches distinct Silver dates |
| G2 | Weekly total_purchases spot check | silver count == gold value for chosen week/account |
| G3 | Gold total_signed_amount spot check | ABS(gold - silver) < 0.001 for two dates |
| I1 | Bronze idempotency | counts identical after re-run |
| I2 | Silver idempotency | counts identical after re-run |
| I3 | Quarantine idempotency | counts identical after re-run |
| I4 | Gold idempotency + watermark | counts identical and watermark unchanged after re-run |
| A1 | Bronze audit completeness | 0 null _pipeline_run_id across all Bronze entities |
| A2 | Silver audit completeness | 0 null _pipeline_run_id across all Silver entities |
| A3 | Gold audit completeness | 0 null _pipeline_run_id across both Gold files |
| A4 | Run log traceability | 0 Silver _pipeline_run_ids missing from run_log SUCCESS entries |
| S6 | Account conservation (INV-15) aggregate | bronze_distinct_accounts = silver_distinct + quarantined_accounts |
| G4 | Gold struct shape (INV-16) | every row has exactly PURCHASE, PAYMENT, FEE, INTEREST; REFUND absent |

**Verification command:**
```bash
docker compose run --rm pipeline python -c "
import duckdb

# B1
bronze_txn = duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/bronze/transactions/*/*.parquet')\").fetchone()[0]
source_txn = sum(duckdb.execute(f\"SELECT COUNT(*) FROM read_csv_auto('/source/transactions_2024-01-0{i}.csv')\").fetchone()[0] for i in range(1,8))
assert bronze_txn == source_txn, f'B1 FAIL: {bronze_txn} != {source_txn}'; print('B1 PASS')

# S1
for d in [f'2024-01-0{i}' for i in range(1,8)]:
    br = duckdb.execute(f\"SELECT COUNT(*) FROM '/data/bronze/transactions/date={d}/data.parquet'\").fetchone()[0]
    try: sv = duckdb.execute(f\"SELECT COUNT(*) FROM '/data/silver/transactions/date={d}/data.parquet'\").fetchone()[0]
    except: sv = 0
    try: qu = duckdb.execute(f\"SELECT COUNT(*) FROM '/data/silver/quarantine/date={d}/rejected.parquet' WHERE _source_file LIKE 'transactions%'\").fetchone()[0]
    except: qu = 0
    assert br == sv + qu, f'S1 FAIL {d}: {br} != {sv}+{qu}'
print('S1 PASS')

# S2
total = duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/*/*.parquet')\").fetchone()[0]
distinct = duckdb.execute(\"SELECT COUNT(DISTINCT transaction_id) FROM read_parquet('/data/silver/transactions/*/*.parquet')\").fetchone()[0]
assert total == distinct, f'S2 FAIL: {total} rows, {distinct} distinct'; print('S2 PASS')

# S3
inv = duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/*/*.parquet') st WHERE st.transaction_code NOT IN (SELECT transaction_code FROM '/data/silver/transaction_codes/data.parquet')\").fetchone()[0]
assert inv == 0, f'S3 FAIL: {inv}'; print('S3 PASS')

# S4
nulls = duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/*/*.parquet') WHERE _signed_amount IS NULL\").fetchone()[0]
assert nulls == 0, f'S4 FAIL: {nulls}'; print('S4 PASS')

# G1
gd = duckdb.execute(\"SELECT COUNT(*) FROM '/data/gold/daily_summary/data.parquet'\").fetchone()[0]
sd = duckdb.execute(\"SELECT COUNT(DISTINCT transaction_date) FROM read_parquet('/data/silver/transactions/*/*.parquet') WHERE _is_resolvable = true\").fetchone()[0]
assert gd == sd, f'G1 FAIL: gold={gd} silver={sd}'; print('G1 PASS')

# A1
for label, q in [
    ('Bronze txn', \"SELECT COUNT(*) FROM read_parquet('/data/bronze/transactions/*/*.parquet') WHERE _pipeline_run_id IS NULL\"),
    ('Bronze acc', \"SELECT COUNT(*) FROM read_parquet('/data/bronze/accounts/*/*.parquet') WHERE _pipeline_run_id IS NULL\"),
    ('Bronze tc',  \"SELECT COUNT(*) FROM '/data/bronze/transaction_codes/data.parquet' WHERE _pipeline_run_id IS NULL\"),
]:
    n = duckdb.execute(q).fetchone()[0]; assert n == 0, f'A1 FAIL {label}: {n}'
print('A1 PASS')

# A4
untraceable = duckdb.execute(\"SELECT COUNT(DISTINCT _pipeline_run_id) FROM read_parquet('/data/silver/transactions/*/*.parquet') WHERE _pipeline_run_id NOT IN (SELECT run_id FROM '/data/pipeline/run_log.parquet' WHERE status='SUCCESS')\").fetchone()[0]
assert untraceable == 0, f'A4 FAIL: {untraceable}'; print('A4 PASS')

# S6 — INV-15 Account Promotion Conservation (aggregate form — v1.7)
bronze_distinct_acc = duckdb.execute(\"SELECT COUNT(DISTINCT account_id) FROM read_parquet('/data/bronze/accounts/*/*.parquet')\").fetchone()[0]
silver_distinct_acc = duckdb.execute(\"SELECT COUNT(*) FROM '/data/silver/accounts/data.parquet'\").fetchone()[0]
try: quarant_accts = duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('/data/silver/quarantine/*/*.parquet') WHERE _source_file LIKE 'accounts_%'\").fetchone()[0]
except: quarant_accts = 0
assert bronze_distinct_acc == silver_distinct_acc + quarant_accts, f'S6 FAIL: bronze_distinct={bronze_distinct_acc} != silver({silver_distinct_acc}) + quar({quarant_accts})'
print(f'S6 PASS — INV-15 aggregate account conservation holds (bronze_distinct={bronze_distinct_acc}, silver={silver_distinct_acc}, quar={quarant_accts})')

# G4 — INV-16 Gold Struct Shape Integrity
rows_g4 = duckdb.execute(\"SELECT transactions_by_type FROM '/data/gold/daily_summary/data.parquet'\").fetchall()
expected_keys = {'PURCHASE', 'PAYMENT', 'FEE', 'INTEREST'}
for row in rows_g4:
    struct = row[0]
    actual_keys = set(struct.keys()) if hasattr(struct, 'keys') else set(struct._fields)
    assert actual_keys == expected_keys, f'G4 FAIL: struct keys {actual_keys} != {expected_keys}'
    assert 'REFUND' not in actual_keys, 'G4 FAIL: REFUND must not be in struct'
print('G4 PASS — INV-16 struct shape correct on all rows')

print('Core checks PASS — run B2/B3/S5/G2/G3/I1-I4/A2/A3 manually per CC Prompt above.')
"
```

**Invariant enforcement:** This task validates all invariants end-to-end — INV-01 through INV-16.

**Regression classification:** REGRESSION-RELEVANT — these verification commands form the Phase 8 sign-off suite.

---

### Task 6.2 — VERIFICATION_CHECKLIST.md Production

**Description:** Produce `verification/VERIFICATION_CHECKLIST.md` from Task 6.1 results.

**CC Prompt:**
```
This is a documentation task. Do not write any new code.

Produce verification/VERIFICATION_CHECKLIST.md using the results recorded in Task 6.1.
All check IDs (B1–B3, S1–S6, G1–G4, I1–I4, A1–A4) are defined in the canonical check
table in Task 6.1. Use those IDs verbatim — do not rename, merge, or omit any check.
Do NOT use "Section 10" as a heading or label anywhere in this document.

Structure:

# VERIFICATION_CHECKLIST.md — Credit Card Transactions Lake
## Phase 8 System Sign-Off

### Invariant Verification

One row per invariant for INV-01 through INV-16:
  | Invariant ID | Name | Scope | Verification command | Result | Notes |
  All 16 rows must be present. Scope is GLOBAL or TASK-SCOPED per INVARIANTS.md.
  Result must be PASS or FAIL. Notes required if FAIL.

### Phase 8 Check Results

One row per check using the canonical IDs from Task 6.1:
  | Check ID | Category | Command | Result | Actual value |
  Rows in this order: B1, B2, B3, S1, S2, S3, S4, S5, S6, G1, G2, G3, G4, I1, I2, I3, I4, A1, A2, A3, A4
  All 21 rows must be present. Result must be PASS or FAIL.

### Sign-Off
[ ] All 16 invariants verified PASS
[ ] All 21 Phase 8 checks PASS
[ ] System matches ARCHITECTURE.md — no undocumented components
[ ] Regression suite committed to verification/REGRESSION_SUITE.sh

Engineer sign-off: _______________
Date: _______________

Leave sign-off fields blank — engineer fills at Phase 8 sign-off.
```

**Test cases:**

| Case | Scenario | Expected |
|---|---|---|
| TC-1 | File exists at correct path | verification/VERIFICATION_CHECKLIST.md present |
| TC-2 | All 16 invariants listed | 16 INV- entries |
| TC-3 | All 21 Phase 8 checks listed | B1–B3, S1–S6, G1–G4, I1–I4, A1–A4 all present by ID |
| TC-4 | No "Section 10" label in file | string "Section 10" does not appear |
| TC-5 | Sign-off fields blank | no pre-filled signature or date |

**Verification command:**
```bash
ls -la verification/VERIFICATION_CHECKLIST.md
grep -c "INV-" verification/VERIFICATION_CHECKLIST.md
for id in B1 B2 B3 S1 S2 S3 S4 S5 S6 G1 G2 G3 G4 I1 I2 I3 I4 A1 A2 A3 A4; do
  grep -q "| $id " verification/VERIFICATION_CHECKLIST.md || echo "MISSING: $id"
done
grep -c "Section 10" verification/VERIFICATION_CHECKLIST.md | grep -q "^0$" && echo "TC-4 PASS" || echo "TC-4 FAIL"
```

**Invariant enforcement:** None — documentation task.

**Regression classification:** NOT-REGRESSION-RELEVANT — documentation artifact.

---

### Task 6.3 — Regression Suite Assembly

**Description:** Collect all portable verification commands from REGRESSION-RELEVANT tasks and assemble into `verification/REGRESSION_SUITE.sh`.

**CC Prompt:**
```
This is an assembly task. Do not write any new code beyond the shell script.

Create verification/REGRESSION_SUITE.sh.

This script must:
1. Have a bash shebang: #!/bin/bash
2. Include a header comment: # Credit Card Transactions Lake — Regression Suite
3. Include a comment listing which tasks each block came from
4. Run the portable verification commands from these REGRESSION-RELEVANT tasks in order:
   Tasks 1.3, 1.4, 1.5, 2.1, 2.2, 2.3, 2.4, 3.1, 3.2, 3.3, 3.4, 3.5,
   4.1, 4.2, 4.3, 5.1, 5.2, 5.3, 5.4, 6.1
5. Each block exits with code 1 if any assertion fails, with a message naming the task.
6. Final line: echo "REGRESSION SUITE PASS" if all blocks completed without exit.

Make the script executable: chmod +x verification/REGRESSION_SUITE.sh

Non-portable commands: list them in a comment block at the bottom with the reason.
Do not silently omit any REGRESSION-RELEVANT task.
```

**Test cases:**

| Case | Scenario | Expected |
|---|---|---|
| TC-1 | Script is executable | chmod +x confirmed |
| TC-2 | All 20 REGRESSION-RELEVANT task commands included | 20 task blocks present |
| TC-3 | Script runs end-to-end on passing system | exits 0, prints "REGRESSION SUITE PASS" |

**Verification command:**
```bash
ls -la verification/REGRESSION_SUITE.sh
docker compose run --rm pipeline bash verification/REGRESSION_SUITE.sh
```

**Invariant enforcement:** None — assembly task.

**Regression classification:** NOT-REGRESSION-RELEVANT — this is the regression suite itself.

---

## Invariant Cross-Reference

| INV | Enforced in Tasks | Finding additions |
|---|---|---|
| INV-01 Conservation Equation | 3.3, 3.4, 5.3, 6.1 | F2 (corrupt partition gap); F3 (empty partition gap); F6 (inter-model rule parity) |
| INV-02 Sign Assignment Origin | 3.4, 6.1 | — |
| INV-03 Transaction Code Reference Validation | 3.3, 3.4, 6.1 | — |
| INV-04 Referential Isolation | 3.4, 4.1, 4.2, 6.1 | F5 (silver_accounts pre-check in 5.2) |
| INV-05 Audit Chain Continuity | 2.1, 2.2, 2.3, 3.1, 3.2, 3.3, 3.4, 4.1, 4.2, 5.4, 6.1 | F2 (re-read integrity); F3 (WARNING log entry) |
| INV-06 Run Log Append-Only | 1.5, 2.4, 3.5, 4.3, 5.1, 5.3 | F3 (WARNING entries use append_run_log) |
| INV-07 Bronze Immutability | 2.1, 2.2, 2.3 | F2 (corrupt partition skipped silently — now surfaces before skip decision) |
| INV-08 Atomic Pipeline Execution | 1.3, 1.4, 2.4, 3.5, 4.3, 5.1, 5.2 | F5 (accounts pre-check in 5.2 gates before phase calls) |
| INV-09 Watermark Hard-Lock | 5.1, 5.2 | F1 (explicit init + read-back verify in 5.1 and 5.2) |
| INV-10 Idempotency | 2.1, 2.2, 2.3, 3.2, 4.1, 4.2, 5.3 | — |
| INV-11 Tooling Boundary | 2.1, 2.2, 2.3, 3.1, 3.2, 3.3, 3.4 | — |
| INV-12 Gold Unique Key Enforcement | 4.1, 4.2 | — |
| INV-13 Source File Immutability | 1.4, 2.1, 2.2, 2.3 | — |
| INV-14 Transaction Codes Precedence | 3.5, 3.4 | G8 (Task 3.4 added — enforces via JOIN to silver_transaction_codes in the INVALID_TRANSACTION_CODE rejection rule; the phase-level halt if the file is absent or empty remains the primary guard and lives in Task 3.5) |
| INV-15 Account Promotion Conservation | 3.2, 3.3, 6.1 | G1 (added to Task 3.2 CC prompt, test cases, verification command; S6 check added to Task 6.1); v1.7 (S6 revised to aggregate form — per-date _source_file methodology incompatible with latest-wins model) |
| INV-16 Gold Struct Shape Integrity | 4.1, 6.1 | G2 (REFUND removed from struct definition; fixed four-key STRUCT_PACK pattern added; TC-5/TC-6 and G4 check added) |

---

## Regression Classification Summary

| Task | Classification |
|---|---|
| 1.1 Repository Scaffold | NOT-REGRESSION-RELEVANT |
| 1.2 Dockerfile and Docker Compose | NOT-REGRESSION-RELEVANT |
| 1.3 Config Validation Module | REGRESSION-RELEVANT |
| 1.4 Source File Pre-flight Check | REGRESSION-RELEVANT |
| 1.5 Run Log and Control Table Helpers | REGRESSION-RELEVANT |
| 2.1 Bronze Transaction Codes Loader | REGRESSION-RELEVANT |
| 2.2 Bronze Accounts Loader | REGRESSION-RELEVANT |
| 2.3 Bronze Transactions Loader | REGRESSION-RELEVANT |
| 2.4 Bronze Phase Function | REGRESSION-RELEVANT |
| 3.1 Silver Transaction Codes Model | REGRESSION-RELEVANT |
| 3.2 Silver Accounts Model | REGRESSION-RELEVANT |
| 3.3 Silver Quarantine Model | REGRESSION-RELEVANT |
| 3.4 Silver Transactions Model | REGRESSION-RELEVANT |
| 3.5 Silver Phase Function | REGRESSION-RELEVANT |
| 4.1 Gold Daily Summary Model | REGRESSION-RELEVANT |
| 4.2 Gold Weekly Account Summary Model | REGRESSION-RELEVANT |
| 4.3 Gold Phase Function | REGRESSION-RELEVANT |
| 5.1 Historical Pipeline Orchestrator | REGRESSION-RELEVANT |
| 5.2 Incremental Pipeline Orchestrator | REGRESSION-RELEVANT |
| 5.3 Idempotency Hardening | REGRESSION-RELEVANT |
| 5.4 Audit Trail Verification | HARNESS-CANDIDATE |
| 6.1 Phase 8 Verification Command Suite | REGRESSION-RELEVANT |
| 6.2 VERIFICATION_CHECKLIST.md Production | NOT-REGRESSION-RELEVANT |
| 6.3 Regression Suite Assembly | NOT-REGRESSION-RELEVANT |

---

v1.0 signed off: Vijal at 17/04/2026

v1.1 signed off: Vijal at 17/04/2026

v1.2 signed off: Vijal at 17/04/2026

v1.3 signed off: Vijal at 21/04/2026

v1.4 signed off: Vijal at 21/04/2026

v1.5 signed off: Vijal at 22/04/2026

v1.6 signed off: Vijal at 22/04/2026

v1.7 signed off: Vijal at 04/05/2026
