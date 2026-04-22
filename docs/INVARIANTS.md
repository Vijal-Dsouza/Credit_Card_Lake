# INVARIANTS.md — Credit Card Transactions Lake

## Changelog
| Version | Date | Author | Change |
|---|---|---|---|
| v1.1 | 2026-04-21 | Engineer | INV-05 updated — `_record_valid_from` enforcement point added for `silver_accounts` (ARCHITECTURE v1.2 addition). INV-15 added — Account Promotion Conservation (accounts Bronze→Silver+Quarantine path had no conservation invariant). INV-16 added — Gold Struct Shape Integrity (REFUND absent from `transactions_by_type` struct; architecture decision to fix four types documented and enforced). |
| v1.0 | 2026-04-16 | Engineer | Greenfield — Initial |

---

## How to Read This File

Each invariant carries:
- **Category:** Data Correctness | Operational
- **Scope:** GLOBAL (applies to every task) or TASK-SCOPED
  (applies only when the named components are touched)
- **Why this matters:** the concrete failure scenario if violated
- **Enforcement points:** where in the system this must be enforced

GLOBAL invariants go into Claude.md Section 2.
TASK-SCOPED invariants are embedded inline in the CC prompt of each
relevant task in EXECUTION_PLAN.md. They do not appear in Claude.md.

---

## INV-01 — Conservation Equation

**Category:** Data Correctness | **Scope:** TASK-SCOPED

For every date partition processed, the Bronze source row count must equal
the sum of Silver promoted rows and Quarantine rejected rows for that
partition. No record may appear in both Silver and Quarantine. No record
may be silently dropped at any point in the Silver promotion pipeline.

Additionally, no `transaction_id` may appear more than once across all
Silver transactions partitions combined. A `transaction_id` already present
in any Silver partition must be rejected to Quarantine with rejection code
DUPLICATE_TRANSACTION_ID — it must not enter Silver a second time under
any date partition.

**Why this matters:** A silent drop or double-count corrupts Gold
aggregations with no signal to the analyst. A cross-partition duplicate
`transaction_id` causes the same transaction to be counted and summed
twice in Gold — producing inflated figures with no visible error. Neither
failure surfaces without an explicit row count and uniqueness check against
Bronze.

**Enforcement points:**
- `silver_transactions` dbt model — promotion and quarantine write paths
  must be exhaustive and mutually exclusive
- Cross-partition deduplication — `transaction_id` checked against all
  existing Silver partitions before promotion; duplicate goes to Quarantine
- Post-promotion reconciliation: Bronze row count = Silver promoted +
  Quarantine rejected for each date partition

---

## INV-02 — Sign Assignment Origin

**Category:** Data Correctness | **Scope:** TASK-SCOPED

The numerical sign of a transaction amount in Silver and Gold must be
derived exclusively from the `debit_credit_indicator` field in the
`silver_transaction_codes` reference table. No hardcoded sign logic —
CASE statements, IF expressions, or Python-side mappings — is permitted
in any dbt model or Python code.

**Why this matters:** Hardcoded sign logic produces correct results for
anticipated codes but silently misassigns signs for any unanticipated code.
The error is invisible in Gold query output — an analyst summing signed
amounts has no signal that the signs are wrong.

**Enforcement points:**
- `silver_transactions` dbt model — `_signed_amount` expression must be
  a join-derived lookup from `silver_transaction_codes.debit_credit_indicator`,
  not an inline conditional expression

---

## INV-03 — Transaction Code Reference Validation

**Category:** Data Correctness | **Scope:** TASK-SCOPED

During Silver transactions promotion, transaction_code validation must be
performed exclusively via a join to the `silver_transaction_codes` reference
table. No hardcoded code lists or inline enumeration logic is permitted in
any dbt model or Python code.

**Why this matters:** Validation against a hardcoded list would silently
accept codes not in the reference or reject valid ones — producing corrupt
Silver data with no visible error signal.

**Enforcement points:**
- `silver_transactions` dbt model — INVALID_TRANSACTION_CODE rejection
  logic must reference `silver_transaction_codes` via join, not a literal
  enumeration or inline CASE expression

---

## INV-04 — Referential Isolation

**Category:** Data Correctness | **Scope:** TASK-SCOPED

Transactions with an `account_id` not present in `silver_accounts` at
promotion time must enter Silver with `_is_resolvable = false`. They must
never be quarantined for this condition alone. They must be strictly
excluded from all Gold aggregations — both `gold_daily_summary` and
`gold_weekly_account_summary` — without exception.

**Why this matters:** Quarantining these records permanently removes
potentially valid transactions from the audit trail. Allowing them into
Gold inflates aggregations with unverified data. Neither failure surfaces
in normal query output.

**Enforcement points:**
- `silver_transactions` dbt model — account_id join logic must produce
  `_is_resolvable = false` for unmatched records, not a quarantine write
- `gold_daily_summary` dbt model — must filter `WHERE _is_resolvable = true`
  before any aggregation
- `gold_weekly_account_summary` dbt model — must filter
  `WHERE _is_resolvable = true` before any aggregation

---

## INV-05 — Audit Chain Continuity

**Category:** Data Correctness | **Scope:** GLOBAL

Every record written to Bronze, Silver, and Gold must carry all three
non-null audit columns: `_pipeline_run_id`, `_source_file`, and
`_ingested_at` (or `_bronze_ingested_at` in Silver and Gold where
carried forward from Bronze). Every `_pipeline_run_id` appearing in any
layer record must have a corresponding entry in `pipeline/run_log.parquet`
with `status = SUCCESS` for that model. No layer record may exist whose
`_pipeline_run_id` cannot be traced to a successful run log entry.

**Why this matters:** A null `_pipeline_run_id` breaks the audit chain —
an analyst cannot trace a Gold aggregate back to its source promotion
event. A null `_source_file` means a Bronze record cannot be traced back
to its origin extract file. A null `_ingested_at` removes the temporal
audit marker. All three breaks are invisible in query output and only
surface during an explicit audit.

**Enforcement points:**
- Bronze loader — all three audit columns injected as non-null at write
  time for every record; `_source_file` populated from the source CSV
  filename; `_ingested_at` populated from the current timestamp at
  ingestion time
- All Silver dbt models — `_pipeline_run_id` sourced from active run
  context; `_source_file` and `_bronze_ingested_at` carried forward from
  Bronze without modification
- `silver_accounts` dbt model — `_record_valid_from` must be non-null for
  every record; set to `_ingested_at` from Bronze on initial insert and to
  the pipeline's promotion timestamp on upsert; never null, never carried
  forward unchanged on an upsert that replaces an existing record
- All Gold dbt models — `_pipeline_run_id` sourced from active run context
- Run log write path — entry written with `status = SUCCESS` only after
  model completion; entry written with `status = FAILED` on any model
  failure before pipeline exit

---

## INV-06 — Run Log Append-Only

**Category:** Data Correctness | **Scope:** GLOBAL

`pipeline/run_log.parquet` must only ever be written to by appending new
rows. No pipeline code path may truncate, overwrite, or delete existing
rows in the run log under any circumstances — including re-runs, failure
recovery, or idempotency enforcement.

**Why this matters:** Truncating the run log destroys the audit history
for all prior runs. Every `_pipeline_run_id` in Bronze, Silver, and Gold
becomes a dangling pointer with no traceable source. The destruction is
silent and irreversible.

**Enforcement points:**
- Run log write path in `pipeline.py` — must use append mode exclusively;
  never called with overwrite or truncate semantics; no code path
  conditionally rewrites the file on re-run

---

## INV-07 — Bronze Immutability

**Category:** Data Correctness | **Scope:** TASK-SCOPED

Once a Bronze partition is successfully written, it must never be
overwritten, appended to, or deleted by the pipeline under any
circumstances. If a Bronze partition path already exists, the loader must
skip ingestion for that date and leave the existing partition untouched.

**Why this matters:** Overwriting a Bronze partition breaks the audit
trail — Silver and Gold records with run_ids from the original write would
reference source data that no longer matches. The corruption is invisible
without an explicit Bronze-to-Silver lineage check.

**Enforcement points:**
- Bronze loader — partition existence check must be unconditional; skip
  logic executes without exception if partition path exists
- No code path in `pipeline.py` may call delete or overwrite operations
  on any path under `data/bronze/`

---

## INV-08 — Atomic Pipeline Execution

**Category:** Operational | **Scope:** GLOBAL

A pipeline run is all-or-nothing for a given date. If any phase —
Bronze, Silver, or Gold — fails for a date, the pipeline must exit with
a non-zero error code, log the failure to `pipeline/run_log.parquet`
with `status = FAILED`, and leave the watermark unchanged. The pipeline
must never advance to a subsequent phase after a prior phase returns
failure.

Source file presence for the target date or date range is a precondition
of pipeline execution. If any required source file is absent, the pipeline
must fail at startup validation — before any phase function runs — log
the failure, and exit with a non-zero error code.

**Why this matters:** A partially successful run leaves Bronze populated
for a date but Silver and Gold absent. The unchanged watermark means the
date will be reprocessed — but the Bronze partition is already present
and will be skipped by INV-07, so Silver and Gold will run against
existing Bronze data on retry. This is recoverable — but the failure must
be surfaced explicitly, not silently accepted as a valid terminal state.

**Enforcement points:**
- `pipeline.py` startup validation — source file existence check runs
  before any phase function is invoked; missing file triggers immediate
  exit with non-zero code and FAILED run log entry
- `pipeline.py` phase-gating logic — orchestrator must not invoke the
  next phase if the prior phase returned failure; each phase function
  returns a structured `PhaseResult`
- Exit code must be non-zero on any phase failure
- Run log entry with `status = FAILED` written before pipeline exit on
  any failure path

---

## INV-09 — Watermark Hard-Lock

**Category:** Operational | **Scope:** TASK-SCOPED

The `last_processed_date` in `pipeline/control.parquet` may only be
advanced to Date-N after all three layer phases for Date-N have completed
with `status = SUCCESS` in the run log. The watermark write must be the
final operation of any successful pipeline run. The watermark must never
be advanced on a run that produced any `status = FAILED` run log entry.

**Why this matters:** A watermark advanced before Gold completes means
that date is permanently skipped on the next incremental run. The missing
Gold data only surfaces when an analyst queries for that date — which may
be days later.

**Enforcement points:**
- Watermark advance logic in `pipeline.py` — structurally unreachable
  until all three phase functions return success
- Watermark write is the last statement before pipeline exit on the
  success path
- No code path advances the watermark on the failure exit path

---

## INV-10 — Idempotency

**Category:** Operational | **Scope:** GLOBAL

Re-executing the pipeline for any date or date range against an existing
destination must produce output identical to the initial successful
execution — identical row counts, identical record content, identical
Gold aggregations. This must hold for both the historical and incremental
pipelines.

**Why this matters:** Non-idempotent behaviour on re-run produces changed
Gold aggregations with no signal to the analyst. An analyst re-querying
after a re-run cannot distinguish a legitimate data change from a pipeline
defect.

**Enforcement points:**
- Bronze loader skip logic — INV-07 enforces Bronze idempotency
- `silver_transactions` dbt model — cross-partition deduplication on
  `transaction_id` must produce identical Silver output for identical
  Bronze input on every run
- `silver_accounts` dbt model — upsert on `account_id` must produce
  identical latest-record output for identical Bronze input on every run
- Both Gold dbt models — must produce deterministic output for a given
  Silver input on every run

---

## INV-11 — Tooling Boundary

**Category:** Operational | **Scope:** GLOBAL

Bronze ingestion must be implemented exclusively in Python with DuckDB.
Silver and Gold transformations must be implemented exclusively as dbt
models. No dbt model may reference the `source/` directory directly. No
Python code in `pipeline.py` may write records to any path under
`data/silver/` or `data/gold/`.

**Why this matters:** A dbt model reading `source/` CSVs directly
bypasses the Bronze audit column injection — records enter Silver with
no `_source_file`, `_ingested_at`, or `_pipeline_run_id`. Python writing
Silver directly bypasses the dbt run log entry for that model. Both
breaks are silent.

**Enforcement points:**
- All dbt model `source()` references — must point to Bronze Parquet
  paths under `data/bronze/`, never to `source/`
- `pipeline.py` — no DuckDB write statements targeting `data/silver/`
  or `data/gold/`; dbt models are the exclusive write mechanism for
  Silver and Gold

---

## INV-12 — Gold Unique Key Enforcement

**Category:** Data Correctness | **Scope:** TASK-SCOPED

`gold_daily_summary` must contain exactly one row per distinct
`transaction_date`. `gold_weekly_account_summary` must contain exactly
one row per `(week_start_date, account_id)` combination. A dbt model run
that produces duplicate keys must fail — it must not write output and
must return a non-zero status to the orchestrator.

**Why this matters:** A duplicate row in Gold daily summary means an
analyst summing `total_transactions` gets double-counted results with no
visible error. The corruption is only detectable with an explicit
deduplication check against the key columns.

**Enforcement points:**
- Both Gold dbt models — `unique_key` configuration must be set in dbt
  model config
- dbt test suite — `unique` and `not_null` tests on key columns must be
  defined and must run as part of model execution
- Test failure must propagate as a non-zero exit code to `pipeline.py`

---

## INV-13 — Source File Immutability

**Category:** Operational | **Scope:** GLOBAL

No pipeline code path — in `pipeline.py` or any dbt model — may write
to, modify, append to, or delete any file under the `source/` directory.
Source CSV files are read-only inputs. The pipeline may only open them
in read mode.

**Why this matters:** A pipeline accidentally overwriting or deleting a
source CSV is irreversible — the original extract is permanently lost.
The corruption is not visible until someone attempts to reprocess that
date and finds the source file missing or altered. By that point the
original data cannot be recovered.

**Enforcement points:**
- Bronze loader — source CSV files opened in read mode only; no write,
  append, or delete operations on any path under `source/`
- No dbt model may reference `source/` as a write target
- `pipeline.py` — no file system operations targeting `source/` other
  than read and existence checks

---

## INV-14 — Transaction Codes Precedence

**Category:** Data Correctness | **Scope:** TASK-SCOPED

`silver_transaction_codes` must be confirmed present and non-empty before
any `silver_transactions` promotion runs for any date — in both the
historical and incremental pipelines. If `silver_transaction_codes` is
absent or empty at the point of transactions promotion, the pipeline must
halt with a non-zero error code before promoting any transaction records.

**Why this matters:** If `silver_transaction_codes` is absent or empty
at promotion time, every transaction fails the INVALID_TRANSACTION_CODE
check and is quarantined. Silver transactions is empty. Gold is empty.
The pipeline reports success with zero records promoted and zero errors
thrown — a silent total failure indistinguishable from a legitimate
zero-transaction day.

**Enforcement points:**
- Historical pipeline — transaction codes Bronze load and Silver promotion
  must complete and be confirmed non-empty before accounts or transactions
  files are processed for any date
- `silver_transactions` dbt model — must include a pre-execution check
  confirming `silver_transaction_codes` row count > 0; halt if check fails
- Incremental pipeline — must confirm `silver_transaction_codes` is
  present and non-empty at startup validation before Bronze phase begins

---

## INV-15 — Account Promotion Conservation

**Category:** Data Correctness | **Scope:** TASK-SCOPED

For every date partition processed, every Bronze accounts record must exit
the `silver_accounts` promotion step as either a successfully upserted
Silver record or a Quarantine record. No account record may be silently
dropped at any point in the accounts promotion pipeline.

**Why this matters:** A silently dropped account record means that
account's latest state is never reflected in Silver. Any transaction
referencing that `account_id` will permanently carry `_is_resolvable =
false` and be excluded from all Gold aggregations — with no visible error
signal. The loss only surfaces if an analyst explicitly cross-checks
Bronze accounts row counts against Silver.

**Enforcement points:**
- `silver_accounts` dbt model — promotion and quarantine write paths must
  be exhaustive and mutually exclusive; every Bronze accounts record for
  a given date must be accounted for in exactly one of: upsert to Silver
  or write to Quarantine
- Post-promotion reconciliation: Bronze accounts row count for each date
  partition must equal Silver upserted records plus Quarantine rejected
  records for that partition

---

## INV-16 — Gold Struct Shape Integrity

**Category:** Data Correctness | **Scope:** TASK-SCOPED

The `transactions_by_type` STRUCT in `gold_daily_summary` must contain
exactly four keys: PURCHASE, PAYMENT, FEE, INTEREST. This set is fixed
to the four transaction types present in `silver_transaction_codes` for
this exercise. REFUND is explicitly excluded — it is defined in the
requirements brief as a valid transaction type in the source system but
is not present in the seed data for this exercise and is therefore out of
scope for the struct definition.

Every row in `gold_daily_summary` must contain all four keys regardless
of whether transactions of that type occurred on the given day. Keys with
no transactions must carry `count = 0` and `sum_signed_amount = 0.00`.
A row with a missing key or an inconsistent key set across rows must not
be written to Gold.

**Why this matters:** A struct with a variable key set — rows with
different keys depending on which transaction types occurred that day —
breaks any analyst query that references a specific key by name. The
query succeeds on days where the key is present and returns null or errors
on days where it is absent. The inconsistency is invisible without an
explicit struct key inspection across all rows.

**Enforcement points:**
- `gold_daily_summary` dbt model — `transactions_by_type` STRUCT must be
  constructed with all four keys explicitly; missing-type counts and sums
  must be coalesced to 0 and 0.00 respectively rather than omitted
- dbt test suite — a test must assert that every row in
  `gold_daily_summary` contains all four expected struct keys
- The key set must not be derived dynamically from the distinct values in
  `silver_transaction_codes` at model execution time — it must be the
  fixed four-key definition stated above

---

## Invariant Classification Summary

| ID | Name | Category | Scope |
|---|---|---|---|
| INV-01 | Conservation Equation | Data Correctness | TASK-SCOPED |
| INV-02 | Sign Assignment Origin | Data Correctness | TASK-SCOPED |
| INV-03 | Transaction Code Reference Validation | Data Correctness | TASK-SCOPED |
| INV-04 | Referential Isolation | Data Correctness | TASK-SCOPED |
| INV-05 | Audit Chain Continuity | Data Correctness | GLOBAL |
| INV-06 | Run Log Append-Only | Data Correctness | GLOBAL |
| INV-07 | Bronze Immutability | Data Correctness | TASK-SCOPED |
| INV-08 | Atomic Pipeline Execution | Operational | GLOBAL |
| INV-09 | Watermark Hard-Lock | Operational | TASK-SCOPED |
| INV-10 | Idempotency | Operational | GLOBAL |
| INV-11 | Tooling Boundary | Operational | GLOBAL |
| INV-12 | Gold Unique Key Enforcement | Data Correctness | TASK-SCOPED |
| INV-13 | Source File Immutability | Operational | GLOBAL |
| INV-14 | Transaction Codes Precedence | Data Correctness | TASK-SCOPED |
| INV-15 | Account Promotion Conservation | Data Correctness | TASK-SCOPED |
| INV-16 | Gold Struct Shape Integrity | Data Correctness | TASK-SCOPED |

**GLOBAL invariants (→ Claude.md Section 2):**
INV-05, INV-06, INV-08, INV-10, INV-11, INV-13

**TASK-SCOPED invariants (→ embedded in EXECUTION_PLAN.md task prompts):**
INV-01, INV-02, INV-03, INV-04, INV-07, INV-09, INV-12, INV-14, INV-15, INV-16

---

## What Was Reclassified and Why

| Proposed | Decision | Reason |
|---|---|---|
| No external network calls | Implementation guidance | Violation immediately visible as a connection error — does not pass harm/detectability test. Enforce via Claude.md Scope Boundary and task prompts. |
| No concurrent pipeline execution | Documented assumption | System cannot detect or enforce it — no locking mechanism exists. Remains in ARCHITECTURE.md Section 5 as a stated assumption. |
| Output format uniformity (Parquet) | Implementation guidance | Violation immediately visible — downstream DuckDB read fails on non-Parquet input. Enforce via task prompts. |
| File naming format | Implementation guidance | Violation immediately visible — pipeline exits with file-not-found. Enforce via Bronze loader task prompt. |
| REFUND in `transactions_by_type` struct | Explicit exclusion (INV-16) | REFUND is defined in the brief as a valid source transaction type but is absent from the seed data for this exercise. The struct key set is fixed to the four types present in seed data. This is a deliberate scope decision — not a dynamic derivation. Enforced as a fixed four-key definition in INV-16. |

---

## Engineer Sign-Off

I confirm this invariant set is accurate to my current understanding.
Phase 3 may surface new information not reflected here — any additions
or amendments will be versioned and documented in the changelog above.

**Signed:** Vijal 
**Date:** 21/04/2026
