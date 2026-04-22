# ARCHITECTURE.md — Credit Card Transactions Lake

## Changelog
| Version | Date | Author | Change |
|---|---|---|---|
| v1.2 | 2026-04-21 | Engineer | Section 8 updated — closing_balance join logic added to Gold Weekly Account Summary; _record_valid_from audit column documented in Silver Accounts; transactions_by_type STRUCT field documented in Gold Daily Summary |
| v1.1 | 2026-04-16 | Engineer | Decision 3 updated — source file presence added as startup pre-flight check alongside .env validation |
| v1.0 | 2026-04-14 | Engineer | Greenfield — Initial |

---

## 1. Problem Framing

### What this system solves
A financial services client needs a structured, queryable data lake that ingests
daily CSV extract files, enforces defined quality rules at each layer boundary,
and produces Gold-layer aggregations analysts can query with confidence via DuckDB.
The system replaces direct analyst access to raw extract files, which produces
inconsistent results and provides no audit trail.

### What this system explicitly does not solve
- Risk computation or credit decisions
- Modification of source system records
- Backfill of specific historical dates to correct errors
- SCD Type 2 history for account attribute changes
- Transaction code dimension changes during pipeline operation
- Streaming or near-realtime ingestion
- A serving API layer — Gold is queried directly via DuckDB CLI
- Schema evolution
- Data encryption at rest or in transit
- Production deployment, monitoring, or alerting infrastructure
- Resolution of _is_resolvable = false records

### What success looks like
Analysts query Gold-layer Parquet files via DuckDB and receive consistent,
auditable aggregations. Every Gold record is traceable to the Silver record that
produced it, and every Silver record is traceable to the Bronze record and source
file it came from. Running the pipeline twice on the same input produces identical
output at every layer.

---

## 2. Key Design Decisions

### Decision 1 — Layer-Gated Orchestrator (Architecture 2)

**What was decided:** `pipeline.py` is structured as three discrete phase functions
— `run_bronze_phase()`, `run_silver_phase()`, `run_gold_phase()` — each returning
a structured `PhaseResult`. The orchestration block calls each phase in sequence
and gates progression on the prior phase returning success. The watermark advance
is structurally unreachable unless all three phases return success.

**Rationale:** The watermark integrity guarantee — watermark never advances on
partial success — is enforced by structure rather than exception handling discipline.
A flat sequential orchestrator (Architecture 1) satisfies the same rule but makes
it dependent on correct exception wrapping around every dbt invocation. The
layer-gated approach makes the guarantee structurally impossible to accidentally
bypass. Per-model run log entries are a natural output of phase functions that
already know what models they ran and what they produced.

**Alternatives rejected:**
- Architecture 1 (Flat Sequential): Watermark integrity depends on exception
  handling discipline rather than structure. A future model addition requires
  manually adding correct exception wrapping. Rejected — structural enforcement
  is meaningfully stronger for the same implementation effort.
- Architecture 3 (dbt-Centric): Pipeline state management — watermark, run log,
  control table — pushed into dbt hooks or models. These are pipeline concerns,
  not transformation concerns. Creates hidden coupling between Python Bronze
  loader results and dbt hook logic. Rejected — pipeline state management belongs
  in the Python orchestrator where it is transparent and independently testable.

**Strongest argument against this decision:** Architecture 2 produces more code
surface than Architecture 1 — phase functions, a PhaseResult contract, and gate
logic. For a training system built and maintained by one engineer, that additional
complexity carries a documentation burden. Rejected: the structural watermark
guarantee and natural run log production outweigh the additional code surface.

### Decision 2 — Pipeline Mode via .env File

**What was decided:** The pipeline reads `PIPELINE_MODE` from a `.env` file at
startup. Valid values are `historical` and `incremental`. For historical mode,
`START_DATE` and `END_DATE` are also read from `.env`. For incremental mode,
the next date is derived from the watermark in the control table.

**Rationale:** The brief requires the pipeline to run end-to-end from
`docker compose up` with no manual steps beyond providing a `.env` file. All
pipeline configuration must therefore live in `.env`. Separate Compose service
definitions per mode were rejected — a single service with mode-driven branching
keeps the invocation interface simple and consistent.

**Alternatives rejected:** Command-line arguments passed at `docker compose up`
time — requires manual step beyond `.env` file. Separate Compose service
definitions — duplicates configuration and complicates the startup interface.

**Strongest argument against:** `.env`-driven mode switching is less explicit
than a named CLI command. An engineer who forgets to update `PIPELINE_MODE`
before running will silently execute the wrong pipeline mode. Mitigated by:
startup validation that reads and logs `PIPELINE_MODE` before any pipeline
work begins.

### Decision 3 — Startup Validation Before Any Pipeline Work

**What was decided:** `pipeline.py` validates all required `.env` values before
any phase function runs. For historical mode: `START_DATE` and `END_DATE` must
be present, valid dates, and in the correct order. For incremental mode: the
control table must exist and contain a valid watermark. If any validation fails,
the pipeline exits with a clear error message before touching any data. For both
modes, all required source files for the target date or date range are validated
for existence before any phase function runs. If any required source file is
absent, the pipeline exits with a clear error message before touching any data.

**Rationale:** A pipeline that begins writing Bronze records and then discovers
a configuration error mid-run leaves partial state that the idempotency logic
must handle. Failing fast before any writes keeps the failure mode clean and
the error message unambiguous.

**Strongest argument against:** Adds a startup latency step. Immaterial for a
batch pipeline with no latency requirement.

### Decision 4 — Bronze Idempotency via Partition Existence Check

**What was decided:** Before writing any Bronze partition, the loader checks
whether the partition path already exists. If it does, the load is skipped
entirely — no read, no write, no deduplication logic. The existing partition
is left untouched.

**Rationale:** Bronze partitions are immutable after initial write. The correct
implementation of "loading the same source file twice must not create duplicate
records" is not append-with-dedup — it is to not write at all if the partition
is already present. This is simpler, faster, and structurally enforces immutability.

**Strongest argument against:** A corrupted partial Bronze partition — written
during a prior failed run — would be silently skipped on re-run rather than
corrected. Accepted: the brief does not require Bronze self-repair, and Bronze
corruption is an operational concern outside this system's scope.

### Decision 5 — Transaction Codes Loaded to Silver Before Any Transaction Processing

**What was decided:** The historical pipeline loads transaction codes to Bronze
and promotes them to Silver reference before processing any accounts or
transactions files. The incremental pipeline does not reload transaction codes —
the reference is treated as static after historical initialisation.

**Rationale:** Silver transaction promotion enforces the `INVALID_TRANSACTION_CODE`
quality rule — transaction_code must exist in Silver transaction_codes. If the
reference table is absent, every transaction record fails this check and is
quarantined, producing a silently empty Silver transactions table with no error
thrown. Loading transaction codes first is a data integrity guarantee, not a
constraint of convenience.

**Strongest argument against:** None material — the brief explicitly specifies
this sequencing.

### Decision 6 — Engineer-Owned Directory Structure and dbt Project Layout

**What was decided:** The companion scaffold repository referenced in the brief
is not available. The directory structure, dbt project layout, Docker Compose
configuration, and pipeline.py structure are designed here and owned by the
engineer.

**Rationale:** The brief assumes a scaffold exists. It does not. All structural
decisions that the scaffold would have fixed must be made explicit in this
architecture.

**Directory structure:**
```
project-root/
├── .env                                      # Pipeline configuration — not committed
├── docker-compose.yml
├── Dockerfile
├── pipeline.py                               # Primary orchestrator
├── source/                                   # Read-only source CSV files
│   ├── transactions_YYYY-MM-DD.csv           # 7 daily files
│   ├── accounts_YYYY-MM-DD.csv               # 7 daily delta files
│   └── transaction_codes.csv                 # Static reference — loaded once
├── data/                                     # Parquet outputs — bind-mounted
│   ├── bronze/
│   │   ├── transactions/date=YYYY-MM-DD/data.parquet
│   │   ├── accounts/date=YYYY-MM-DD/data.parquet
│   │   └── transaction_codes/data.parquet
│   ├── silver/
│   │   ├── transactions/date=YYYY-MM-DD/data.parquet
│   │   ├── accounts/data.parquet
│   │   ├── transaction_codes/data.parquet
│   │   └── quarantine/date=YYYY-MM-DD/rejected.parquet
│   ├── gold/
│   │   ├── daily_summary/data.parquet
│   │   └── weekly_account_summary/data.parquet
│   └── pipeline/
│       ├── control.parquet
│       └── run_log.parquet
└── dbt_project/                              # dbt project root
    ├── dbt_project.yml
    ├── profiles.yml
    └── models/
        ├── silver/
        │   ├── silver_transaction_codes.sql
        │   ├── silver_accounts.sql
        │   ├── silver_transactions.sql
        │   └── silver_quarantine.sql
        └── gold/
            ├── gold_daily_summary.sql
            └── gold_weekly_account_summary.sql
```

**Strongest argument against:** An engineer-designed structure may diverge from
production conventions. Accepted — this is a training system and the brief
explicitly removes the scaffold dependency.

### Decision 7 — Single Python Container with Bind-Mounted Data Directory

**What was decided:** Docker Compose defines a single service — a Python 3.11
container with DuckDB installed as a Python package. The `data/` and `source/`
directories are bind-mounted into the container. The container entrypoint invokes
`pipeline.py` directly. No additional services, no networking, no database server.

**Rationale:** DuckDB is an embedded library — it runs inside the Python process.
No separate DuckDB container or database server is required. The brief prohibits
external service calls. A single-service Compose file is the minimal correct
implementation.

**Strongest argument against:** Bind-mounting `data/` means Parquet files are
written to the host filesystem. On some systems, file permission conflicts between
the container user and the host user can cause write failures. Mitigated by
explicit user configuration in the Dockerfile.

---

## 3. Challenges to Decisions

| Decision | Strongest challenge | Assessment |
|---|---|---|
| Layer-gated orchestrator | More code surface than flat sequential | REJECTED — structural watermark guarantee outweighs code surface cost |
| .env mode switching | Silent wrong-mode execution if engineer forgets to update | REJECTED — startup validation catches and exits before any data is touched |
| Bronze existence check | Corrupted partial partition silently skipped | ACCEPTED — Bronze self-repair is outside system scope |
| Transaction codes first | None material | N/A |
| Engineer-owned structure | May diverge from production conventions | ACCEPTED — training system, scaffold not available |
| Single container | Host filesystem permission conflicts | MITIGATED — explicit Dockerfile user configuration |
| Source file pre-flight check | Adds a file-system scan before any pipeline work | REJECTED — cost is immaterial for a batch pipeline; benefit is a clean failure before any partial state is written |

---

## 4. Key Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Unresolvable account IDs produce permanent Gold exclusions | Analysts see incomplete Gold aggregations with no self-correcting mechanism | Flagged in Silver with _is_resolvable = false — documented limitation, backfill out of scope |
| Watermark not initialised before incremental run | Incremental pipeline has no watermark to read | Startup validation checks control table existence and exits with clear error |
| Transaction codes absent at Silver promotion | All transactions quarantined silently | Sequencing enforced: transaction codes Silver load is gated before any transaction Silver promotion |
| Source file missing for next incremental date | Pipeline exits without processing | Pre-flight validation catches absence before Bronze phase begins — logged as FAILED in run log, watermark unchanged, next run retries same date |
| Bronze partition partially written on prior failure | Existence check skips repair | Accepted limitation — Bronze self-repair outside scope |

---

## 5. Key Assumptions

- The 7-day source dataset is complete and correctly named per brief conventions
- `transaction_codes.csv` is static for the duration of this exercise
- DuckDB embedded mode is sufficient — no concurrent pipeline runs are expected
- The `.env` file is provided by the engineer before `docker compose up`
- Silver accounts latest-record-only simplification is acceptable for this training exercise
- Bind-mounted Parquet files on the host filesystem are sufficient for persistence

---

## 6. Open Questions

None — all open questions identified during Phase 1 interrogation are resolved
with concrete decisions above.

---

## 7. Future Enhancements (Parking Lot)

| Enhancement | Rationale for deferral |
|---|---|
| Backfill pipeline for specific historical dates | Requires dedicated watermark guard logic — explicitly out of scope in brief |
| SCD Type 2 for accounts | Significant additional complexity — brief explicitly defers this |
| Resolution pipeline for _is_resolvable = false records | Depends on backfill pipeline — deferred with it |
| Schema evolution handling | CSV schema is fixed for this exercise |
| Production monitoring and alerting | Out of scope per brief |
| Data encryption at rest | Out of scope per brief |

---

## 8. Data Model

### Source Entities

**Transactions** — append-only daily fact. One CSV per calendar day.
Primary key: `transaction_id`. Foreign keys: `account_id`, `transaction_code`.
Amount is always positive in source — sign assigned in Silver via
`debit_credit_indicator` from Transaction Codes.

**Transaction Codes** — static reference dimension. Single CSV loaded once.
Primary key: `transaction_code`. Authoritative source for sign assignment
and transaction type classification.

**Accounts** — daily delta. One CSV per calendar day containing only new
or changed records. Primary key: `account_id`. Silver maintains latest record
only — no history retained.

### Layer Entities

**Bronze** — immutable raw partitions. One Parquet partition per source
file per date. Audit columns added: `_source_file`, `_ingested_at`,
`_pipeline_run_id`. No transformations.

**Silver Transactions** — clean, validated, signed records partitioned by
source date. Deduplication enforced across all partitions on `transaction_id`.
Unresolvable account IDs flagged with `_is_resolvable = false` — not quarantined.

**Silver Accounts** — single non-partitioned file. Latest record per
`account_id`. Delta records upserted — no history retained.
Silver Accounts carries a _record_valid_from audit column (TIMESTAMP) recording when this version of the record became current in Silver. On initial insert this is set to _ingested_at from Bronze. On upsert it is set to the pipeline's promotion timestamp. This column does not reconstruct historical state — it records when the latest record arrived, not when the underlying account attribute changed in the source system.


**Silver Transaction Codes** — single reference file. Loaded once from
Bronze during historical initialisation.

**Silver Quarantine** — rejected records partitioned by source date.
Contains original source record plus rejection audit columns including
`_rejection_reason` from the pre-defined code list.

**Gold Daily Summary** — one record per calendar day. Computed exclusively
from Silver transactions where `_is_resolvable = true`.
The transactions_by_type column is a DuckDB STRUCT containing one entry per transaction_type in Silver Transaction Codes. Each entry holds count (INTEGER) and sum_signed_amount (DECIMAL) for that type on the given day. Types with zero transactions on a given day are included with count = 0 and sum_signed_amount = 0.00 to ensure a consistent struct shape across all rows. The set of keys is fixed to the four transaction types present in the seed data: PURCHASE, PAYMENT, FEE, INTEREST.

**Gold Weekly Account Summary** — one record per account per calendar week
(Monday–Sunday ISO). Only accounts with at least one resolvable transaction
in the week are included.
closing_balance is sourced from Silver Accounts (current_balance) by joining on account_id. Since Silver Accounts is non-partitioned and holds only the latest record, the join always returns the most recent available balance — there is no date-scoped lookup. This is a deliberate simplification: the value reflects the account's state at the time of Silver promotion, not necessarily at week_end_date. This limitation is consistent with the SCD Type 2 deferral documented in Section 7.

**Pipeline Control** — single Parquet file tracking the watermark for
incremental runs. Advances only after all three layers complete successfully.

**Pipeline Run Log** — append-only Parquet file. One row per dbt model
(or Bronze loader) per pipeline invocation. Never overwritten.

Signed off by Vijal - 21/04/2026