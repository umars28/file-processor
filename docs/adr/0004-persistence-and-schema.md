# 0004 — Persistence layer and schema

- Status: accepted
- Date: 2026-10-30
- Amended: 2026-10-30 (T9: schema details)

## Context

ADR 0003 stores job state, file reports, capped row errors, and daily aggregates in PostgreSQL, written when a job changes status and once in a single transaction at the end. This ADR decides how the Go code talks to PostgreSQL, how migrations are managed, and the shape of the schema.

Constraints:

- The database is written a handful of times per job, never per row (ADR 0003).
- The final write can contain many rows: one per outlet × product × day, plus up to 100 row errors per file (ADR 0002). It must stay one transaction.
- Amounts are `int64` in the smallest currency unit (ADR 0001).
- The pipeline package must not import anything database-related.
- Migrations must run the same way locally, in Docker Compose, and in integration tests.
- Schema design and query efficiency are explicitly assessed.

## Options

### A. `database/sql` with `lib/pq`, `golang-migrate`

- Pros:
  - Standard library interface; widely known.
  - `golang-migrate` is the most common migration tool.
- Cons:
  - `lib/pq` is in maintenance mode.
  - Bulk insert means building large multi-row `INSERT` statements or many round trips.

### B. `pgx/v5` with `pgxpool`, hand-written SQL, `goose` with embedded SQL migrations

- Pros:
  - Actively maintained, PostgreSQL-native driver.
  - `CopyFrom` uses the COPY protocol, the fastest way to write the final batch of aggregates and errors inside one transaction.
  - Migrations are plain `.sql` files embedded in the binary with `embed`, so the app, Compose, and tests run the same files.
  - A small number of queries, all visible as SQL in the repository.
- Cons:
  - Query results are scanned by hand; mistakes in column order surface only at runtime, so every query needs an integration test.
  - Ties the persistence code to PostgreSQL.

### C. `pgx/v5` with `sqlc`, `goose`

- Pros:
  - Type-safe Go generated from SQL; column mismatches fail at generation time.
  - Same `CopyFrom` and migration benefits as B.
- Cons:
  - Extra code generation step in the build and in CI.
  - Generated code adds files to review that are not about the problem.

### D. GORM with auto-migrate

- Pros:
  - Least code for basic CRUD.
- Cons:
  - Schema lives in struct tags instead of reviewable migration files, which weakens the schema deliverable.
  - Bulk insert and transaction behavior are less explicit.
  - Hides the SQL that the assessment wants to see.

## Decision

B: `pgx/v5` with `pgxpool`, hand-written SQL, and `goose` with migrations embedded in the binary.

There are maybe eight queries in this whole project. That's too few to justify a code generator like sqlc, and an ORM would hide exactly the part I want people to see. With pgx I get `CopyFrom`, which is the right tool for the one heavy write at the end of a job: thousands of aggregate rows in one transaction without building a giant `INSERT`. The trade-off is that I scan rows by hand, so every query gets an integration test against a real PostgreSQL.

Migrations are plain SQL files run by `goose`. The binary has a `migrate` command, and Compose runs it before the API starts, so local, Docker, and tests all go through the same files.

The schema:

- `jobs`: `id uuid` primary key, `status` with a `CHECK` on the allowed values, `created_at`, `started_at`, `finished_at`, `error`.
- `job_files`: one row per input file, `job_id` foreign key with `ON DELETE CASCADE`, `path`, `status`, `total_rows`, `valid_rows`, `invalid_rows`, `last_line`, `error`, and `error_counts jsonb` for the count per error kind. Unique on `(job_id, path)`.
- `row_errors`: `job_file_id` foreign key, `line`, `column_name`, `kind`, `message`. At most 100 per file, enforced by the application.
- `sales_daily`: `job_id`, `outlet_id`, `product_id`, `sale_date`, `quantity bigint`, `amount bigint`, with the primary key `(job_id, outlet_id, product_id, sale_date)`.

When I wrote the migration I adjusted a few details:

- `sales_daily` also stores `transactions`, because the daily result from T7 counts them, and its primary key is `(job_id, sale_date, outlet_id, product_id)` so the results query reads rows in the order the API returns them.
- `job_files` keeps the file's `position` in the job (unique per job), stores `close_error` next to `error` (ADR 0002), and drops `total_rows`, which is just `valid_rows + invalid_rows`. Row errors use `(job_file_id, line)` as their key because a file has at most one error per line.
- A partial index on `jobs (status)` for `queued` and `running` serves the startup recovery query, which runs on every start.
- Job IDs come from `gen_random_uuid()` in PostgreSQL, so the Go side needs no UUID library.

Results are scoped to a job. Two jobs that read the same file produce two independent result sets; I'm not merging across jobs.

Indexes follow the endpoints: the primary key on `sales_daily` already serves "results for a job", and `job_files(job_id)` and `row_errors(job_file_id)` serve the report. I'm not adding anything else until a query needs it.

The persistence code lives in its own package behind a small interface. The pipeline never sees it; the HTTP adapter and the job runner do.

## Consequences

- Every query has an integration test against PostgreSQL (testcontainers or the Compose database).
- The schema and migration deliverable is the `migrations/` folder plus a short schema section in the README.
- The final write is a single transaction: job status, file reports, row errors, and `sales_daily` via `CopyFrom`. If it fails, the job is marked `failed` and nothing partial is visible.
- A cancelled job writes file reports and status but no `sales_daily` rows (ADR 0001).
- Moving off PostgreSQL would mean rewriting the persistence package, which is acceptable given ADR 0003.
