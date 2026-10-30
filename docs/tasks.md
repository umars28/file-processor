# Tasks

Status: `draft` → `ac-draft` → `ac-approved` → `done`

Owner: `AI` (AI writes, human reviews) or `human` (human writes, AI only reviews).

Order: T1 → T2 → T3 → T4 → T5 → T6 → T7 → T9 → T10 → T8

## T1 — Project skeleton

- Owner: AI
- Status: done
- ADR: 0003, 0004
- Scope: `go.mod`, package layout with the pipeline as an internal library and `cmd/` holding one binary with `run` (CLI), `serve` (HTTP), and `migrate` subcommands, Dockerfile, compose.yaml with PostgreSQL, `go.uber.org/goleak` dependency. `make check` passes on the empty skeleton.

### AC

- [ ] `make check` passes on a clean clone: lint, race tests, coverage at or above 80%, `govulncheck`, `gitleaks`.
- [ ] `go build ./cmd/fileproc` produces one binary with subcommands `run`, `serve`, and `migrate`; `fileproc <subcommand> -h` exits 0 and prints that subcommand's flags.
- [ ] Edge case: `fileproc` with no arguments, or with an unknown subcommand, prints usage to stderr and exits 2.
- [ ] Failure case: `fileproc migrate` and `fileproc serve` without `DATABASE_URL` exit 1 with a message naming the missing variable; no connection is attempted.
- [ ] `internal/pipeline` does not depend on `net/http`, `database/sql`, `pgx`, or the persistence package, enforced by a test that inspects `go list -deps`.
- [ ] `internal/pipeline` has a `TestMain` calling `goleak.VerifyTestMain`, and it passes.
- [ ] `docker compose up --build` starts PostgreSQL with a healthcheck and the app with `serve` only after the database is healthy; `GET /healthz` returns 200; the app container does not run as root (`docker inspect` shows a non-root `User`).

## T2 — Streaming CSV reader

- Owner: AI
- Status: draft
- ADR: 0001, 0002
- Scope: read a single file row by row with `csv.Reader`, strip a UTF-8 byte order mark, validate the header, record the physical line number of each row, emit rows in batches.

### AC

## T3 — Worker pool and backpressure

- Owner: human
- Status: draft
- ADR: 0001
- Scope: readers bounded by `--readers`, fixed pool of `--workers`, buffered channels between stages, single aggregator owning all maps, amounts as `int64`.

### AC

## T4 — Cancellation and graceful shutdown

- Owner: human
- Status: draft
- ADR: 0001
- Scope: `context` driven by OS signals, channels closed only by their senders, all goroutines stop on both normal finish and cancel, no goroutine leaks. A cancelled run keeps file reports and drops aggregates.

### AC

## T5 — Per-row and per-file error handling

- Owner: human
- Status: draft
- ADR: 0002
- Scope: typed `RowError` and `FileError` with sentinel kinds, first 100 row errors per file plus complete counts per kind, file statuses, file completion via batch count, CLI exit codes. Replaces the T1 "not implemented" stubs so exit 1 only means a `partial` or `failed` file; exit 2 for usage errors is documented next to the ADR 0002 codes in the README.

### AC

## T6 — Progress tracking

- Owner: AI
- Status: draft
- ADR: 0005
- Scope: counting reader for bytes read, snapshot built by the aggregator every 200 ms and published through `atomic.Pointer`, CLI progress on stderr.

### AC

## T7 — Aggregation and CLI output

- Owner: AI
- Status: draft
- ADR: 0001, 0003
- Scope: sales aggregated per outlet, product, and day; deterministic results regardless of scheduling; `run` subcommand prints results and reports as JSON.

### AC

## T8 — Benchmarks, data generator, README

- Owner: AI
- Status: draft
- ADR: -
- Scope: large dummy CSV generator, memory and throughput benchmarks per stage (reader, workers, aggregator), README covering the required deliverables, including the coverage report from `coverage.txt`, known limitations, and an `AI Usage` section from `docs/ai-log.md`.

### AC

## T9 — Database schema, migrations, persistence

- Owner: AI
- Status: draft
- ADR: 0004
- Scope: `goose` SQL migrations embedded in the binary for `jobs`, `job_files`, `row_errors`, `sales_daily`; `migrate` subcommand, run by Compose as a one-shot service that must complete before `app` starts; persistence package on `pgxpool` writing the final job result in one transaction with `CopyFrom`; integration tests against PostgreSQL.

### AC

## T10 — Job runner, HTTP API, OpenAPI

- Owner: AI
- Status: draft
- ADR: 0003, 0005
- Scope: background job runner with a bounded number of concurrent jobs, paths confined to the configured data directory, jobs left `running` marked `failed` on startup; `serve` subcommand with `POST /jobs`, `GET /jobs/{id}`, `GET /jobs/{id}/report`, `GET /jobs/{id}/results` on `net/http`; OpenAPI spec in `docs/openapi.yaml`.

### AC
