# file-processor

A concurrent processor for POS sales transaction CSV files, written in Go. It reads many files at once through a bounded worker pool, reports bad rows without stopping a file, shows live progress, and aggregates sales per outlet, product, and day.

It runs two ways:

- `fileproc run`: a CLI that processes local files and prints a JSON report.
- `fileproc serve`: an HTTP API that runs the same pipeline as background jobs and stores results in PostgreSQL.

Built for the Majoo Software Architect Engineer take-home, Test Case 1 (Concurrent Data Processing).

## Take-home sections

| Section | Repository |
|---|---|
| 1. Go programming test: concurrent file processing | [file-processor](https://github.com/umars28/file-processor) (this repo) |
| 2. System architecture: e-commerce platform | [ecommerce-system-design](https://github.com/umars28/ecommerce-system-design) |
| 3. Database design: social media platform | [social-media-db-design](https://github.com/umars28/social-media-db-design) |

## Contents

- [Quick start](#quick-start)
- [CLI](#cli)
- [HTTP API](#http-api)
- [Architecture](#architecture)
- [Technology choices](#technology-choices)
- [Database schema](#database-schema)
- [Testing and coverage](#testing-and-coverage)
- [Performance](#performance)
- [Known limitations](#known-limitations)
- [Future improvements](#future-improvements)
- [AI usage](#ai-usage)

## Quick start

Requirements: Docker with Compose, and Go 1.27.3 or newer for the local commands.

### With Docker Compose

```sh
docker compose up --build -d
curl -s localhost:8080/healthz
```

If you changed `APP_PORT`, replace 8080 in the `curl` commands below.

`db` starts PostgreSQL, `migrate` applies the schema once and exits, and `app` starts the API after that. Ports are bound to `127.0.0.1` only; set `APP_PORT` or `POSTGRES_PORT` if 8080 or 55432 are taken, for example `APP_PORT=18080 docker compose up --build -d`.

Generate a sample file into `./data` (mounted read-only at `/data` in the container; generating needs Go) and submit it. The container runs as a non-root user, so the file must be world-readable, which it is with the usual umask of 022:

```sh
go run ./cmd/gendata -rows 100000 -invalid-rate 0.01 -out data/sales.csv
curl -s -X POST localhost:8080/jobs -d '{"files": ["sales.csv"]}'
```

The response holds the job ID. Poll it, then read the report and results:

```sh
curl -s localhost:8080/jobs/<id>
curl -s localhost:8080/jobs/<id>/report
curl -s localhost:8080/jobs/<id>/results
```

### Locally, CLI only

```sh
go build -o bin/fileproc ./cmd/fileproc
go run ./cmd/gendata -rows 1000000 -invalid-rate 0.001 -out data/sales.csv
bin/fileproc run -progress data/sales.csv > results.json
```

The CLI needs no database. The summary and progress go to stderr; the JSON report goes to stdout.

## CLI

```
fileproc run [flags] <file.csv>...   process files and print a JSON report
fileproc serve [flags]               start the HTTP API
fileproc migrate                     apply database migrations
```

### Input format

```
transaction_id,outlet_id,product_id,quantity,amount,transacted_at
trx-1,outlet-3,product-41,2,15000,2026-10-30T09:15:00+07:00
```

- The header must match exactly; a leading UTF-8 byte order mark is ignored.
- `quantity` is an integer of at least 1; `amount` is a non-negative integer in the smallest currency unit (rupiah).
- `transacted_at` is RFC 3339. The sale date is the calendar date in the offset written in the record, so an outlet's day follows its own clock.

### `run` flags

| Flag | Default | Meaning |
|---|---|---|
| `-readers` | 4 | files read at the same time |
| `-workers` | number of CPUs | goroutines converting and validating rows |
| `-batch-size` | 512 | rows per message between stages |
| `-max-errors-per-file` | 100 | row errors kept per file (counts are always complete) |
| `-max-record-bytes` | 65536 | approximate largest record before the file fails |
| `-max-daily-keys` | 0 (no limit) | distinct (date, outlet, product) keys allowed before the run fails |
| `-progress` | off | print progress lines to stderr |

### File statuses and exit codes

| File status | Meaning |
|---|---|
| `ok` | every row valid |
| `ok_with_errors` | some rows invalid; the valid rows are counted |
| `partial` | the file broke after at least one data row was read (I/O error, record too large); rows read so far are counted |
| `failed` | the file broke before any data row (cannot open, wrong header, empty, first record too large) |
| `cancelled` | the run was cancelled while the file was being read |

| Exit code | When |
|---|---|
| `0` | every file is `ok` or `ok_with_errors` |
| `1` | at least one file is `partial` or `failed` |
| `2` | invalid usage or flags |
| `130` | the run was cancelled (Ctrl+C or SIGTERM) |

`fileproc run -h` prints the same table.

### Data generator

`cmd/gendata` writes realistic data for testing and benchmarks. The same flags and seed always produce the same file, and it streams, so ten million rows stay under 4 MB of heap.

```sh
go run ./cmd/gendata -rows 5000000 -outlets 50 -products 500 -days 30 -invalid-rate 0.01 -seed 1 -out data/big.csv
```

It prints how many rows of each invalid kind it wrote, which match what `fileproc run` reports for the file.

## HTTP API

The full contract is in [`docs/openapi.yaml`](docs/openapi.yaml), also served at `GET /openapi.yaml`.

| Method and path | Purpose |
|---|---|
| `POST /jobs` | submit `{"files": [...]}`, paths relative to `DATA_DIR`; answers `202` with `Location` |
| `GET /jobs/{id}` | status, timestamps, and live progress while running |
| `GET /jobs/{id}/report` | per-file statuses, counts, and kept row errors; `409` until finished |
| `GET /jobs/{id}/results` | totals and daily sales; `409` until finished |
| `GET /healthz` | liveness |

Errors are RFC 9457 `application/problem+json`. A full queue or a server shutting down answers `503` with `Retry-After`.

`serve` reads `DATABASE_URL`, `DATA_DIR` (default `./data`), and `HTTP_ADDR` (default `:8080`), plus flags `-addr` (overrides `HTTP_ADDR`), `-max-jobs` (2), `-queue-size` (100), and `-max-daily-keys` (1,000,000). On start it marks jobs left unfinished by a previous process as `failed`; on SIGTERM it stops accepting requests, cancels running jobs, saves them as `cancelled`, and exits 0.

## Architecture

### Pipeline

```
                 ┌──────────┐  batches   ┌──────────┐  parsed batches  ┌────────────┐
 sources ──────▶ │ R readers│ ─────────▶ │ N workers│ ───────────────▶ │ aggregator │ ──▶ Result
  (feeder)       │ csv +    │  (bounded  │ convert +│   (bounded       │ one owner  │     + Snapshot
                 │ header + │  channel)  │ validate │    channel)      │ of all maps│
                 │ size cap │            └──────────┘                  └────────────┘
                 └──────────┘                                                 ▲
                      │ file done (with batch count) ─────────────────────────┘
```

- One feeder hands out files; at most `R` are open at once.
- Each reader owns one `csv.Reader`, strips the byte order mark, checks the header, caps record size, and sends rows in batches with their physical line numbers.
- `N` workers parse and validate rows. Bad rows become typed row errors that travel inside the batch.
- One aggregator goroutine owns every total, daily sum, file report, and progress snapshot, so none of them need a lock. It knows a file is complete when the reader's final message and all of that file's batches have arrived.
- Channels are bounded, so a slow aggregator slows the workers, which slow the readers. Memory used for rows in flight is fixed by the channel sizes, whatever the file size. What does grow is the aggregator's map, by one entry per distinct (date, outlet, product) key; `serve` caps it at 1,000,000 keys by default.
- Only senders close channels. On cancel, producers stop at once and sources are closed. Batches already sent are still aggregated, which keeps results deterministic.

### Components

```
cmd/fileproc ──▶ internal/cli ──┬──▶ internal/pipeline   (no HTTP, no database: enforced by a test)
                                ├──▶ internal/report     (JSON document shared by CLI and HTTP)
                                ├──▶ internal/jobs       (queue, workers, os.Root confinement)
                                ├──▶ internal/httpapi    (net/http handlers, problem+json)
                                └──▶ internal/store      (pgx, goose migrations)
cmd/gendata  ──▶ internal/gendata
```

The decisions behind this are written down as ADRs:

| ADR | Decision |
|---|---|
| [0001](docs/adr/0001-concurrency-model.md) | row-batch pipeline: readers → worker pool → single aggregator; cancellation rules |
| [0002](docs/adr/0002-error-handling.md) | bounded per-file error report, file statuses, partial versus failed |
| [0003](docs/adr/0003-delivery-and-storage.md) | pipeline as a library behind a CLI and a thin HTTP API, PostgreSQL |
| [0004](docs/adr/0004-persistence-and-schema.md) | `pgx` with hand-written SQL, `goose` migrations, schema |
| [0005](docs/adr/0005-progress-tracking.md) | progress as immutable snapshots published by the aggregator |

## Technology choices

| Choice | Why |
|---|---|
| Go standard library for concurrency, CSV, HTTP (`net/http` routing) | goroutines, channels, and `encoding/csv` cover the whole pipeline, and `net/http` routing has had method and path patterns since Go 1.22; a framework would add dependencies without removing code |
| Single aggregator instead of mutex-protected maps | one owner means no lock contention, no data races, and deterministic results with `int64` sums (ADR 0001) |
| Byte-based progress through `atomic.Pointer` snapshots | row totals are unknown while streaming, file sizes are not; readers never block on a lock (ADR 0005) |
| `os.Root` for file access | confinement to the data directory is enforced by the OS, not by string checks; symlink escapes fail at open |
| PostgreSQL with `pgx` and `CopyFrom` | the final write is one transaction; COPY saves 25,000 daily rows and 10,000 errors in about 0.4 s (ADR 0004) |
| `goose` with embedded SQL migrations | the same files run locally, in Compose, and in tests |
| Distroless `nonroot` image | no shell, no package manager, not root |
| `golangci-lint` (with `gosec`), `govulncheck`, `gitleaks`, `goleak`, `-race` | one `make check` covers style, security, known CVEs, secrets, leaks, and races |

## Database schema

```
jobs        (id uuid PK, status CHECK, created_at, started_at, finished_at, error)
  └─< job_files   (id PK, job_id FK CASCADE, position, path, status CHECK,
  │                valid_rows, invalid_rows, last_line, error, close_error, error_counts jsonb,
  │                UNIQUE (job_id, position), UNIQUE (job_id, path))
  │      └─< row_errors (job_file_id FK CASCADE, line, column_name, kind, message,
  │                      PK (job_file_id, line))
  └─< sales_daily (job_id FK CASCADE, sale_date, outlet_id, product_id,
                   transactions, quantity, amount, PK (job_id, sale_date, outlet_id, product_id))
```

The migration is [`migrations/00001_init.sql`](migrations/00001_init.sql). Amounts are `bigint` in rupiah. A partial index on unfinished jobs serves the startup recovery query; the primary keys serve every read the API makes.

## Testing and coverage

```sh
docker compose up -d db
make check          # lint + race tests with coverage + govulncheck + gitleaks
make test-integration
make bench
make latency
```

`make latency` is a timing measurement, so it is not part of `make check`, which runs every package in parallel under `-race`; the 200 ms cancel bound is still checked there by the smaller T4 tests. `make check` needs the Compose database, because store, API, and end-to-end tests run against real PostgreSQL. Each DB-backed test package uses its own schema, so packages run in parallel. Plain `go test ./...` without `TEST_DATABASE_URL` skips the database tests.

What the tests cover beyond the happy path: 200 random-point cancellations per run with leak and race checks, determinism across repeated runs, memory bounds over millions of rows, a 300 MB single line and an unclosed quote, symlink and FIFO tricks against the data directory, slow clients, transactions that fail halfway, and an end-to-end check that HTTP results equal CLI output for the same files.

Coverage from the latest `make cover` (total 90.1%, minimum 80% enforced):

| Package | Coverage |
|---|---|
| `internal/pipeline` | 97.2% |
| `internal/jobs` | 94.7% |
| `internal/gendata` | 94.7% |
| `cmd/gendata` | 89.5% |
| `internal/httpapi` | 93.2% |
| `internal/report` | 92.0% |
| `internal/cli` | 89.5% |
| `internal/store` | 81.2% |
| `internal/config` | 100.0% |
| `internal/testdb` | 0.0%: a helper that sets up a schema for other packages' tests; it has no tests of its own |
| `cmd/fileproc` | 0.0%: a four-line `main`. A test builds and signals the real binary, but a subprocess is not instrumented, so it is exercised without being counted |

## Performance

Measured on an Apple M5 (10 cores, 24 GB) with Go 1.27.3, without `-race`. Benchmarks ran 5 times (`make bench`); the tables give the range.

| Benchmark | Result |
|---|---|
| Reader, one file | 273–280 MB/s |
| Row conversion and validation | 86–96 ns per row, 0 allocations |
| Aggregation, existing key (~48,000 keys) | 75–76 ns per transaction, 0 allocations |
| Aggregation, new key every time | 460–534 ns per transaction |
| Whole pipeline, 4 files × 250,000 rows | 116–191 MB/s, 1.8–2.9 million rows/s |

| Measurement | Result |
|---|---|
| Cancel to return with at least 100,000 distinct keys (`make latency`, 50 runs) | median 14 ms, worst 18–48 ms across runs (target 200 ms) |
| Reader, 1 million rows (`TestReadCSVMemoryStaysBounded`, 3 runs) | peak heap 3.8 MB |
| Pipeline, 2 million rows over 500 keys (`TestRunMemoryStaysBounded`, 3 runs) | peak heap 3.8–6.4 MB |
| Pipeline, 2 million rows over 25,000 keys (3 runs) | peak heap 19–22 MB |
| Generator, 10 million rows | peak heap 3.6–3.7 MB |
| Saving 25,000 daily rows and 10,000 row errors to PostgreSQL | 0.41 s (0.79 s with `-race`) |

Peak heap figures are sampled every few milliseconds, so short spikes can be missed; the tests assert hard bounds of 32 MB and 64 MB.

Where the time goes depends on the data. For one file, the cost is split between the reader (one `csv.Reader` per file, ADR 0001) and the aggregator inserting new keys, which is about 7 times more expensive than updating an existing one. One million rows with 25 distinct keys take 0.86 s and 21 MB of memory through `fileproc run`. The same number of rows with 552,663 keys (the generator's defaults) take 1.35 s and 761 MB, most of it the daily map and the JSON document. Many files scale across readers, and the aggregator remains a single goroutine.

## Known limitations

- Summed quantity and amount use `int64` in the pipeline without an overflow check. One key would have to exceed about 9.2 × 10¹⁸ rupiah to wrap; the store does detect overflow when it reads results back.
- No authentication or authorization on the HTTP API.
- No file upload: files must already be in the server's data directory.
- Jobs do not resume after a restart; unfinished jobs are marked `failed` on start.
- Duplicate `transaction_id`s, within or across files, are not deduplicated.
- Memory grows with the number of distinct (date, outlet, product) keys, and the CLI builds the whole JSON document in memory. `fileproc run` has no key limit by default (`-max-daily-keys 0`); `serve` stops a job at 1,000,000 keys.
- Single-node job runner: the queue lives in process memory.
- The header must match exactly, including column order.
- An unclosed quote in a small file swallows the rows after it into one syntax error (CSV quoting rules); in a large file the record size cap fails the file instead.
- Only regular files are accepted, because closing a FIFO or device does not interrupt a blocked read on every platform.
- Unknown CSV dialects (other separators, encodings other than UTF-8) are not supported.

## Future improvements

- Authentication and per-client rate limiting on the API.
- Upload or object storage sources (S3) instead of a shared directory.
- A durable queue so jobs survive restarts and can run on several nodes.
- Column mapping by header name instead of a fixed order.
- Checked addition with an explicit `overflow` status in the pipeline.
- Fewer allocations per row in the reader (record reuse with a pooled copy).
- Stream the JSON report instead of building it in memory, and spill the daily map to disk or the database for very high key counts.
- CI running `make check` on every pull request.

## AI usage

This project was built with Claude Code as a pair, under a workflow kept in this repository:

- [`CLAUDE.md`](CLAUDE.md): the rules for the AI (never commit or merge, no work without approved acceptance criteria, ADRs are not changed silently).
- [`.claude/commands`](.claude/commands): `/adr` drafts options and leaves the decision to the owner; `/ac` drafts acceptance criteria for approval; `/task` writes tests first, implements, and runs the reviewer; `/pr` opens a pull request after `make check`.
- [`.claude/agents/reviewer.md`](.claude/agents/reviewer.md): a separate reviewer agent that checks every task against its ADRs and acceptance criteria, runs the checks, and probes edge cases.

How the work was split: the owner made the architecture decisions (the five ADRs), approved every set of acceptance criteria, reviewed and merged every pull request, and ran the Docker checks. The AI drafted ADR options, acceptance criteria, tests, and code. The core concurrency tasks (worker pool, cancellation, error report) were owned by the human and drafted by the AI at the owner's request, which is recorded in the log.

Every AI suggestion that was rejected or changed is in [`docs/ai-log.md`](docs/ai-log.md): 45 entries. 6 came from the owner (5 from reviewing ADR drafts, 1 from running Docker Compose), 24 from the reviewer agent, and 15 from the AI's own checks or design notes. Some of the more instructive ones:

- ADR 0001's first draft claimed parsing was spread across workers. In fact `csv.Reader` tokenizes in the one reader per file, and that is now named as the throughput ceiling.
- Drafts used `float64` for amounts. `int64` in rupiah is required, because float addition order would make totals depend on scheduling.
- The CSV reader had no record size limit. A 300 MB line reached 1.2 GB of heap; with the cap the reviewer's re-run peaked at 1.6 MB (recorded in pull request #4).
- A pinned Go toolchain looked like a fix for reachable stdlib CVEs, but the official Docker image ignores the pin. The builder image is now pinned too.
- `Report` read two tables without a snapshot and panicked when polled during a save. The regression test fails under `READ COMMITTED` and passes with the fix.
- A FIFO created after a job was accepted blocked `open` forever and stopped SIGTERM from finishing. Files are now opened non-blocking and must be regular.

Where a fix needed proof, the log records how it was verified, for example by reverting the fix to show the new test fails.
