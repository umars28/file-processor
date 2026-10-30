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
- Status: done
- ADR: 0001, 0002
- Scope: read a single file row by row with `csv.Reader`, strip a UTF-8 byte order mark, validate the header, record the physical line number of each row, emit rows in batches. Also the pure row conversion function the workers will call in T3, with the sentinel error kinds from ADR 0002.

### AC

- [ ] Header: the first record must be exactly `transaction_id,outlet_id,product_id,quantity,amount,transacted_at` after stripping a leading UTF-8 byte order mark. A missing, reordered, or extra column, or an empty file, returns an error matching `ErrHeaderMismatch` and sends no batch.
- [ ] Rows are sent on the output channel in batches of at most `BatchSize`; every row carries its file ID and the physical line where its record starts, including after a quoted field that spans several lines. Mutating a record after it was sent does not change any other row.
- [ ] Edge case: a row with bad quoting or the wrong number of fields does not stop the file; it is attached to the batch as a row error matching `ErrCSVSyntax` or `ErrFieldCount` with its line number, and the rows after it are still sent.
- [ ] Failure case: an I/O error from the underlying reader mid-file stops the reader and returns an error that wraps the cause and reports the last line read; batches sent before the error are not withdrawn.
- [ ] `ParseTransaction` converts a record into a `Transaction` with `int64` quantity and amount and a `time.Time` from RFC 3339; empty fields, non-integer or overflowing numbers, a quantity below 1, a negative amount, and unparsable timestamps return errors matching `ErrMissingField`, `ErrInvalidNumber`, or `ErrInvalidDate` and name the column.
- [ ] A reader blocked on a full output channel returns an error matching `context.Canceled` within 100 ms of cancellation, and `goleak` reports no leaked goroutine.
- [ ] Reading a generated 1,000,000-row stream that never touches disk, while the consumer discards batches, keeps `HeapAlloc` below 32 MB at every batch.

## T3 — Worker pool and backpressure

- Owner: human
- Status: done
- ADR: 0001
- Scope: readers bounded by `--readers`, fixed pool of `--workers`, buffered channels between stages, single aggregator owning all maps, amounts as `int64`.

### AC

- [ ] `pipeline.Run(ctx, cfg, sources)` processes every source, where a source is a file ID plus an `Open` function returning an `io.ReadCloser`. The result's totals (valid transactions, quantity, amount as `int64`) equal a sum computed independently from the generated input, for `(readers, workers)` of `(1, 1)`, `(4, 8)`, and `(2, runtime.NumCPU())`.
- [ ] Determinism: 20 repeated runs over the same 50 generated sources with `(4, 8)` produce identical results.
- [ ] At most `Readers` sources are open at the same time, measured by an instrumented `Open`/`Close`, and every opened source is closed, including when reading it fails.
- [ ] The goroutine count is independent of input size: while processing 1,000 sources, `runtime.NumGoroutine()` never exceeds the count before `Run` plus `Readers + Workers + 4`.
- [ ] Rows rejected by `ParseTransaction` are counted as invalid for their file and never reach the totals. Edge case: a source whose rows are all invalid contributes zero to the totals, and its invalid count equals its row count.
- [ ] Failure case: a source that fails to open, has a wrong header, or returns an I/O error mid-file is recorded in the result for that file only; every other source is still fully processed, and `Run` returns a nil error.
- [ ] Processing 200 sources of 10,000 rows each keeps `HeapAlloc` below 64 MB, and `Run` returns only after every goroutine it started has exited (`goleak` passes with `-race`).

## T4 — Cancellation and graceful shutdown

- Owner: human
- Status: done
- ADR: 0001
- Scope: `context` driven by OS signals, channels closed only by their senders, all goroutines stop on both normal finish and cancel, no goroutine leaks. A cancelled run keeps file reports and drops aggregates. Carried over from the T3 review: cancelled files must be distinguishable from completed ones (currently `Err == nil` and `Done == false`), `context.DeadlineExceeded` must be treated like `context.Canceled` instead of surfacing as a raw file error, `Run` must not return partial `Totals` on cancel, and a blocking `Source.Open` must either take a `context` or be documented as a limit on how fast cancel takes effect.

### AC

- [ ] Cancelling at a random point (0–5 ms after start) in each of 200 runs over 100 sources: every `Run` returns within 200 ms of cancellation with an error matching `context.Canceled`, every opened source is closed, no `send on closed channel` panic occurs, and `goleak` passes with `-race`.
- [ ] A run that hits a `context.WithTimeout` deadline behaves exactly like a cancelled run: the error matches `context.DeadlineExceeded`, and no file result carries a raw context error.
- [ ] A cancelled run returns zero `Totals`. Files that finished before the cancel keep their counts and errors with `Done == true`; every other file, including ones never opened, has `Cancelled == true`.
- [ ] Edge case: when the context is cancelled only after every file has finished, `Run` returns the full totals and a nil error, and no file is marked cancelled.
- [ ] `Source.Open` receives the run's context. Failure case: an `Open` that blocks until its context is done does not hold up cancellation; `Run` still returns within 200 ms.
- [ ] `fileproc serve` started as a real process stops on `SIGINT` and on `SIGTERM`, logs `stopped`, and exits 0 within 2 seconds.

## T5 — Per-row and per-file error handling

- Owner: human
- Status: done
- ADR: 0002
- Scope: typed `RowError` and `FileError` with sentinel kinds, first 100 row errors per file plus complete counts per kind, file statuses, file completion via batch count, CLI exit codes. Replaces the T1 "not implemented" stubs so exit 1 only means a `partial` or `failed` file; exit codes 0/1/2/130 are listed in `fileproc run -h`; the README section is handed to T8.

### AC

- [ ] Every file ends with exactly one status. In a single run containing one source of each kind: `ok` (no invalid rows, no file error), `ok_with_errors` (at least one invalid row), `partial` (a file error after at least one data row was read: mid-file I/O error or `ErrRecordTooLarge`), `failed` (a file error before any data row: open failure, missing or wrong header, empty file), `cancelled` (as defined in T4).
- [ ] Each file keeps at most `MaxErrorsPerFile` row errors: those with the lowest line numbers, sorted by line, each with line, column (when the error is about one field), and kind. With `(readers, workers) = (4, 8)` the kept list is identical across 20 runs. Edge case: `MaxErrorsPerFile = 0` keeps no errors but still counts them.
- [ ] Counts per error kind are complete regardless of the cap: a file with 10,000 invalid rows of mixed kinds reports exact per-kind counts that sum to its invalid count, while its list holds 100 errors.
- [ ] Error kinds have stable names derived with `errors.Is`: `csv_syntax`, `field_count`, `missing_field`, `invalid_number`, `invalid_date`, and `other` for anything else.
- [ ] A `partial` or `failed` file reports its file error and the last line read; a `partial` file's valid rows still count in the totals.
- [ ] `fileproc run a.csv b.csv` runs the pipeline over real files and prints one summary line per file to stderr (path, status, valid, invalid, first kept error). It exits 0 when every file is `ok` or `ok_with_errors`.
- [ ] Failure case: `fileproc run` exits 1 when any file is `partial` or `failed`, including a path that does not exist, and exits 130 when the run is cancelled. `fileproc run -h` lists the exit codes 0, 1, 2, and 130.

## T6 — Progress tracking

- Owner: AI
- Status: done
- ADR: 0005
- Scope: counting reader for bytes read, snapshot built by the aggregator every 200 ms and published through `atomic.Pointer`, CLI progress on stderr.

### AC

- [ ] Progress is opt-in: `RunWithProgress(ctx, cfg, sources, p)` publishes into a `*Progress`, `Run` keeps its current behaviour, and `p.Snapshot()` is safe to call from any goroutine at any time, returning nil before the first publish.
- [ ] Percentages are byte-based: a source declares its `Size`, each reader counts bytes through a counting reader with one atomic add per `Read`, and the final snapshot reports `BytesRead == Size` for every fully read file and 100% overall. Edge case: a file with `Size == 0` reports no percentage of its own and is left out of the overall byte total.
- [ ] A slowed run (sources released row by row) produces at least 3 intermediate snapshots, published no more often than the configured interval, in which `BytesRead`, `Valid`, `Invalid`, and `FilesDone` never decrease.
- [ ] Every snapshot is internally consistent: the per-file valid and invalid counts sum to the snapshot's totals, and `FilesDone` equals the number of files with a final status. Checked on snapshots read concurrently while the run is in progress, under `-race`.
- [ ] Before `RunWithProgress` returns it publishes a final snapshot (`Final == true`) whose per-file statuses, counts, and totals equal the returned `Result`, including for a cancelled run.
- [ ] Progress adds no goroutine to the pipeline and no lock or channel send per row: the T3 goroutine bound still holds with progress enabled.
- [ ] `fileproc run --progress` prints `progress:` lines to stderr (files done/total, percentage, valid, invalid, elapsed) and always ends with one line for the final snapshot; without the flag it prints no progress lines.

## T7 — Aggregation and CLI output

- Owner: AI
- Status: done
- ADR: 0001, 0003
- Scope: sales aggregated per outlet, product, and day; deterministic results regardless of scheduling; `run` subcommand prints results and reports as JSON.

### AC

- [ ] `Result.Daily` holds one entry per (sale date, outlet, product) with transaction count, quantity, and amount as `int64`, and equals a map computed independently from the generated input, for `(readers, workers)` of `(1, 1)` and `(4, 8)`. The entries sum to `Result.Totals`.
- [ ] `Result.Daily` is sorted by date, then outlet, then product, and is identical across 20 runs with `(4, 8)`.
- [ ] Edge case: the sale date is the calendar date in the offset written in the record. `2026-10-30T23:59:59+07:00` and `2026-10-31T00:00:00+07:00` land on different dates, and `2026-10-31T01:00:00+07:00` lands on 2026-10-31 while the same instant written as `2026-10-30T18:00:00Z` lands on 2026-10-30.
- [ ] Rows from a `partial` file are in `Daily`; a cancelled run returns an empty `Daily` (ADR 0001).
- [ ] Aggregation memory grows with the number of distinct (date, outlet, product) keys, not with rows: 200 sources of 10,000 rows over 25,000 keys keep `HeapAlloc` below 64 MB.
- [ ] `fileproc run` writes one JSON document to stdout with `status` (`completed` or `cancelled`), `totals`, `daily`, and `files` (path, status, valid, invalid, error counts, kept errors with line, column, kind, and message, file error, last line). Amounts are JSON integers. The stderr summary from T5 is unchanged.
- [ ] Failure case: stdout is still a single valid JSON document when a file fails (exit 1) and when the run is cancelled (exit 130, `status` `cancelled`, empty `daily`).

## T8 — Benchmarks, data generator, README

- Owner: AI
- Status: draft
- ADR: -
- Scope: large dummy CSV generator, memory and throughput benchmarks per stage (reader, workers, aggregator), CLI exit codes 0/1/2/130 next to the ADR 0002 statuses, known limitations including unchecked `int64` overflow of summed quantity and amount (handed over from the T7 review), time from cancel to return under a realistic aggregator cost (ADR 0001, 200 ms target from T4), README covering the required deliverables, including the coverage report from `coverage.txt`, known limitations, and an `AI Usage` section from `docs/ai-log.md`.

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
- Scope: background job runner with a bounded number of concurrent jobs, paths confined to the configured data directory, jobs left `running` marked `failed` on startup; `serve` subcommand with `POST /jobs`, `GET /jobs/{id}`, `GET /jobs/{id}/report`, `GET /jobs/{id}/results` on `net/http`; OpenAPI spec in `docs/openapi.yaml`. Reports returned over HTTP must not expose absolute server paths from `*os.PathError`, in `path`, `error`, or `close_error` (handed over from the T5 and T7 reviews). Jobs must cap the number of distinct (date, outlet, product) keys, because a client-supplied file with a unique key per row grows the aggregator map without bound (handed over from the T7 review).

### AC
