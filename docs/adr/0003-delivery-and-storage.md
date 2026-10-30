# 0003 — Delivery interface and result storage

- Status: accepted
- Date: 2026-10-30
- Amended: 2026-10-30 (T7: which task writes to PostgreSQL)

## Context

ADR 0001 defines the processing pipeline and ADR 0002 defines how errors are recorded. Neither says how a run is started, how progress is observed, or where aggregates and reports end up. T6 (progress tracking) and T7 (aggregation output) depend on this.

The take-home lists general deliverables that apply to every test case: database schema and migration files, API documentation (OpenAPI preferred), Docker configuration, and a README. A pure CSV processor has no natural need for a database or an HTTP API, so this decision also determines which of those deliverables are meaningful for this project.

Constraints:

- The pipeline from ADR 0001 must stay the core and must not depend on how it is invoked.
- Progress must be observable while a run is in progress, not only at the end.
- Writing results must not slow down the hot path of the pipeline.
- Input files are local paths; anything that accepts a path from outside must confine it to a known directory.
- Scope must stay focused on concurrent processing. Authentication, file upload, and distributed job queues are out of scope.

## Options

### A. CLI only, results to stdout

A single binary takes file paths as arguments, shows progress on stderr, and prints the aggregates and error report as JSON to stdout.

- Pros:
  - Smallest surface; all time goes into the pipeline.
  - Trivial to run, test, and benchmark.
  - Exit codes from ADR 0002 map directly to the process result.
- Cons:
  - Database schema, migrations, and API documentation deliverables have nothing to describe.
  - Progress is only visible to whoever runs the process.
  - Results are not queryable after the process exits.

### B. CLI with file output

Same as A, but results and reports are written to JSON or CSV files in an output directory.

- Pros:
  - Results persist and can be diffed between runs.
  - Still a single binary with no external dependencies.
- Cons:
  - Same missing deliverables as A.
  - Output files need naming, overwrite, and cleanup rules.
  - Concurrent runs writing to the same directory need coordination.

### C. Engine as a library, CLI and HTTP API as adapters, PostgreSQL for storage

The pipeline lives in an internal package with no knowledge of its caller. A CLI adapter runs it directly. An HTTP adapter accepts jobs (`POST /jobs` with paths under a configured data directory), runs them in the background, and exposes status, progress, error report, and results. Job state, file reports, capped row errors, and daily aggregates are stored in PostgreSQL.

- Pros:
  - Every general deliverable has a real purpose: schema and migrations for job and result tables, OpenAPI for the HTTP adapter, Docker Compose for the app and database.
  - Progress is observable by any client through `GET /jobs/{id}`.
  - Results stay queryable after the run.
  - The library and adapter split makes package boundaries explicit and keeps the pipeline testable without HTTP or a database.
- Cons:
  - More code that is not about concurrency: handlers, persistence, migrations.
  - Integration tests need a real PostgreSQL instance, which slows the test suite.
  - Job lifecycle questions appear: concurrent jobs, process restarts mid-job.
  - Risk of scope creep toward a full web service.

### D. Same as C with SQLite

As C, but storage is an embedded SQLite database through a pure-Go driver.

- Pros:
  - Deliverables are covered while the app stays a single binary.
  - No database container needed to run or test.
- Cons:
  - Single writer; concurrent jobs serialize on writes.
  - Less representative of the multi-instance production setup a POS platform would run.
  - SQL dialect and migration tooling differ from what a production deployment would use.

## Decision

I'm going with C.

The main reason is that I don't want the deliverables to be decoration. The brief asks for a schema, migrations, and OpenAPI docs, and with A or B I would either skip them or invent something just to tick the box. With C each of them describes something the app actually does. I also like that it forces a clean split: the pipeline from ADR 0001 sits in its own package and has no idea whether it was started from the terminal or from an HTTP request. That makes the core easy to test on its own, which matters more to me than the API itself.

I picked PostgreSQL over SQLite (D) because a POS platform with many outlets would realistically run more than one instance against a shared database, and I'd rather show migrations and queries in the dialect I'd actually use in production.

To keep this from turning into a web project, I'm keeping the HTTP side deliberately thin:

- Four endpoints: create a job, get its status and progress, get its error report, get its results.
- Jobs take file paths inside a configured data directory. No uploads, no auth.
- Progress is served from memory while a job runs. The database is only written when a job changes status and once at the end, when aggregates and reports are saved in a single transaction. Nothing on the per-row path touches the database.
- Only a small number of jobs run at the same time (configurable), so the total goroutine count stays bounded.
- If the process restarts, jobs that were still running are marked as failed on startup. I'm not trying to resume them.

The CLI stays as well, mainly for local runs and benchmarks. It prints JSON and uses the exit codes from ADR 0002.

## Consequences

- Two new tasks: database schema and migrations, and the HTTP API with its OpenAPI spec.
- T7 aggregates and prints results as JSON from the CLI; T9 and T10 write the same aggregates and reports to PostgreSQL in one transaction at the end of a job.
- T6 progress has to be readable from another goroutine (the HTTP handler) while the job is running.
- Integration tests run against a real PostgreSQL. Unit tests for the pipeline must not need it.
- The exit codes in ADR 0002 apply to the CLI only; over HTTP the same information shows up as the job status.
- The README must list what's left out on purpose: auth, uploads, resuming jobs after a restart, and horizontal scaling of job workers.
