# Tasks

Status: `draft` → `ac-draft` → `ac-approved` → `done`

Owner: `AI` (AI writes, human reviews) or `human` (human writes, AI only reviews).

## T1 — Project skeleton

- Owner: AI
- Status: draft
- ADR: -
- Scope: `go.mod`, package layout, CLI entrypoint, Dockerfile, compose.yaml, `go.uber.org/goleak` dependency. `make check` passes on the empty skeleton.

### AC

## T2 — Streaming CSV reader

- Owner: AI
- Status: draft
- ADR: -
- Scope: read a single file row by row, parse into a transaction struct, validate each row.

### AC

## T3 — Worker pool and backpressure

- Owner: human
- Status: draft
- ADR: 0001
- Scope: fixed-size worker pool, buffered channels, multiple files processed concurrently.

### AC

## T4 — Cancellation and graceful shutdown

- Owner: human
- Status: draft
- ADR: 0001
- Scope: `context` driven by OS signals, all workers stop cleanly, no goroutine leaks.

### AC

## T5 — Per-row and per-file error handling

- Owner: human
- Status: draft
- ADR: 0002
- Scope: a bad row does not fail the whole file, error summary per file and line number.

### AC

## T6 — Progress tracking

- Owner: AI
- Status: draft
- ADR: -
- Scope: per-file and overall progress, safe for concurrent access.

### AC

## T7 — Aggregation and output

- Owner: AI
- Status: draft
- ADR: 0003
- Scope: sales aggregated per outlet, product, and day; deterministic results; persisted as decided in the ADR.

### AC

## T8 — Benchmarks, data generator, README

- Owner: AI
- Status: draft
- ADR: -
- Scope: large dummy CSV generator, memory and throughput benchmarks, README covering the required deliverables, including the coverage report from `coverage.txt`.

### AC
