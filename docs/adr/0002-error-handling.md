# 0002 — Error handling

- Status: accepted
- Date: 2026-10-30

## Context

The processor ingests POS sales transaction CSVs from multiple outlets through the row-batch pipeline of ADR 0001 (readers → worker pool → single aggregator). Real outlet exports are messy: missing fields, malformed numbers or dates, wrong column counts, duplicated headers, a UTF-8 byte order mark from spreadsheet exports. T5 requires that a bad row does not fail the whole file and that errors are summarized per file with line numbers.

Two classes of failure exist:

- Row-level: one row fails. The rest of the file is still usable.
- File-level: the file cannot be opened, the header is missing or does not match the expected schema, or an I/O error occurs mid-read. Nothing more can be read from that file, but other files must keep going.

Row-level errors have two sources in the ADR 0001 pipeline:

- Reader: CSV syntax errors from `csv.Reader` (bad quoting, wrong field count).
- Workers: validation and conversion errors (missing field, invalid number, invalid date).

Both must end up in the same per-file report.

Constraints:

- Memory must stay bounded; a file with millions of bad rows must not grow an unbounded error list.
- Line numbers must be the physical line in the source file, not a row index after fan-out, so `csv.Reader.FieldPos` or an equivalent counter is needed in the reader and carried with each row.
- Errors are produced by R readers and N workers concurrently. Following ADR 0001, shared state has a single owning goroutine instead of a mutex.
- Cancellation (T4) is not an error in a file; it must be distinguishable from real failures in the summary and exit code.
- A file that fails mid-read may already have contributed batches to the aggregates, because batches are processed out of order.
- Aggregation (T7) must only include valid rows, and the summary must show how many rows were dropped.
- Errors are typed so tests can assert on them with `errors.Is` / `errors.As` instead of string matching.

The decision has two parts: how row errors are recorded, and when a file counts as failed.

## Options: recording row errors

### A. Bounded in-memory report

Invalid rows are skipped. Each file has a `FileReport` with counts (`total`, `valid`, `invalid`), counts per error kind, and the first K `RowError{Line, Column, Err}` values.

- Pros:
  - Memory bound is explicit (K errors per file) regardless of input size.
  - Counts per error kind still give the full picture when individual errors are truncated.
  - No extra I/O and no extra goroutine to shut down.
  - Easy to assert in tests.
- Cons:
  - Rows beyond the first K errors are not individually listed; recovering them means rerunning with a larger K.
  - Nothing is persisted unless the summary output is redirected.

### B. Reject file per input file

Invalid rows are written to `<name>.rejected.csv` with extra columns `line` and `error`, streamed as they occur. Memory holds only counters.

- Pros:
  - Every bad row is recoverable and can be fixed and replayed through the same processor.
  - Memory stays constant.
- Cons:
  - Rows are re-encoded from parsed fields with `csv.Writer`, so the output is not byte-identical to the source line.
  - Rejects come from many workers, so a dedicated writer goroutine is needed; it must be shut down correctly on cancellation, which adds to T4.
  - New failure mode: the reject file itself can fail to write.
  - Extra I/O on the hot path and extra output artifacts to clean up between runs.

## Options: when a file fails

### C1. Fatal only on structural failure

A file fails only when it cannot be opened, its header does not match the expected schema after stripping a UTF-8 byte order mark, or an I/O error occurs mid-read. Any number of invalid rows is allowed.

- Pros:
  - Header validation catches the most damaging case, a file with the wrong schema, before any row is aggregated.
  - No arbitrary threshold to justify.
- Cons:
  - A file with a valid header but systematically broken rows still contributes its valid rows; the operator has to notice the invalid count in the report.

### C2. Error budget

As C1, plus a file is aborted when invalid rows exceed a budget (count or ratio after a minimum sample).

- Pros:
  - Stops a systematically broken file from contributing a fraction of its rows.
- Cons:
  - Thresholds are arbitrary and need flags and justified defaults.
  - Aborting means discarding rows already aggregated out of order, which requires per-file staging in the aggregator and a reliable end-of-file signal.
  - Ratio checks are noisy early in a file.

## Decision

A for recording errors, C1 for deciding when a file has failed.

I want a bad row to cost one line in a report, not a stopped file. Real outlet exports are messy, and someone running this would rather get 99% of the data plus a clear list of what was wrong than nothing at all. I looked at the reject file in B and I like the idea, but it adds a writer goroutine that has to shut down cleanly on cancel, and it can fail on its own. That's a lot of moving parts for something I can add later without changing anything else. C2's error budget sounds safe, but I couldn't come up with a threshold I could actually defend, and the header check in C1 already catches the case I care about most: a file with the wrong columns.

How it works:

- The aggregator owns the per-file reports, the same way it owns the totals in ADR 0001. Readers attach CSV syntax errors to the batch they send, and workers attach validation errors to the batch they pass on. No extra channel, no mutex.
- Each file keeps its first 100 row errors (`--max-errors-per-file`), with the line, column, and reason. The count per error kind is always complete, even after the list is full.
- The reader strips a UTF-8 byte order mark and checks the header before sending anything. If the header doesn't match, the file fails with nothing aggregated.
- If a file breaks halfway through (an I/O error), I keep what was already counted and mark the file as `partial`, with the error and the last line read. Throwing those rows away would need per-file staging in the aggregator, and I think a clearly marked partial file is the more honest result anyway.
- To know a file is finished, its reader sends a final status with the number of batches it produced. The aggregator closes the file once that status and all of its batches have arrived.
- Errors are typed: `RowError` and `FileError` wrap sentinel kinds such as `ErrCSVSyntax`, `ErrFieldCount`, `ErrMissingField`, `ErrInvalidNumber`, `ErrInvalidDate`, and `ErrHeaderMismatch`, so tests use `errors.Is` and `errors.As` instead of comparing strings.
- Each file ends as `ok`, `ok_with_errors`, `partial`, `failed`, or `cancelled`.

How it's reported depends on the entry point (ADR 0003):

- CLI: exit code `0` when every file is `ok` or `ok_with_errors`, `1` when any file is `partial` or `failed`, `130` when the run was cancelled.
- HTTP: the same outcome shows up as the job status and the per-file statuses in the report endpoint.

## Consequences

- T2 strips the byte order mark, validates the header, and records physical line numbers.
- T5 implements the report types, error kinds, statuses, and CLI exit codes; its AC can assert on `errors.Is` and on exact counts.
- T7 aggregates may include rows from a `partial` file, so the README and the report have to make that visible.
- Invalid rows beyond the first 100 are counted but not listed. If someone needs every rejected row, B can be added later without changing who owns the report.
- T4 doesn't have an extra writer goroutine to shut down.
