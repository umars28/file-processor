# 0005 — Progress tracking

- Status: accepted
- Date: 2026-10-30
- Amended: 2026-10-30 (T6: snapshot totals and byte accounting)

## Context

The brief requires progress tracking. ADR 0003 makes progress readable by the CLI while it runs and by the HTTP API through `GET /jobs/{id}`, served from memory, not from the database.

Two problems shape the design:

- What progress means. Files are streamed, so the number of rows in a file is unknown until it has been read completely. A row-based percentage is not available up front.
- Who writes and who reads. Readers, workers, and the aggregator all touch something progress-related, and an HTTP handler on a different goroutine reads it at any time. Tests run with `-race`.

Constraints:

- Updating progress must not add a lock or a channel send per row.
- A reader of progress must see a consistent view: per-file numbers and totals that belong to the same moment.
- Memory for progress is bounded by the number of files in a job.

## Options

### A. Atomic counters per file, read directly

Each file has a struct of `atomic.Int64` counters (bytes read, rows processed, rows invalid). Readers and the aggregator increment them; the HTTP handler and CLI load them.

- Pros:
  - Cheap updates, no locks.
  - Always current.
- Cons:
  - Each counter is atomic on its own, so a reader can see rows processed from one moment and invalid rows from another; totals can briefly disagree with per-file numbers.
  - Every field read by the API has to be atomic, which spreads synchronization across the code.

### B. Mutex-protected progress struct

One struct with a `sync.RWMutex`, updated by the aggregator per batch and read by the API.

- Pros:
  - Consistent view under the lock.
  - Simple to understand.
- Cons:
  - Writers and API readers contend on the same lock.
  - Easy to hold the lock too long or forget it on a new field.

### C. Aggregator publishes immutable snapshots

The aggregator, which already owns all per-file state (ADR 0001, ADR 0002), builds a new progress snapshot at a fixed interval and stores it with `atomic.Pointer[Snapshot]`. Readers load the pointer and get a complete, immutable snapshot. Bytes read come from a counting `io.Reader` in each reader, exposed as one `atomic.Int64` per file that the aggregator reads when building the snapshot.

- Pros:
  - Readers never block writers and always see a consistent snapshot.
  - Fits the existing ownership model: the aggregator owns the state, everyone else reads copies.
  - Byte-based percentage is available from the start using the file size from `os.Stat`.
- Cons:
  - Progress is up to one interval stale.
  - Allocates one snapshot per interval, proportional to the number of files.

## Decision

C.

The aggregator already owns every per-file number from ADRs 0001 and 0002, so I don't want progress to be the one thing that breaks that rule with shared counters or a mutex. Instead it builds a snapshot every 200 ms and publishes it through an `atomic.Pointer`. Anyone who wants progress, the CLI or an HTTP handler, loads the pointer and gets a complete picture from a single moment, without locking anything.

For the percentage I'm using bytes, not rows. Row totals are unknown until a file has been read completely, but the file size is known up front, so `bytes read / file size` gives a real percentage from the first batch. Each reader wraps its file in a small counting reader that updates one `atomic.Int64`, and the aggregator reads that value when it builds the snapshot. Rows processed and rows invalid are reported next to the percentage as absolute numbers.

A snapshot contains, per file: status, bytes read, size, rows processed, rows invalid. For the whole job: the same totals, files done out of total, and elapsed time. Being up to 200 ms behind is fine for something a person is watching.

## Consequences

- T6 implements the counting reader, the snapshot type, and the publishing loop inside the aggregator; its AC can check that snapshots are consistent under `-race`.
- The CLI renders the latest snapshot on stderr; the HTTP API returns it from `GET /jobs/{id}`.
- When a job ends, the final snapshot is what gets saved with the job (ADR 0004), so the last progress seen and the stored result agree.
- In a snapshot, `Valid` and `Invalid` are the rows aggregated so far, while `Totals` (transactions, quantity, amount) follow the result: running totals while the job runs, and zero in the final snapshot of a cancelled job, because ADR 0001 drops the aggregates of a cancelled run. A cancelled final snapshot therefore shows rows counted but no totals; that is intended.
- In the final snapshot every file that finished, including one that failed early, counts its whole size as read, so a finished job reads 100%. Per-file bytes are capped at the declared size, because a wrong `Size` must not push a file past 100%.
- The interval is a constant for now; it can become a flag if it ever matters.
