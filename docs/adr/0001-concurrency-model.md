# 0001 — Concurrency model

- Status: accepted
- Date: 2026-10-30
- Amended: 2026-10-30 (T4: cancellation)

## Context

The processor reads POS sales transaction CSVs from multiple outlets and must process several files concurrently. Files can be large, so rows are streamed (T2) instead of loaded into memory. The concurrency model decided here constrains three tasks:

- T3: a fixed-size worker pool with buffered channels and backpressure, processing multiple files at once.
- T4: cancellation through a `context` tied to OS signals; every goroutine must stop cleanly and `goleak.VerifyTestMain` must pass.
- T6 and T7: progress tracking and aggregation must be safe under concurrent access, and aggregation results must be deterministic regardless of scheduling.

Constraints:

- Memory must stay bounded regardless of file size or file count, so every queue between stages has a fixed capacity.
- Total goroutine count must be bounded and independent of input size.
- `encoding/csv.Reader` is not safe for concurrent use, so a single file can only be read by one goroutine at a time. CSV tokenization (splitting a line into fields) therefore always happens in that one goroutine.
- Row order inside a file is not guaranteed after fan-out; aggregation must not depend on it.
- Monetary amounts are stored as `int64` in the smallest currency unit. Floating-point addition is not associative, so `float64` sums would make results depend on scheduling order.
- Tests run with `-race`, so shared state needs explicit ownership or synchronization.
- Every option must define what happens to partial results when processing is cancelled.

The question is the unit of parallel work and how stages are connected.

## Options

### A. File-level worker pool

N workers pull file paths from a channel. Each worker reads, parses, validates, and aggregates one whole file sequentially into a per-file partial result, then sends the partial result to a single merger goroutine.

- Pros:
  - Simplest model: one channel of jobs, one channel of results, one `sync.WaitGroup` or `errgroup`.
  - Each file has a single owner, so per-file error summaries (T5) and per-file progress (T6) need no locking within the file.
  - At most N files open at once; memory is roughly N × one row buffer plus partial aggregates.
  - Line numbers stay correct without extra bookkeeping.
  - Cancellation is checked once per row in one loop per worker.
- Cons:
  - Parallelism is capped by the number of files. One huge file plus several small ones leaves workers idle while one does all the work.
  - All per-row work for a single file is limited to one core.
  - No backpressure between reading and processing, since both happen in the same goroutine.

### B. Row-batch pipeline (readers → worker pool → aggregator)

Up to R reader goroutines, one per file and bounded by a semaphore, tokenize rows with `csv.Reader` and send them in batches to a shared buffered channel. A fixed pool of N workers converts and validates each batch and sends valid transactions in batches to a buffered channel consumed by a single aggregator goroutine that owns the result maps.

- Pros:
  - Type conversion and validation are spread across N workers, so one large file is not limited to one core for that part of the work.
  - Backpressure is explicit at every stage: when the aggregator falls behind, workers block, then readers block, and memory stays bounded by channel capacity × batch size.
  - The single aggregator owns its maps, so no mutex is needed and results are deterministic given `int64` sums and counts.
  - Each stage can be benchmarked in isolation in T8.
- Cons:
  - CSV tokenization of a single file stays in one reader goroutine, so the reader is the throughput ceiling for one huge file.
  - More goroutines and channels to shut down in the right order; closing channels after cancellation is the most common source of leaks and `send on closed channel` panics, which makes T4 harder.
  - Each row has to carry its file ID and line number through the pipeline so errors and progress can be attributed (T5, T6).
  - Per-message channel overhead must be amortized by batching rows; batch size needs tuning.
  - The single aggregator can become the bottleneck.

### C. File-level pool with per-worker partial aggregation

N workers process whole files as in A, but each worker keeps its own partial aggregate for all files it handles. After all workers finish, the main goroutine merges the N partials once. Progress is reported through atomic counters.

- Pros:
  - No coordination on the hot path: no channel per row, no shared map, no mutex; only atomics for progress.
  - Hypothesis to verify in T8: highest throughput per core when per-row work is light, since there is no channel overhead.
  - Shutdown is just "cancel context, wait for N workers", which keeps T4 small.
  - Determinism comes from a single final merge with sorted keys.
- Cons:
  - Same imbalance as A when one file dominates.
  - No intermediate results until all workers finish.
  - Memory per worker grows with the number of distinct outlet × product × day keys, multiplied by N.
  - No backpressure between stages, since there are no stages.

## Decision

I'm going with B, and batching is part of the design rather than an optimization for later.

The brief is about reading many files at once with a worker pool, and B is the only option where the pool actually shares the work across files instead of just splitting files between workers. I'm aware the reader is still one goroutine per file, so a single huge file is capped by how fast `csv.Reader` can tokenize it. I'd rather have that limit be explicit and measured than pretend parsing is fully parallel.

How I want it built:

- At most R files are read at the same time (`--readers`). Each reader owns its `csv.Reader` and copies records into a batch. I'm not using `ReuseRecord`, because a reused slice crossing into another goroutine is exactly the kind of race that only shows up under load.
- Rows travel in batches (`--batch-size`, starting at 512) so the channel cost is paid per batch, not per row. Each row keeps its file ID and line number so errors and progress can be traced back.
- A fixed pool of N workers (`--workers`, defaults to the number of CPUs) converts and validates rows.
- One aggregator goroutine owns all the maps. Amounts are `int64` in the smallest currency unit, so the totals don't depend on the order batches arrive in.
- Only the side that sends on a channel closes it, and only after every sender is done: readers finish, then the rows channel closes; workers finish, then the transactions channel closes.
- On cancel, the stages that produce work stop right away: the feeder stops handing out files, readers stop at their next batch and never open another file, and an open file is closed so even a blocked read returns. Their batch sends listen on `ctx.Done()`. Each reader still always reports the file it took, which is safe because that channel is buffered per reader and the aggregator never stops reading it.
- Closing the file on cancel happens on a different goroutine than the one reading it, so a source's `Close` must be safe to call while a `Read` is in progress. Files, network connections, and HTTP bodies already behave that way; a custom source has to as well. If the close interrupts a file, that file counts as cancelled even if the read ends in a clean EOF.
- The stages after the readers don't stop early. Workers finish the batches already in their channel and the aggregator keeps reading until every channel is closed. I changed this during T4: if workers also bailed out on cancel, whether a file that had been fully read still counted would depend on scheduling. Now the rule is simple, every batch that left a reader is aggregated, and the extra work is bounded by the channel buffers (a few times `--workers` batches).
- A file counts as cancelled only when the run was cancelled while it was still being read. A source's own error, even one that wraps a context deadline from its storage client, stays a file error.

When a run is cancelled I keep the per-file reports but I don't save any aggregates. A half-processed total is more misleading than no total.

## Consequences

- A single very large file is bounded by its reader's tokenization speed; T8 measures reader throughput separately to show where that ceiling is.
- The aggregator is a possible bottleneck; T8 measures it, and sharding by key is the fallback if it turns out to be one.
- T4 owns the shutdown order and has to prove with `goleak` that no goroutine survives either a normal finish or a cancel.
- The time between cancel and `Run` returning grows with the aggregator's cost per batch times the channel buffers; T8 measures it against the 200 ms target from T4.
- T5 and T6 depend on the file ID and line number carried with each row.
- A cancelled job ends with file reports and no aggregates (see ADR 0003 for how results are saved).
