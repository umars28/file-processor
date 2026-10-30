package pipeline

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"slices"
	"sync"
)

type Source struct {
	ID   FileID
	Open func(ctx context.Context) (io.ReadCloser, error)
}

type Totals struct {
	Transactions int64
	Quantity     int64
	Amount       int64
}

type FileResult struct {
	ID        FileID
	Valid     int64
	Invalid   int64
	Done      bool
	Cancelled bool
	Err       error
}

type Result struct {
	Totals Totals
	Files  []FileResult
}

type txBatch struct {
	file   FileID
	txs    []Transaction
	errors []*RowError
}

type fileDone struct {
	file      FileID
	stats     ReadStats
	err       error
	cancelled bool
}

func Run(ctx context.Context, cfg Config, sources []Source) (Result, error) {
	if err := cfg.Validate(); err != nil {
		return Result{}, err
	}
	if err := validateSources(sources); err != nil {
		return Result{}, err
	}

	jobs := make(chan Source)
	batches := make(chan Batch, cfg.Workers)
	done := make(chan fileDone, cfg.Readers)
	results := make(chan txBatch, cfg.Workers)

	go feed(ctx, sources, jobs)

	var readers sync.WaitGroup
	for range cfg.Readers {
		readers.Go(func() { readSources(ctx, cfg, jobs, batches, done) })
	}

	var workers sync.WaitGroup
	for range cfg.Workers {
		workers.Go(func() { convert(batches, results) })
	}

	go func() {
		readers.Wait()
		close(batches)
		close(done)
		workers.Wait()
		close(results)
	}()

	totals, states := aggregate(sources, results, done)
	return finalize(totals, states, ctx.Err())
}

func validateSources(sources []Source) error {
	seen := make(map[FileID]bool, len(sources))
	for _, src := range sources {
		if src.Open == nil {
			return fmt.Errorf("%w: source %d has no Open function", ErrInvalidConfig, src.ID)
		}
		if seen[src.ID] {
			return fmt.Errorf("%w: duplicate source ID %d", ErrInvalidConfig, src.ID)
		}
		seen[src.ID] = true
	}
	return nil
}

func feed(ctx context.Context, sources []Source, jobs chan<- Source) {
	defer close(jobs)
	for _, src := range sources {
		select {
		case jobs <- src:
		case <-ctx.Done():
			return
		}
	}
}

func readSources(ctx context.Context, cfg Config, jobs <-chan Source, batches chan<- Batch, done chan<- fileDone) {
	for src := range jobs {
		if ctx.Err() != nil {
			return
		}
		stats, interrupted, err := readSource(ctx, cfg, src, batches)
		cancelled := interrupted || (err != nil && ctx.Err() != nil)
		done <- fileDone{file: src.ID, stats: stats, err: err, cancelled: cancelled}
	}
}

func readSource(ctx context.Context, cfg Config, src Source, batches chan<- Batch) (stats ReadStats, interrupted bool, err error) {
	rc, err := src.Open(ctx)
	if err != nil {
		return ReadStats{}, false, &FileError{File: src.ID, Err: fmt.Errorf("open: %w", err)}
	}
	closedOnCancel := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = rc.Close()
		close(closedOnCancel)
	})
	defer func() {
		if !stop() {
			<-closedOnCancel
			interrupted = true
			return
		}
		if closeErr := rc.Close(); closeErr != nil && err == nil {
			err = &FileError{File: src.ID, Err: fmt.Errorf("close: %w", closeErr)}
		}
	}()
	stats, err = ReadCSV(ctx, src.ID, rc, cfg, batches)
	return stats, false, err
}

func convert(batches <-chan Batch, results chan<- txBatch) {
	for b := range batches {
		out := txBatch{file: b.File, txs: make([]Transaction, 0, len(b.Rows)), errors: b.Errors}
		for _, row := range b.Rows {
			tx, err := ParseTransaction(row.Record)
			if err != nil {
				out.errors = append(out.errors, NewRowError(row.File, row.Line, err))
				continue
			}
			out.txs = append(out.txs, tx)
		}
		results <- out
	}
}

type fileState struct {
	result    FileResult
	received  int
	expected  int
	finished  bool
	cancelled bool
}

func (f *fileState) done() bool {
	return f.finished && !f.cancelled && f.received == f.expected
}

func aggregate(sources []Source, results <-chan txBatch, done <-chan fileDone) (Totals, []*fileState) {
	files := make(map[FileID]*fileState, len(sources))
	states := make([]*fileState, 0, len(sources))
	for _, src := range sources {
		f := &fileState{result: FileResult{ID: src.ID}}
		files[src.ID] = f
		states = append(states, f)
	}
	var totals Totals
	for results != nil || done != nil {
		select {
		case b, ok := <-results:
			if !ok {
				results = nil
				continue
			}
			f := files[b.file]
			f.received++
			f.result.Valid += int64(len(b.txs))
			f.result.Invalid += int64(len(b.errors))
			for _, tx := range b.txs {
				totals.Transactions++
				totals.Quantity += tx.Quantity
				totals.Amount += tx.Amount
			}
		case d, ok := <-done:
			if !ok {
				done = nil
				continue
			}
			f := files[d.file]
			f.expected = d.stats.Batches
			if d.cancelled {
				f.cancelled = true
				continue
			}
			f.finished = true
			f.result.Err = d.err
		}
	}
	return totals, states
}

func finalize(totals Totals, states []*fileState, ctxErr error) (Result, error) {
	res := Result{Totals: totals, Files: make([]FileResult, 0, len(states))}
	cancelled := false
	for _, f := range states {
		f.result.Done = f.done()
		f.result.Cancelled = !f.result.Done
		cancelled = cancelled || f.result.Cancelled
		res.Files = append(res.Files, f.result)
	}
	slices.SortFunc(res.Files, func(a, b FileResult) int { return cmp.Compare(a.ID, b.ID) })
	if !cancelled {
		return res, nil
	}
	res.Totals = Totals{}
	if ctxErr == nil {
		ctxErr = context.Canceled
	}
	return res, ctxErr
}
