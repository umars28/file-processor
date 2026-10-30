package pipeline

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
)

type Source struct {
	ID   FileID
	Open func() (io.ReadCloser, error)
}

type Totals struct {
	Transactions int64
	Quantity     int64
	Amount       int64
}

type FileResult struct {
	ID      FileID
	Valid   int64
	Invalid int64
	Done    bool
	Err     error
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
	file  FileID
	stats ReadStats
	err   error
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
		workers.Go(func() { convert(ctx, batches, results) })
	}

	go func() {
		readers.Wait()
		close(batches)
		close(done)
		workers.Wait()
		close(results)
	}()

	res := aggregate(sources, results, done)
	return res, ctx.Err()
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
		stats, err := readSource(ctx, cfg, src, batches)
		select {
		case done <- fileDone{file: src.ID, stats: stats, err: err}:
		case <-ctx.Done():
			return
		}
	}
}

func readSource(ctx context.Context, cfg Config, src Source, batches chan<- Batch) (stats ReadStats, err error) {
	rc, err := src.Open()
	if err != nil {
		return ReadStats{}, &FileError{File: src.ID, Err: fmt.Errorf("open: %w", err)}
	}
	defer func() {
		if closeErr := rc.Close(); closeErr != nil && err == nil {
			err = &FileError{File: src.ID, Err: fmt.Errorf("close: %w", closeErr)}
		}
	}()
	return ReadCSV(ctx, src.ID, rc, cfg, batches)
}

func convert(ctx context.Context, batches <-chan Batch, results chan<- txBatch) {
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
		select {
		case results <- out:
		case <-ctx.Done():
			return
		}
	}
}

type fileState struct {
	result   FileResult
	received int
	expected int
	finished bool
}

func aggregate(sources []Source, results <-chan txBatch, done <-chan fileDone) Result {
	files := make(map[FileID]*fileState, len(sources))
	for _, src := range sources {
		files[src.ID] = &fileState{result: FileResult{ID: src.ID}}
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
			f.finished = true
			f.expected = d.stats.Batches
			if d.err != nil && !errors.Is(d.err, context.Canceled) {
				f.result.Err = d.err
			}
		}
	}

	res := Result{Totals: totals, Files: make([]FileResult, 0, len(files))}
	for _, f := range files {
		f.result.Done = f.finished && f.received == f.expected
		res.Files = append(res.Files, f.result)
	}
	slices.SortFunc(res.Files, func(a, b FileResult) int { return cmp.Compare(a.ID, b.ID) })
	return res
}
