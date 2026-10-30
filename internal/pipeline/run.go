package pipeline

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"sync"
	"sync/atomic"
)

type Source struct {
	ID   FileID
	Size int64
	Open func(ctx context.Context) (io.ReadCloser, error)
}

type Totals struct {
	Transactions int64
	Quantity     int64
	Amount       int64
}

type FileResult struct {
	ID          FileID
	Status      FileStatus
	Valid       int64
	Invalid     int64
	ErrorCounts map[string]int64
	Errors      []*RowError
	LastLine    int
	CloseErr    error
	Done        bool
	Cancelled   bool
	Err         error
}

type Result struct {
	Totals Totals
	Daily  []DailySales
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
	closeErr  error
	cancelled bool
}

func Run(ctx context.Context, cfg Config, sources []Source) (Result, error) {
	return RunWithProgress(ctx, cfg, sources, nil)
}

func RunWithProgress(ctx context.Context, cfg Config, sources []Source, p *Progress) (Result, error) {
	if err := cfg.Validate(); err != nil {
		return Result{}, err
	}
	if err := validateSources(sources); err != nil {
		return Result{}, err
	}

	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	tr := newTracker(p, sources)
	jobs := make(chan Source)
	batches := make(chan Batch, cfg.Workers)
	done := make(chan fileDone, cfg.Readers)
	results := make(chan txBatch, cfg.Workers)

	go feed(runCtx, sources, jobs)

	var readers sync.WaitGroup
	for range cfg.Readers {
		readers.Go(func() { readSources(runCtx, cfg, tr, jobs, batches, done) })
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

	sum := newSums(cfg.MaxDailyKeys)
	states := aggregate(sources, cfg.MaxErrorsPerFile, sum, cancel, tr, results, done)
	var cause error
	if runCtx.Err() != nil {
		cause = context.Cause(runCtx)
	}
	res, err := finalize(sum, states, cause)
	tr.publish(states, res.Totals, true)
	return res, err
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

func readSources(ctx context.Context, cfg Config, tr *tracker, jobs <-chan Source, batches chan<- Batch, done chan<- fileDone) {
	for src := range jobs {
		if ctx.Err() != nil {
			return
		}
		r := readSource(ctx, cfg, src, tr.bytes[src.ID], batches)
		r.cancelled = r.cancelled || (r.err != nil && ctx.Err() != nil)
		done <- r
	}
}

func readSource(ctx context.Context, cfg Config, src Source, read *atomic.Int64, batches chan<- Batch) (res fileDone) {
	res.file = src.ID
	rc, err := src.Open(ctx)
	if err != nil {
		if !errors.As(err, new(*fs.PathError)) {
			err = fmt.Errorf("open: %w", err)
		}
		res.err = &FileError{File: src.ID, Err: err}
		return res
	}
	closedOnCancel := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = rc.Close()
		close(closedOnCancel)
	})
	defer func() {
		if !stop() {
			<-closedOnCancel
			res.cancelled = true
			return
		}
		if closeErr := rc.Close(); closeErr != nil {
			res.closeErr = &FileError{File: src.ID, LastLine: res.stats.LastLine, Err: fmt.Errorf("close: %w", closeErr)}
		}
	}()
	res.stats, res.err = ReadCSV(ctx, src.ID, byteCounter{r: rc, n: read}, cfg, batches)
	return res
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
	kept      lowestLines
	received  int
	expected  int
	finished  bool
	cancelled bool
}

func (f *fileState) done() bool {
	return f.finished && !f.cancelled && f.received == f.expected
}

func aggregate(sources []Source, maxErrors int, sum *sums, stop context.CancelCauseFunc, tr *tracker, results <-chan txBatch, done <-chan fileDone) []*fileState {
	files := make(map[FileID]*fileState, len(sources))
	states := make([]*fileState, 0, len(sources))
	for _, src := range sources {
		f := &fileState{
			result: FileResult{ID: src.ID, ErrorCounts: map[string]int64{}},
			kept:   lowestLines{limit: maxErrors},
		}
		files[src.ID] = f
		states = append(states, f)
	}
	tick, stopTicks := tr.ticks()
	defer stopTicks()
	for results != nil || done != nil {
		select {
		case <-tick:
			tr.publish(states, sum.totals, false)
		case b, ok := <-results:
			if !ok {
				results = nil
				continue
			}
			f := files[b.file]
			f.received++
			f.result.Valid += int64(len(b.txs))
			f.result.Invalid += int64(len(b.errors))
			for _, e := range b.errors {
				f.result.ErrorCounts[e.Kind()]++
				f.kept.add(e)
			}
			for _, tx := range b.txs {
				sum.add(tx)
				if sum.overflowed {
					stop(fmt.Errorf("%w: more than %d", ErrTooManyKeys, sum.maxKeys))
				}
			}
		case d, ok := <-done:
			if !ok {
				done = nil
				continue
			}
			f := files[d.file]
			f.expected = d.stats.Batches
			f.result.LastLine = d.stats.LastLine
			if d.cancelled {
				f.cancelled = true
				continue
			}
			f.finished = true
			f.result.Err = d.err
			f.result.CloseErr = d.closeErr
		}
	}
	return states
}

func finalize(sum *sums, states []*fileState, ctxErr error) (Result, error) {
	res := Result{Totals: sum.totals, Daily: []DailySales{}, Files: make([]FileResult, 0, len(states))}
	cancelled := false
	for _, f := range states {
		f.result.Done = f.done()
		f.result.Cancelled = !f.result.Done
		f.result.Errors = f.kept.sorted()
		f.result.Status = fileStatus(f.result)
		cancelled = cancelled || f.result.Cancelled
		res.Files = append(res.Files, f.result)
	}
	slices.SortFunc(res.Files, func(a, b FileResult) int { return cmp.Compare(a.ID, b.ID) })
	if sum.overflowed {
		res.Totals = Totals{}
		return res, fmt.Errorf("%w: more than %d", ErrTooManyKeys, sum.maxKeys)
	}
	if !cancelled {
		res.Daily = sum.sortedDaily()
		return res, nil
	}
	res.Totals = Totals{}
	if ctxErr == nil {
		ctxErr = context.Canceled
	}
	return res, ctxErr
}
