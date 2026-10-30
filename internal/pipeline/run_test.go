package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func generateSource(id FileID, rows int) (Source, Totals) {
	buf := []byte(headerLine)
	var t Totals
	for i := range rows {
		q := int64(i%5 + 1)
		a := int64(int(id+1)*1000 + i)
		buf = fmt.Appendf(buf, "trx-%d-%d,outlet-%d,product-%d,%d,%d,2026-10-30T09:00:00Z\n", id, i, id%7, i%11, q, a)
		t.Transactions++
		t.Quantity += q
		t.Amount += a
	}
	return contentSource(id, string(buf)), t
}

func contentSource(id FileID, content string) Source {
	return Source{ID: id, Open: func(ctx context.Context) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(content)), nil
	}}
}

func generateSources(n, rows int) ([]Source, Totals) {
	var sources []Source
	var total Totals
	for i := range n {
		s, t := generateSource(FileID(i), rows)
		sources = append(sources, s)
		total.Transactions += t.Transactions
		total.Quantity += t.Quantity
		total.Amount += t.Amount
	}
	return sources, total
}

func runConfig(readers, workers int) Config {
	cfg := DefaultConfig()
	cfg.Readers = readers
	cfg.Workers = workers
	cfg.BatchSize = 64
	return cfg
}

func fileResult(t *testing.T, res Result, id FileID) FileResult {
	t.Helper()
	for _, f := range res.Files {
		if f.ID == id {
			return f
		}
	}
	t.Fatalf("no result for file %d in %+v", id, res.Files)
	return FileResult{}
}

func TestRunTotals(t *testing.T) {
	sources, want := generateSources(30, 500)
	for _, rw := range [][2]int{{1, 1}, {4, 8}, {2, runtime.NumCPU()}} {
		t.Run(fmt.Sprintf("readers=%d workers=%d", rw[0], rw[1]), func(t *testing.T) {
			res, err := Run(t.Context(), runConfig(rw[0], rw[1]), sources)
			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			if res.Totals != want {
				t.Fatalf("Totals = %+v, want %+v", res.Totals, want)
			}
			if len(res.Files) != len(sources) {
				t.Fatalf("got %d file results, want %d", len(res.Files), len(sources))
			}
			for _, f := range res.Files {
				if f.Valid != 500 || f.Invalid != 0 || f.Err != nil || !f.Done {
					t.Fatalf("file result = %+v, want 500 valid rows, no invalid, no error, done", f)
				}
			}
		})
	}
}

func TestRunIsDeterministic(t *testing.T) {
	sources, _ := generateSources(50, 200)
	first, err := Run(t.Context(), runConfig(4, 8), sources)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	for i := range 19 {
		got, err := Run(t.Context(), runConfig(4, 8), sources)
		if err != nil {
			t.Fatalf("run %d: Run() error = %v", i+2, err)
		}
		if !reflect.DeepEqual(got, first) {
			t.Fatalf("run %d differs from run 1", i+2)
		}
	}
}

type openTracker struct {
	mu      sync.Mutex
	open    int
	maxOpen int
	opened  int
	closed  int
}

func (o *openTracker) wrap(s Source) Source {
	return Source{ID: s.ID, Open: func(ctx context.Context) (io.ReadCloser, error) {
		rc, err := s.Open(ctx)
		if err != nil {
			return nil, err
		}
		o.mu.Lock()
		o.open++
		o.opened++
		o.maxOpen = max(o.maxOpen, o.open)
		o.mu.Unlock()
		return &trackedCloser{ReadCloser: rc, tracker: o}, nil
	}}
}

type trackedCloser struct {
	io.ReadCloser
	tracker *openTracker
	once    sync.Once
}

func (c *trackedCloser) Close() error {
	c.once.Do(func() {
		c.tracker.mu.Lock()
		c.tracker.open--
		c.tracker.closed++
		c.tracker.mu.Unlock()
	})
	return c.ReadCloser.Close()
}

func midFileIOErrorSource(id FileID, rowsBefore int, cause error) Source {
	good, _ := generateSource(id, rowsBefore)
	return Source{ID: id, Open: func(ctx context.Context) (io.ReadCloser, error) {
		rc, err := good.Open(ctx)
		if err != nil {
			return nil, err
		}
		return io.NopCloser(io.MultiReader(rc, failingReader{cause})), nil
	}}
}

func TestRunBoundsOpenSources(t *testing.T) {
	sources, _ := generateSources(40, 100)
	sources = append(sources,
		contentSource(40, "wrong,header\n"),
		midFileIOErrorSource(41, 10, errors.New("disk on fire")),
	)
	tracker := &openTracker{}
	var wrapped []Source
	for _, s := range sources {
		wrapped = append(wrapped, tracker.wrap(s))
	}
	cfg := runConfig(3, 4)
	if _, err := Run(t.Context(), cfg, wrapped); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if tracker.maxOpen > cfg.Readers {
		t.Fatalf("%d sources open at once, want at most %d", tracker.maxOpen, cfg.Readers)
	}
	if tracker.opened != len(sources) || tracker.closed != tracker.opened {
		t.Fatalf("opened %d, closed %d, want both %d", tracker.opened, tracker.closed, len(sources))
	}
}

func TestRunGoroutineCountIndependentOfInputSize(t *testing.T) {
	sources, _ := generateSources(1000, 20)
	cfg := runConfig(4, 8)

	var mu sync.Mutex
	highest, samples := 0, 0
	sampled := make([]Source, len(sources))
	for i, s := range sources {
		sampled[i] = Source{ID: s.ID, Open: func(ctx context.Context) (io.ReadCloser, error) {
			n := runtime.NumGoroutine()
			mu.Lock()
			highest = max(highest, n)
			samples++
			mu.Unlock()
			return s.Open(ctx)
		}}
	}
	base := runtime.NumGoroutine()

	if _, err := Run(t.Context(), cfg, sampled); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if samples != len(sources) {
		t.Fatalf("took %d samples, want one per source (%d)", samples, len(sources))
	}
	if limit := base + cfg.Readers + cfg.Workers + 4; highest > limit {
		t.Fatalf("peak goroutines = %d, want at most %d", highest, limit)
	}
}

func TestRunInvalidRowsNeverReachTotals(t *testing.T) {
	good, want := generateSource(0, 100)
	buf := []byte(headerLine)
	for i := range 25 {
		buf = fmt.Appendf(buf, "bad-%d,outlet-1,product-1,lots,1000,2026-10-30T09:00:00Z\n", i)
	}
	allBad := contentSource(1, string(buf))
	mixed := contentSource(2, headerLine+
		"m-1,outlet-1,product-1,2,500,2026-10-30T09:00:00Z\n"+
		"m-2,outlet-1,product-1,0,500,2026-10-30T09:00:00Z\n"+
		"m-3,outlet-1\n"+
		"m-4,outlet-1,product-1,1,-5,2026-10-30T09:00:00Z\n")
	want.Transactions++
	want.Quantity += 2
	want.Amount += 500

	res, err := Run(t.Context(), runConfig(2, 3), []Source{good, allBad, mixed})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if res.Totals != want {
		t.Fatalf("Totals = %+v, want %+v", res.Totals, want)
	}
	if f := fileResult(t, res, 1); f.Valid != 0 || f.Invalid != 25 || f.Err != nil {
		t.Fatalf("all-invalid file = %+v, want 0 rows and 25 invalid", f)
	}
	if f := fileResult(t, res, 2); f.Valid != 1 || f.Invalid != 3 {
		t.Fatalf("mixed file = %+v, want 1 row and 3 invalid", f)
	}
}

func TestRunFileFailuresStayIsolated(t *testing.T) {
	errOpen := errors.New("permission denied")
	errDisk := errors.New("disk on fire")
	good, want := generateSources(10, 300)
	sources := append(good,
		Source{ID: 100, Open: func(ctx context.Context) (io.ReadCloser, error) { return nil, errOpen }},
		contentSource(101, "outlet_id,transaction_id\n"+row(1)),
		midFileIOErrorSource(102, 40, errDisk),
	)

	res, err := Run(t.Context(), runConfig(3, 4), sources)
	if err != nil {
		t.Fatalf("Run() error = %v, want nil when only files fail", err)
	}
	if f := fileResult(t, res, 100); !errors.Is(f.Err, errOpen) {
		t.Fatalf("open failure result = %+v, want errOpen", f)
	}
	if f := fileResult(t, res, 101); !errors.Is(f.Err, ErrHeaderMismatch) || f.Valid != 0 {
		t.Fatalf("header failure result = %+v, want ErrHeaderMismatch and no rows", f)
	}
	f := fileResult(t, res, 102)
	if !errors.Is(f.Err, errDisk) {
		t.Fatalf("mid-file failure result = %+v, want errDisk", f)
	}
	_, partial := generateSource(102, 40)
	want.Transactions += partial.Transactions
	want.Quantity += partial.Quantity
	want.Amount += partial.Amount
	if f.Valid != 40 || res.Totals != want {
		t.Fatalf("totals = %+v with %d rows from file 102, want %+v with 40", res.Totals, f.Valid, want)
	}
	for _, id := range []FileID{0, 5, 9} {
		if f := fileResult(t, res, id); f.Valid != 300 || f.Err != nil {
			t.Fatalf("healthy file %d = %+v, want 300 rows", id, f)
		}
	}
}

func TestRunRejectsInvalidConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Workers = 0
	if _, err := Run(t.Context(), cfg, nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Run() error = %v, want ErrInvalidConfig", err)
	}
}

func TestRunRejectsInvalidSources(t *testing.T) {
	ok, _ := generateSource(1, 5)
	dup, _ := generateSource(1, 5)
	tests := map[string][]Source{
		"nil open":     {ok, {ID: 2}},
		"duplicate id": {ok, dup},
	}
	for name, sources := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := Run(t.Context(), DefaultConfig(), sources); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("Run() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestRunEmptySources(t *testing.T) {
	res, err := Run(t.Context(), DefaultConfig(), nil)
	if err != nil || res.Totals != (Totals{}) || len(res.Files) != 0 {
		t.Fatalf("Run(nil) = %+v, %v, want empty result and no error", res, err)
	}
}

func TestAggregateCountsBatchesPerFile(t *testing.T) {
	sources := []Source{{ID: 1}, {ID: 2}}
	results := make(chan txBatch, 4)
	done := make(chan fileDone, 4)
	done <- fileDone{file: 1, stats: ReadStats{Batches: 2}}
	done <- fileDone{file: 2, stats: ReadStats{Batches: 2}}
	results <- txBatch{file: 1, txs: []Transaction{{Quantity: 1, Amount: 10}}}
	results <- txBatch{file: 1, txs: []Transaction{{Quantity: 2, Amount: 20}}}
	results <- txBatch{file: 2, txs: []Transaction{{Quantity: 3, Amount: 30}}}
	close(results)
	close(done)

	totals, states := aggregate(sources, 100, newTracker(nil, sources), results, done)
	res, _ := finalize(totals, states, nil)
	if f := fileResult(t, res, 1); !f.Done || f.Valid != 2 {
		t.Fatalf("file 1 = %+v, want done with 2 valid rows after its done message arrived first", f)
	}
	if f := fileResult(t, res, 2); f.Done {
		t.Fatalf("file 2 = %+v, want not done: 1 of 2 batches arrived", f)
	}
}

func TestRunMemoryStaysBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("processes two million rows")
	}
	const files, rows = 200, 10_000
	const limit = 64 << 20
	var sources []Source
	for i := range files {
		sources = append(sources, Source{ID: FileID(i), Open: func(ctx context.Context) (io.ReadCloser, error) {
			return io.NopCloser(&rowGenerator{rows: rows}), nil
		}})
	}

	stop := make(chan struct{})
	peak := make(chan uint64, 1)
	go func() {
		var highest uint64
		var ms runtime.MemStats
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				peak <- highest
				return
			case <-ticker.C:
				runtime.ReadMemStats(&ms)
				highest = max(highest, ms.HeapAlloc)
			}
		}
	}()

	res, err := Run(t.Context(), DefaultConfig(), sources)
	close(stop)
	highest := <-peak
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if want := int64(files * rows); res.Totals.Transactions != want {
		t.Fatalf("transactions = %d, want %d", res.Totals.Transactions, want)
	}
	if highest > limit {
		t.Fatalf("peak HeapAlloc = %d MB, want below %d MB", highest>>20, limit>>20)
	}
	t.Logf("peak HeapAlloc %.1f MB", float64(highest)/(1<<20))
}
