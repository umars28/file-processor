package pipeline

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"
)

const headerLine = "transaction_id,outlet_id,product_id,quantity,amount,transacted_at\n"

func row(i int) string {
	return fmt.Sprintf("trx-%d,outlet-1,product-1,1,1000,2026-10-30T09:00:00Z\n", i)
}

func withBatchSize(n int) Config {
	cfg := DefaultConfig()
	cfg.BatchSize = n
	return cfg
}

func readAll(t *testing.T, src io.Reader, batchSize int) ([]Batch, ReadStats, error) {
	t.Helper()
	out := make(chan Batch)
	var batches []Batch
	done := make(chan struct{})
	go func() {
		defer close(done)
		for b := range out {
			batches = append(batches, b)
		}
	}()
	stats, err := ReadCSV(t.Context(), 7, src, withBatchSize(batchSize), out)
	close(out)
	<-done
	return batches, stats, err
}

func TestReadCSVHeaderAccepted(t *testing.T) {
	for name, input := range map[string]string{
		"plain":    headerLine + row(1),
		"with BOM": "\xEF\xBB\xBF" + headerLine + row(1),
	} {
		t.Run(name, func(t *testing.T) {
			batches, _, err := readAll(t, strings.NewReader(input), 10)
			if err != nil {
				t.Fatalf("ReadCSV() error = %v", err)
			}
			if len(batches) != 1 || len(batches[0].Rows) != 1 {
				t.Fatalf("batches = %+v, want one batch with one row", batches)
			}
		})
	}
}

func TestReadCSVHeaderRejected(t *testing.T) {
	tests := map[string]string{
		"empty file":     "",
		"missing column": "transaction_id,outlet_id,product_id,quantity,amount\n" + row(1),
		"reordered":      "outlet_id,transaction_id,product_id,quantity,amount,transacted_at\n" + row(1),
		"extra column":   strings.TrimSuffix(headerLine, "\n") + ",note\n" + row(1),
		"data as header": row(1) + row(2),
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			batches, _, err := readAll(t, strings.NewReader(input), 10)
			if !errors.Is(err, ErrHeaderMismatch) {
				t.Fatalf("ReadCSV() error = %v, want ErrHeaderMismatch", err)
			}
			var fe *FileError
			if !errors.As(err, &fe) || fe.File != 7 {
				t.Fatalf("error %v is not a *FileError for file 7", err)
			}
			if len(batches) != 0 {
				t.Fatalf("sent %d batches, want 0", len(batches))
			}
		})
	}
}

func TestReadCSVHeaderOnly(t *testing.T) {
	batches, stats, err := readAll(t, strings.NewReader(headerLine), 10)
	if err != nil {
		t.Fatalf("ReadCSV() error = %v", err)
	}
	if len(batches) != 0 || stats.Batches != 0 || stats.Rows != 0 {
		t.Fatalf("batches = %d, stats = %+v, want nothing", len(batches), stats)
	}
}

func TestReadCSVBatchesAndLineNumbers(t *testing.T) {
	var sb strings.Builder
	sb.WriteString(headerLine)
	for i := 1; i <= 7; i++ {
		sb.WriteString(row(i))
	}
	batches, stats, err := readAll(t, strings.NewReader(sb.String()), 3)
	if err != nil {
		t.Fatalf("ReadCSV() error = %v", err)
	}
	if got := []int{len(batches[0].Rows), len(batches[1].Rows), len(batches[2].Rows)}; len(batches) != 3 || got[0] != 3 || got[1] != 3 || got[2] != 1 {
		t.Fatalf("batch sizes = %v (%d batches), want [3 3 1]", got, len(batches))
	}
	line := 2
	for _, b := range batches {
		if b.File != 7 {
			t.Fatalf("batch file = %d, want 7", b.File)
		}
		for _, r := range b.Rows {
			if r.File != 7 || r.Line != line {
				t.Fatalf("row = {File:%d Line:%d}, want {File:7 Line:%d}", r.File, r.Line, line)
			}
			line++
		}
	}
	want := ReadStats{Batches: 3, Rows: 7, RowErrors: 0, LastLine: 8}
	if stats != want {
		t.Fatalf("stats = %+v, want %+v", stats, want)
	}
}

func TestReadCSVMultilineFieldLineNumbers(t *testing.T) {
	input := headerLine +
		"\"trx\n1\",outlet-1,product-1,1,1000,2026-10-30T09:00:00Z\n" +
		row(2)
	batches, _, err := readAll(t, strings.NewReader(input), 10)
	if err != nil {
		t.Fatalf("ReadCSV() error = %v", err)
	}
	rows := batches[0].Rows
	if len(rows) != 2 || rows[0].Line != 2 || rows[1].Line != 4 {
		t.Fatalf("rows = %+v, want lines 2 and 4", rows)
	}
}

func TestReadCSVRowsDoNotShareMemory(t *testing.T) {
	batches, _, err := readAll(t, strings.NewReader(headerLine+row(1)+row(2)), 10)
	if err != nil {
		t.Fatalf("ReadCSV() error = %v", err)
	}
	rows := batches[0].Rows
	rows[0].Record[0] = "mutated"
	if rows[1].Record[0] != "trx-2" {
		t.Fatalf("second row changed to %q after mutating the first", rows[1].Record[0])
	}
}

func TestReadCSVBadRowsDoNotStopFile(t *testing.T) {
	input := headerLine +
		row(1) +
		"trx-2,outlet-1,product\"x,1,1000,2026-10-30T09:00:00Z\n" +
		"trx-3,outlet-1\n" +
		row(4)
	batches, stats, err := readAll(t, strings.NewReader(input), 10)
	if err != nil {
		t.Fatalf("ReadCSV() error = %v", err)
	}
	if len(batches) != 1 {
		t.Fatalf("got %d batches, want 1", len(batches))
	}
	b := batches[0]
	if len(b.Rows) != 2 || b.Rows[0].Line != 2 || b.Rows[1].Line != 5 {
		t.Fatalf("rows = %+v, want lines 2 and 5", b.Rows)
	}
	if len(b.Errors) != 2 {
		t.Fatalf("errors = %+v, want 2", b.Errors)
	}
	if e := b.Errors[0]; !errors.Is(e, ErrCSVSyntax) || e.Line != 3 || e.File != 7 {
		t.Fatalf("errors[0] = %+v (%v), want ErrCSVSyntax on line 3", e, e)
	}
	var parseErr *csv.ParseError
	if !errors.As(b.Errors[0], &parseErr) || parseErr.Column == 0 {
		t.Fatalf("errors[0] = %v, want the csv.ParseError with its column kept", b.Errors[0])
	}
	if e := b.Errors[1]; !errors.Is(e, ErrFieldCount) || e.Line != 4 {
		t.Fatalf("errors[1] = %+v (%v), want ErrFieldCount on line 4", e, e)
	}
	if stats.Rows != 2 || stats.RowErrors != 2 {
		t.Fatalf("stats = %+v, want 2 rows and 2 row errors", stats)
	}
}

func TestReadCSVErrorsCountTowardBatchSize(t *testing.T) {
	var sb strings.Builder
	sb.WriteString(headerLine)
	for range 5 {
		sb.WriteString("only,two\n")
	}
	batches, _, err := readAll(t, strings.NewReader(sb.String()), 2)
	if err != nil {
		t.Fatalf("ReadCSV() error = %v", err)
	}
	if len(batches) != 3 {
		t.Fatalf("got %d batches, want 3", len(batches))
	}
	for _, b := range batches {
		if len(b.Rows)+len(b.Errors) > 2 {
			t.Fatalf("batch holds %d items, want at most 2", len(b.Rows)+len(b.Errors))
		}
	}
}

type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

func TestReadCSVIOErrorMidFile(t *testing.T) {
	errDisk := errors.New("disk on fire")
	src := io.MultiReader(strings.NewReader(headerLine+row(1)+row(2)), failingReader{errDisk})
	batches, _, err := readAll(t, src, 10)
	if !errors.Is(err, errDisk) {
		t.Fatalf("ReadCSV() error = %v, want it to wrap the I/O error", err)
	}
	var fe *FileError
	if !errors.As(err, &fe) {
		t.Fatalf("error %v is not a *FileError", err)
	}
	if fe.LastLine != 3 {
		t.Fatalf("LastLine = %d, want 3", fe.LastLine)
	}
	if !strings.Contains(err.Error(), "line 3") {
		t.Fatalf("Error() = %q, want it to report line 3", err.Error())
	}
	if len(batches) != 1 || len(batches[0].Rows) != 2 {
		t.Fatalf("batches = %+v, want the 2 rows read before the error", batches)
	}
}

func TestReadCSVIOErrorBeforeHeader(t *testing.T) {
	errDisk := errors.New("disk on fire")
	_, _, err := readAll(t, failingReader{errDisk}, 10)
	if !errors.Is(err, errDisk) || errors.Is(err, ErrHeaderMismatch) {
		t.Fatalf("ReadCSV() error = %v, want the I/O error and not a header mismatch", err)
	}
}

func TestReadCSVRejectsInvalidBatchSize(t *testing.T) {
	for name, cfg := range map[string]Config{
		"batch size":       {BatchSize: 0, MaxRecordBytes: 1024},
		"max record bytes": {BatchSize: 10, MaxRecordBytes: 0},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ReadCSV(t.Context(), 1, strings.NewReader(headerLine), cfg, make(chan Batch))
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("ReadCSV() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestReadCSVStopsOnCancelWhileBlocked(t *testing.T) {
	var sb strings.Builder
	sb.WriteString(headerLine)
	for i := range 100 {
		sb.WriteString(row(i))
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	out := make(chan Batch)
	result := make(chan error, 1)
	go func() {
		_, err := ReadCSV(ctx, 1, strings.NewReader(sb.String()), withBatchSize(1), out)
		result <- err
	}()
	<-out
	time.Sleep(20 * time.Millisecond)

	start := time.Now()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ReadCSV() error = %v, want context.Canceled", err)
		}
		if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
			t.Fatalf("returned %v after cancel, want within 100ms", elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("ReadCSV did not return after cancel")
	}
}

func TestReadCSVStopsOnCancelBeforeStart(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := ReadCSV(ctx, 1, strings.NewReader(headerLine+row(1)), withBatchSize(10), make(chan Batch, 1))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadCSV() error = %v, want context.Canceled", err)
	}
}

type rowGenerator struct {
	rows, next int
	buf        []byte
}

func (g *rowGenerator) Read(p []byte) (int, error) {
	for len(g.buf) == 0 {
		if g.next > g.rows {
			return 0, io.EOF
		}
		if g.next == 0 {
			g.buf = append(g.buf, headerLine...)
		} else {
			g.buf = fmt.Appendf(g.buf, "trx-%d,outlet-%d,product-%d,2,25000,2026-10-30T09:00:00Z\n", g.next, g.next%50, g.next%500)
		}
		g.next++
	}
	n := copy(p, g.buf)
	g.buf = g.buf[n:]
	return n, nil
}

func TestReadCSVMemoryStaysBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("reads one million rows")
	}
	const rows = 1_000_000
	const limit = 32 << 20
	out := make(chan Batch, 4)
	peak := make(chan uint64, 1)
	go func() {
		var max uint64
		var ms runtime.MemStats
		for range out {
			runtime.ReadMemStats(&ms)
			if ms.HeapAlloc > max {
				max = ms.HeapAlloc
			}
		}
		peak <- max
	}()
	stats, err := ReadCSV(t.Context(), 1, &rowGenerator{rows: rows}, DefaultConfig(), out)
	close(out)
	maxHeap := <-peak
	if err != nil {
		t.Fatalf("ReadCSV() error = %v", err)
	}
	if stats.Rows != rows {
		t.Fatalf("rows = %d, want %d", stats.Rows, rows)
	}
	if maxHeap > limit {
		t.Fatalf("peak HeapAlloc = %d MB, want below %d MB", maxHeap>>20, limit>>20)
	}
	t.Logf("peak HeapAlloc %.1f MB over %d batches", float64(maxHeap)/(1<<20), stats.Batches)
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func TestReadCSVHeaderParseError(t *testing.T) {
	input := "transaction_id,outlet\"id,product_id,quantity,amount,transacted_at\n" + row(1)
	batches, _, err := readAll(t, strings.NewReader(input), 10)
	if !errors.Is(err, ErrHeaderMismatch) {
		t.Fatalf("ReadCSV() error = %v, want ErrHeaderMismatch", err)
	}
	if len(batches) != 0 {
		t.Fatalf("sent %d batches, want 0", len(batches))
	}
	var parseErr *csv.ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("error %v does not keep the csv.ParseError", err)
	}
}

func TestReadCSVHeaderMismatchMessageIsTruncated(t *testing.T) {
	input := strings.Repeat("x", 10_000) + "\n" + row(1)
	_, _, err := readAll(t, strings.NewReader(input), 10)
	if !errors.Is(err, ErrHeaderMismatch) {
		t.Fatalf("ReadCSV() error = %v, want ErrHeaderMismatch", err)
	}
	if len(err.Error()) > 400 {
		t.Fatalf("error message is %d bytes, want it truncated", len(err.Error()))
	}
}

func TestReadCSVRejectsOversizedRecord(t *testing.T) {
	cfg := withBatchSize(10)
	cfg.MaxRecordBytes = 1 << 10
	tests := map[string]string{
		"long unquoted line": headerLine + row(1) + strings.Repeat("x", 10<<20) + "\n" + row(3),
		"unterminated quote": headerLine + row(1) + "\"trx-2,outlet-1,product-1,1,1000,2026-10-30T09:00:00Z\n" + strings.Repeat(row(3), 200_000),
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			src := &countingReader{r: strings.NewReader(input)}
			out := make(chan Batch, 10)
			_, err := ReadCSV(t.Context(), 7, src, cfg, out)
			close(out)
			if !errors.Is(err, ErrRecordTooLarge) {
				t.Fatalf("ReadCSV() error = %v, want ErrRecordTooLarge", err)
			}
			var fe *FileError
			if !errors.As(err, &fe) || fe.LastLine != 2 {
				t.Fatalf("error = %v, want *FileError after line 2", err)
			}
			if limit := int64(len(headerLine)+len(row(1))) + int64(cfg.MaxRecordBytes) + 3*csvReadAhead; src.n > limit {
				t.Fatalf("consumed %d bytes from source, want at most %d", src.n, limit)
			}
			var rows int
			for b := range out {
				rows += len(b.Rows)
			}
			if rows != 1 {
				t.Fatalf("sent %d rows, want the 1 row before the oversized record", rows)
			}
		})
	}
}

func TestReadCSVUnterminatedQuoteSwallowsRestOfSmallFile(t *testing.T) {
	input := headerLine + row(1) + "\"trx-2,outlet-1,product-1,1,1000,2026-10-30T09:00:00Z\n" + row(3)
	batches, stats, err := readAll(t, strings.NewReader(input), 10)
	if err != nil {
		t.Fatalf("ReadCSV() error = %v", err)
	}
	if len(batches) != 1 || len(batches[0].Rows) != 1 || len(batches[0].Errors) != 1 {
		t.Fatalf("batches = %+v, want 1 row and 1 error", batches)
	}
	if e := batches[0].Errors[0]; !errors.Is(e, ErrCSVSyntax) || e.Line != 3 {
		t.Fatalf("error = %v, want ErrCSVSyntax starting on line 3", e)
	}
	if stats.Rows != 1 || stats.RowErrors != 1 {
		t.Fatalf("stats = %+v, want the row after the bad quote to be lost", stats)
	}
}

func TestRowErrorColumnFromFieldError(t *testing.T) {
	_, err := ParseTransaction([]string{"trx-1", "outlet-1", "product-1", "x", "1000", "2026-10-30T09:00:00Z"})
	re := NewRowError(3, 12, err)
	if re.Column != "quantity" || re.Line != 12 || re.File != 3 {
		t.Fatalf("RowError = %+v, want column quantity on line 12 of file 3", re)
	}
	if !errors.Is(re, ErrInvalidNumber) {
		t.Fatalf("RowError %v does not match ErrInvalidNumber", re)
	}
	if got := re.Error(); !strings.Contains(got, "line 12") || !strings.Contains(got, "column quantity") {
		t.Fatalf("Error() = %q", got)
	}
	var target *RowError
	if !errors.As(fmt.Errorf("wrapped: %w", re), &target) {
		t.Fatal("errors.As cannot find *RowError")
	}
	plain := NewRowError(1, 5, ErrFieldCount)
	if plain.Column != "" || !strings.Contains(plain.Error(), "line 5") {
		t.Fatalf("RowError without column = %+v, %q", plain, plain.Error())
	}
}

func recordOfSize(size int, multiline bool) string {
	suffix := ",outlet-1,product-1,1,1000,2026-10-30T09:00:00Z\n"
	if multiline {
		return "\"" + strings.Repeat("a", size-len(suffix)-4) + "\nb\"" + suffix
	}
	return strings.Repeat("t", size-len(suffix)) + suffix
}

func TestReadCSVAcceptsRecordAtLimit(t *testing.T) {
	cfg := withBatchSize(10)
	cfg.MaxRecordBytes = 1 << 10
	for name, multiline := range map[string]bool{"single line": false, "quoted multi-line": true} {
		t.Run(name, func(t *testing.T) {
			rec := recordOfSize(cfg.MaxRecordBytes, multiline)
			if len(rec) != cfg.MaxRecordBytes {
				t.Fatalf("test record is %d bytes, want %d", len(rec), cfg.MaxRecordBytes)
			}
			input := headerLine + rec + rec + row(9)
			out := make(chan Batch, 10)
			stats, err := ReadCSV(t.Context(), 7, strings.NewReader(input), cfg, out)
			close(out)
			if err != nil {
				t.Fatalf("ReadCSV() error = %v, want records at the limit accepted", err)
			}
			if stats.Rows != 3 || stats.RowErrors != 0 {
				t.Fatalf("stats = %+v, want 3 rows and no errors", stats)
			}
		})
	}
}
