package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"reflect"
	"strings"
	"testing"
)

func errorLines(f FileResult) []int {
	lines := make([]int, 0, len(f.Errors))
	for _, e := range f.Errors {
		lines = append(lines, e.Line)
	}
	return lines
}

func TestRunFileStatuses(t *testing.T) {
	ok, _ := generateSource(1, 30)
	withErrors := contentSource(2, headerLine+row(1)+"bad,outlet-1,product-1,x,1,2026-10-30T09:00:00Z\n")
	partialIO := midFileIOErrorSource(3, 5, errors.New("disk on fire"))
	partialTooLarge := contentSource(4, headerLine+row(1)+row(2)+strings.Repeat("x", 200<<10)+"\n"+row(4))
	failedOpen := Source{ID: 5, Open: func(context.Context) (io.ReadCloser, error) { return nil, errors.New("no such file") }}
	failedHeader := contentSource(6, "nope\n"+row(1))
	failedEmpty := contentSource(7, "")
	failedAfterHeader := midFileIOErrorSource(8, 0, errors.New("disk on fire"))
	failedTooLargeFirstRow := contentSource(10, headerLine+strings.Repeat("x", 200<<10)+"\n"+row(2))
	failedUnclosedQuote := contentSource(11, headerLine+"\"t-1,outlet-1\n"+strings.Repeat(row(3), 5000))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cancelled := Source{ID: 9, Open: func(ctx context.Context) (io.ReadCloser, error) {
		cancel()
		<-ctx.Done()
		return nil, ctx.Err()
	}}

	sources := []Source{ok, withErrors, partialIO, partialTooLarge, failedOpen, failedHeader, failedEmpty, failedAfterHeader, failedTooLargeFirstRow, failedUnclosedQuote, cancelled}
	res, err := Run(ctx, runConfig(1, 4), sources)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	want := map[FileID]FileStatus{
		1:  StatusOK,
		2:  StatusOKWithErrors,
		3:  StatusPartial,
		4:  StatusPartial,
		5:  StatusFailed,
		6:  StatusFailed,
		7:  StatusFailed,
		8:  StatusFailed,
		9:  StatusCancelled,
		10: StatusFailed,
		11: StatusFailed,
	}
	for id, status := range want {
		if f := fileResult(t, res, id); f.Status != status {
			t.Errorf("file %d status = %q, want %q (%+v)", id, f.Status, status, f)
		}
	}
}

func TestPartialAndFailedFilesReportErrorAndLastLine(t *testing.T) {
	errDisk := errors.New("disk on fire")
	good, want := generateSource(1, 10)
	partial := midFileIOErrorSource(2, 7, errDisk)
	_, partialTotals := generateSource(2, 7)
	want.Transactions += partialTotals.Transactions
	want.Quantity += partialTotals.Quantity
	want.Amount += partialTotals.Amount
	failed := contentSource(3, "nope\n")

	res, err := Run(t.Context(), runConfig(2, 2), []Source{good, partial, failed})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if res.Totals != want {
		t.Fatalf("Totals = %+v, want %+v including the partial file's rows", res.Totals, want)
	}
	if f := fileResult(t, res, 2); f.Status != StatusPartial || !errors.Is(f.Err, errDisk) || f.LastLine != 8 {
		t.Fatalf("partial file = %+v, want partial with errDisk after line 8", f)
	}
	if f := fileResult(t, res, 3); f.Status != StatusFailed || !errors.Is(f.Err, ErrHeaderMismatch) || f.LastLine != 1 {
		t.Fatalf("failed file = %+v, want failed with ErrHeaderMismatch after line 1", f)
	}
}

func invalidRows(n int) string {
	buf := []byte(headerLine)
	for i := range n {
		buf = fmt.Appendf(buf, "bad-%d,outlet-1,product-1,x,1,2026-10-30T09:00:00Z\n", i)
	}
	return string(buf)
}

func TestRunKeepsLowestLineErrors(t *testing.T) {
	cfg := runConfig(4, 8)
	cfg.BatchSize = 3
	cfg.MaxErrorsPerFile = 5
	src := contentSource(1, invalidRows(50))

	var first []int
	for i := range 20 {
		res, err := Run(t.Context(), cfg, []Source{src})
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		f := fileResult(t, res, 1)
		if f.Invalid != 50 {
			t.Fatalf("invalid = %d, want 50", f.Invalid)
		}
		lines := errorLines(f)
		if !reflect.DeepEqual(lines, []int{2, 3, 4, 5, 6}) {
			t.Fatalf("run %d kept lines %v, want [2 3 4 5 6]", i, lines)
		}
		if i == 0 {
			first = lines
			e := f.Errors[0]
			if e.Column != "quantity" || e.Kind() != "invalid_number" {
				t.Fatalf("error = %+v (kind %q), want column quantity and kind invalid_number", e, e.Kind())
			}
		} else if !reflect.DeepEqual(lines, first) {
			t.Fatalf("run %d kept %v, run 1 kept %v", i, lines, first)
		}
	}
}

func TestRunZeroErrorCapStillCounts(t *testing.T) {
	cfg := runConfig(2, 2)
	cfg.MaxErrorsPerFile = 0
	res, err := Run(t.Context(), cfg, []Source{contentSource(1, invalidRows(12))})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	f := fileResult(t, res, 1)
	if len(f.Errors) != 0 || f.Invalid != 12 || f.ErrorCounts["invalid_number"] != 12 {
		t.Fatalf("file = %+v, want no kept errors, 12 invalid, 12 invalid_number", f)
	}
}

func TestRunCountsEveryErrorKind(t *testing.T) {
	buf := []byte(headerLine)
	for i := range 10_000 {
		switch i % 5 {
		case 0:
			buf = fmt.Appendf(buf, ",outlet-1,product-1,1,1,2026-10-30T09:00:00Z\n")
		case 1:
			buf = fmt.Appendf(buf, "t-%d,outlet-1,product-1,x,1,2026-10-30T09:00:00Z\n", i)
		case 2:
			buf = fmt.Appendf(buf, "t-%d,outlet-1,product-1,1,1,yesterday\n", i)
		case 3:
			buf = fmt.Appendf(buf, "t-%d,outlet-1\n", i)
		case 4:
			buf = fmt.Appendf(buf, "t-%d,out\"let,product-1,1,1,2026-10-30T09:00:00Z\n", i)
		}
	}
	res, err := Run(t.Context(), runConfig(4, 8), []Source{contentSource(1, string(buf))})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	f := fileResult(t, res, 1)
	want := map[string]int64{
		"missing_field":  2000,
		"invalid_number": 2000,
		"invalid_date":   2000,
		"field_count":    2000,
		"csv_syntax":     2000,
	}
	if !reflect.DeepEqual(f.ErrorCounts, want) {
		t.Fatalf("ErrorCounts = %v, want %v", f.ErrorCounts, want)
	}
	var sum int64
	for _, n := range f.ErrorCounts {
		sum += n
	}
	if sum != f.Invalid || f.Invalid != 10_000 {
		t.Fatalf("counts sum to %d, invalid = %d, want both 10000", sum, f.Invalid)
	}
	if len(f.Errors) != 100 {
		t.Fatalf("kept %d errors, want 100", len(f.Errors))
	}
}

func TestErrorKindNames(t *testing.T) {
	tests := map[string]error{
		"csv_syntax":     fmt.Errorf("%w: bad quote", ErrCSVSyntax),
		"field_count":    ErrFieldCount,
		"missing_field":  &FieldError{Column: "outlet_id", Err: ErrMissingField},
		"invalid_number": &FieldError{Column: "quantity", Err: fmt.Errorf("%w: must be at least 1", ErrInvalidNumber)},
		"invalid_date":   &FieldError{Column: "transacted_at", Err: ErrInvalidDate},
		"other":          errors.New("something else"),
	}
	for want, err := range tests {
		if got := ErrorKind(err); got != want {
			t.Errorf("ErrorKind(%v) = %q, want %q", err, got, want)
		}
		if got := NewRowError(1, 2, err).Kind(); got != want {
			t.Errorf("RowError.Kind() for %v = %q, want %q", err, got, want)
		}
	}
}

type failingClose struct {
	io.Reader
	err error
}

func (f failingClose) Close() error { return f.err }

func TestRunCloseErrorAfterCleanReadKeepsStatus(t *testing.T) {
	errClose := errors.New("flush failed")
	good, want := generateSource(1, 12)
	closing := Source{ID: 1, Open: func(ctx context.Context) (io.ReadCloser, error) {
		rc, err := good.Open(ctx)
		if err != nil {
			return nil, err
		}
		return failingClose{Reader: rc, err: errClose}, nil
	}}
	res, err := Run(t.Context(), runConfig(1, 2), []Source{closing})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if res.Totals != want {
		t.Fatalf("Totals = %+v, want %+v", res.Totals, want)
	}
	f := fileResult(t, res, 1)
	if f.Status != StatusOK || f.Err != nil || !errors.Is(f.CloseErr, errClose) {
		t.Fatalf("file = %+v, want status ok, no Err, CloseErr kept", f)
	}
	var fe *FileError
	if !errors.As(f.CloseErr, &fe) || fe.LastLine != 13 {
		t.Fatalf("CloseErr = %v, want a FileError after line 13", f.CloseErr)
	}
}

func TestRunOpenErrorsKeepTheirPhase(t *testing.T) {
	custom := Source{ID: 1, Open: func(context.Context) (io.ReadCloser, error) {
		return nil, errors.New("connection refused")
	}}
	pathErr := Source{ID: 2, Open: func(context.Context) (io.ReadCloser, error) {
		return nil, &fs.PathError{Op: "open", Path: "/nope.csv", Err: fs.ErrNotExist}
	}}
	res, err := Run(t.Context(), runConfig(1, 1), []Source{custom, pathErr})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if msg := fileResult(t, res, 1).Err.Error(); !strings.Contains(msg, "open: connection refused") {
		t.Fatalf("custom open error = %q, want the open phase named", msg)
	}
	if msg := fileResult(t, res, 2).Err.Error(); strings.Contains(msg, "open: open") || !strings.Contains(msg, "open /nope.csv") {
		t.Fatalf("path error = %q, want it once, without a repeated prefix", msg)
	}
}
