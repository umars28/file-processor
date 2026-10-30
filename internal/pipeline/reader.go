package pipeline

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
)

const csvReadAhead = 4096

var utf8BOM = []byte{0xEF, 0xBB, 0xBF}

type FileID int

type Row struct {
	File   FileID
	Line   int
	Record []string
}

type Batch struct {
	File   FileID
	Rows   []Row
	Errors []*RowError
}

type ReadStats struct {
	Batches   int
	Rows      int
	RowErrors int
	LastLine  int
}

func ReadCSV(ctx context.Context, id FileID, src io.Reader, cfg Config, out chan<- Batch) (ReadStats, error) {
	if cfg.BatchSize < 1 {
		return ReadStats{}, fmt.Errorf("%w: batch size must be at least 1, got %d", ErrInvalidConfig, cfg.BatchSize)
	}
	if cfg.MaxRecordBytes < 1 {
		return ReadStats{}, fmt.Errorf("%w: max record bytes must be at least 1, got %d", ErrInvalidConfig, cfg.MaxRecordBytes)
	}
	r := &csvReader{ctx: ctx, id: id, batchSize: cfg.BatchSize, maxRecordBytes: cfg.MaxRecordBytes, out: out}
	err := r.run(src)
	return r.stats, err
}

type csvReader struct {
	ctx            context.Context
	id             FileID
	batchSize      int
	maxRecordBytes int
	out            chan<- Batch
	batch          Batch
	stats          ReadStats
}

func (r *csvReader) run(src io.Reader) error {
	if err := r.ctx.Err(); err != nil {
		return err
	}
	br := bufio.NewReader(src)
	if err := skipBOM(br); err != nil {
		return r.fileError(err)
	}
	limit := &recordLimiter{r: br, max: int64(r.maxRecordBytes) + csvReadAhead}
	cr := csv.NewReader(limit)
	cr.FieldsPerRecord = -1
	if err := r.readHeader(cr); err != nil {
		return err
	}
	limit.start = cr.InputOffset()
	r.batch = r.newBatch()
	for {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		record, err := cr.Read()
		limit.start = cr.InputOffset()
		if errors.Is(err, io.EOF) {
			return r.flush()
		}
		var parseErr *csv.ParseError
		if errors.As(err, &parseErr) {
			r.stats.LastLine = parseErr.Line
			if err := r.addError(parseErr.StartLine, fmt.Errorf("%w: %w", ErrCSVSyntax, parseErr)); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			if flushErr := r.flush(); flushErr != nil {
				return flushErr
			}
			return r.fileError(err)
		}
		line, _ := cr.FieldPos(0)
		r.stats.LastLine = line + countNewlines(record)
		if len(record) != len(Header) {
			err := fmt.Errorf("%w: got %d, want %d", ErrFieldCount, len(record), len(Header))
			if err := r.addError(line, err); err != nil {
				return err
			}
			continue
		}
		r.batch.Rows = append(r.batch.Rows, Row{File: r.id, Line: line, Record: record})
		r.stats.Rows++
		if err := r.sendIfFull(); err != nil {
			return err
		}
	}
}

func (r *csvReader) readHeader(cr *csv.Reader) error {
	header, err := cr.Read()
	switch {
	case errors.Is(err, io.EOF):
		return r.fileError(fmt.Errorf("%w: file is empty", ErrHeaderMismatch))
	case errors.As(err, new(*csv.ParseError)):
		return r.fileError(fmt.Errorf("%w: %w", ErrHeaderMismatch, err))
	case err != nil:
		return r.fileError(err)
	}
	r.stats.LastLine = 1
	if !slices.Equal(header, Header) {
		got := truncate(strings.Join(header, ","), maxValueInMessage)
		return r.fileError(fmt.Errorf("%w: got %q, want %q", ErrHeaderMismatch, got, strings.Join(Header, ",")))
	}
	return nil
}

func (r *csvReader) addError(line int, err error) error {
	r.batch.Errors = append(r.batch.Errors, NewRowError(r.id, line, err))
	r.stats.RowErrors++
	return r.sendIfFull()
}

func (r *csvReader) sendIfFull() error {
	if len(r.batch.Rows)+len(r.batch.Errors) < r.batchSize {
		return nil
	}
	return r.flush()
}

func (r *csvReader) flush() error {
	if len(r.batch.Rows) == 0 && len(r.batch.Errors) == 0 {
		return nil
	}
	select {
	case r.out <- r.batch:
	case <-r.ctx.Done():
		return r.ctx.Err()
	}
	r.stats.Batches++
	r.batch = r.newBatch()
	return nil
}

func (r *csvReader) newBatch() Batch {
	return Batch{File: r.id, Rows: make([]Row, 0, r.batchSize)}
}

func (r *csvReader) fileError(err error) error {
	return &FileError{File: r.id, LastLine: r.stats.LastLine, Err: err}
}

type recordLimiter struct {
	r     io.Reader
	read  int64
	start int64
	max   int64
}

func (l *recordLimiter) Read(p []byte) (int, error) {
	if l.read-l.start > l.max {
		return 0, fmt.Errorf("%w: record exceeds about %d bytes", ErrRecordTooLarge, l.max-csvReadAhead)
	}
	n, err := l.r.Read(p)
	l.read += int64(n)
	return n, err
}

func skipBOM(br *bufio.Reader) error {
	prefix, err := br.Peek(len(utf8BOM))
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if bytes.Equal(prefix, utf8BOM) {
		_, _ = br.Discard(len(utf8BOM))
	}
	return nil
}

func countNewlines(record []string) int {
	n := 0
	for _, field := range record {
		n += strings.Count(field, "\n")
	}
	return n
}
