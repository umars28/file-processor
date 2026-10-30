package pipeline

import (
	"errors"
	"fmt"
)

const maxValueInMessage = 64

var (
	ErrCSVSyntax      = errors.New("csv syntax error")
	ErrFieldCount     = errors.New("wrong number of fields")
	ErrMissingField   = errors.New("missing field")
	ErrInvalidNumber  = errors.New("invalid number")
	ErrInvalidDate    = errors.New("invalid date")
	ErrHeaderMismatch = errors.New("header mismatch")
	ErrRecordTooLarge = errors.New("record too large")
	ErrTooManyKeys    = errors.New("too many distinct daily keys")
)

type FieldError struct {
	Column string
	Value  string
	Err    error
}

func (e *FieldError) Error() string {
	return fmt.Sprintf("column %s: %v (value %q)", e.Column, e.Err, e.Value)
}

func (e *FieldError) Unwrap() error { return e.Err }

type RowError struct {
	File   FileID
	Line   int
	Column string
	Err    error
}

func NewRowError(file FileID, line int, err error) *RowError {
	re := &RowError{File: file, Line: line, Err: err}
	var fe *FieldError
	if errors.As(err, &fe) {
		re.Column = fe.Column
	}
	return re
}

func (e *RowError) Error() string {
	if e.Column != "" {
		return fmt.Sprintf("file %d line %d column %s: %v", e.File, e.Line, e.Column, e.Err)
	}
	return fmt.Sprintf("file %d line %d: %v", e.File, e.Line, e.Err)
}

func (e *RowError) Unwrap() error { return e.Err }

type FileError struct {
	File     FileID
	LastLine int
	Err      error
}

func (e *FileError) Error() string {
	return fmt.Sprintf("file %d after line %d: %v", e.File, e.LastLine, e.Err)
}

func (e *FileError) Unwrap() error { return e.Err }

func truncate(s string, n int) string {
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[:n]) + "..."
}
