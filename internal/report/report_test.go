package report

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/umars28/file-processor/internal/pipeline"
)

func TestBuildEmptyCollectionsAreArrays(t *testing.T) {
	res := pipeline.Result{Files: []pipeline.FileResult{{ID: 0, Status: pipeline.StatusFailed, Err: &pipeline.FileError{Err: errors.New("boom")}}}}
	out, err := json.Marshal(Build(res, false, func(pipeline.FileID) string { return "a.csv" }))
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, want := range []string{`"daily":[]`, `"errors":[]`, `"error_counts":{}`, `"error":"boom"`, `"path":"a.csv"`} {
		if !strings.Contains(string(out), want) {
			t.Errorf("JSON missing %s: %s", want, out)
		}
	}
	if strings.Contains(string(out), "close_error") {
		t.Errorf("close_error should be omitted when empty: %s", out)
	}
}

func TestBuildDescribesErrors(t *testing.T) {
	fieldErr := &pipeline.FieldError{Column: "quantity", Value: "x", Err: pipeline.ErrInvalidNumber}
	res := pipeline.Result{Files: []pipeline.FileResult{{
		ID:       3,
		Status:   pipeline.StatusPartial,
		Errors:   []*pipeline.RowError{pipeline.NewRowError(3, 7, fieldErr)},
		Err:      &pipeline.FileError{File: 3, LastLine: 9, Err: errors.New("disk on fire")},
		CloseErr: &pipeline.FileError{File: 3, Err: errors.New("close: flush failed")},
		LastLine: 9,
	}}}
	doc := Build(res, true, func(id pipeline.FileID) string { return "f" })
	f := doc.Files[0]
	if doc.Status != StatusCancelled || f.Error != "disk on fire" || f.CloseError != "close: flush failed" || f.LastLine != 9 {
		t.Fatalf("file = %+v, status %q", f, doc.Status)
	}
	if got := f.Errors[0]; got.Line != 7 || got.Column != "quantity" || got.Kind != "invalid_number" || got.Message != `invalid number (value "x")` {
		t.Fatalf("row error = %+v", got)
	}
}
