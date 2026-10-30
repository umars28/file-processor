package report

import (
	"errors"
	"fmt"

	"github.com/umars28/file-processor/internal/pipeline"
)

const (
	StatusCompleted = "completed"
	StatusCancelled = "cancelled"
)

type Document struct {
	Status string  `json:"status"`
	Totals Totals  `json:"totals"`
	Daily  []Daily `json:"daily"`
	Files  []File  `json:"files"`
}

type Totals struct {
	Transactions int64 `json:"transactions"`
	Quantity     int64 `json:"quantity"`
	Amount       int64 `json:"amount"`
}

type Daily struct {
	Date         string `json:"date"`
	OutletID     string `json:"outlet_id"`
	ProductID    string `json:"product_id"`
	Transactions int64  `json:"transactions"`
	Quantity     int64  `json:"quantity"`
	Amount       int64  `json:"amount"`
}

type File struct {
	Path        string           `json:"path"`
	Status      string           `json:"status"`
	Valid       int64            `json:"valid"`
	Invalid     int64            `json:"invalid"`
	ErrorCounts map[string]int64 `json:"error_counts"`
	Errors      []RowError       `json:"errors"`
	Error       string           `json:"error,omitempty"`
	LastLine    int              `json:"last_line"`
	CloseError  string           `json:"close_error,omitempty"`
}

type RowError struct {
	Line    int    `json:"line"`
	Column  string `json:"column,omitempty"`
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

func Build(res pipeline.Result, cancelled bool, name func(pipeline.FileID) string) Document {
	doc := Document{
		Status: StatusCompleted,
		Totals: Totals(res.Totals),
		Daily:  make([]Daily, 0, len(res.Daily)),
		Files:  make([]File, 0, len(res.Files)),
	}
	if cancelled {
		doc.Status = StatusCancelled
	}
	for _, d := range res.Daily {
		doc.Daily = append(doc.Daily, Daily{
			Date:         d.Date,
			OutletID:     d.OutletID,
			ProductID:    d.ProductID,
			Transactions: d.Transactions,
			Quantity:     d.Quantity,
			Amount:       d.Amount,
		})
	}
	for _, f := range res.Files {
		doc.Files = append(doc.Files, buildFile(f, name(f.ID)))
	}
	return doc
}

func buildFile(f pipeline.FileResult, path string) File {
	out := File{
		Path:        path,
		Status:      string(f.Status),
		Valid:       f.Valid,
		Invalid:     f.Invalid,
		ErrorCounts: map[string]int64{},
		Errors:      make([]RowError, 0, len(f.Errors)),
		LastLine:    f.LastLine,
	}
	for k, v := range f.ErrorCounts {
		out.ErrorCounts[k] = v
	}
	for _, e := range f.Errors {
		out.Errors = append(out.Errors, RowError{Line: e.Line, Column: e.Column, Kind: e.Kind(), Message: Describe(e.Err)})
	}
	if f.Err != nil {
		out.Error = Describe(f.Err)
	}
	if f.CloseErr != nil {
		out.CloseError = Describe(f.CloseErr)
	}
	return out
}

func Describe(err error) string {
	var fileErr *pipeline.FileError
	if errors.As(err, &fileErr) {
		err = fileErr.Err
	}
	var fieldErr *pipeline.FieldError
	if errors.As(err, &fieldErr) {
		return fmt.Sprintf("%v (value %q)", fieldErr.Err, fieldErr.Value)
	}
	return err.Error()
}
