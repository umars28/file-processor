package pipeline

import (
	"cmp"
	"container/heap"
	"errors"
	"slices"
)

type FileStatus string

const (
	StatusOK           FileStatus = "ok"
	StatusOKWithErrors FileStatus = "ok_with_errors"
	StatusPartial      FileStatus = "partial"
	StatusFailed       FileStatus = "failed"
	StatusCancelled    FileStatus = "cancelled"
)

var errorKinds = []struct {
	name string
	err  error
}{
	{"csv_syntax", ErrCSVSyntax},
	{"field_count", ErrFieldCount},
	{"missing_field", ErrMissingField},
	{"invalid_number", ErrInvalidNumber},
	{"invalid_date", ErrInvalidDate},
}

func ErrorKind(err error) string {
	for _, k := range errorKinds {
		if errors.Is(err, k.err) {
			return k.name
		}
	}
	return "other"
}

func (e *RowError) Kind() string { return ErrorKind(e.Err) }

func fileStatus(f FileResult) FileStatus {
	switch {
	case f.Cancelled:
		return StatusCancelled
	case f.Err != nil && f.LastLine > 1:
		return StatusPartial
	case f.Err != nil:
		return StatusFailed
	case f.Invalid > 0:
		return StatusOKWithErrors
	default:
		return StatusOK
	}
}

type lowestLines struct {
	limit int
	items []*RowError
}

func (h *lowestLines) Len() int { return len(h.items) }

func (h *lowestLines) Less(i, j int) bool { return compareRowErrors(h.items[i], h.items[j]) > 0 }

func (h *lowestLines) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

func (h *lowestLines) Push(x any) { h.items = append(h.items, x.(*RowError)) }

func (h *lowestLines) Pop() any {
	last := h.items[len(h.items)-1]
	h.items = h.items[:len(h.items)-1]
	return last
}

func (h *lowestLines) add(e *RowError) {
	if h.limit <= 0 {
		return
	}
	if len(h.items) < h.limit {
		heap.Push(h, e)
		return
	}
	if compareRowErrors(e, h.items[0]) < 0 {
		h.items[0] = e
		heap.Fix(h, 0)
	}
}

func (h *lowestLines) sorted() []*RowError {
	out := slices.Clone(h.items)
	slices.SortFunc(out, compareRowErrors)
	return out
}

func compareRowErrors(a, b *RowError) int {
	return cmp.Or(cmp.Compare(a.Line, b.Line), cmp.Compare(a.Column, b.Column))
}
