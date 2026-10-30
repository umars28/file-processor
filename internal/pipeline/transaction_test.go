package pipeline

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func validRecord() []string {
	return []string{"trx-1", "outlet-1", "product-1", "3", "45000", "2026-10-30T09:15:00+07:00"}
}

func TestParseTransactionValid(t *testing.T) {
	got, err := ParseTransaction(validRecord())
	if err != nil {
		t.Fatalf("ParseTransaction() error = %v", err)
	}
	want := Transaction{
		ID:           "trx-1",
		OutletID:     "outlet-1",
		ProductID:    "product-1",
		Quantity:     3,
		Amount:       45000,
		TransactedAt: time.Date(2026, 10, 30, 9, 15, 0, 0, time.FixedZone("", 7*3600)),
	}
	if got.ID != want.ID || got.OutletID != want.OutletID || got.ProductID != want.ProductID ||
		got.Quantity != want.Quantity || got.Amount != want.Amount || !got.TransactedAt.Equal(want.TransactedAt) {
		t.Fatalf("ParseTransaction() = %+v, want %+v", got, want)
	}
}

func TestParseTransactionAllowsZeroAmount(t *testing.T) {
	rec := validRecord()
	rec[4] = "0"
	if _, err := ParseTransaction(rec); err != nil {
		t.Fatalf("ParseTransaction() error = %v, want nil", err)
	}
}

func TestParseTransactionErrors(t *testing.T) {
	tests := []struct {
		name   string
		column int
		value  string
		kind   error
	}{
		{"empty transaction id", 0, "", ErrMissingField},
		{"blank outlet id", 1, "   ", ErrMissingField},
		{"empty product id", 2, "", ErrMissingField},
		{"empty quantity", 3, "", ErrMissingField},
		{"decimal quantity", 3, "1.5", ErrInvalidNumber},
		{"text quantity", 3, "three", ErrInvalidNumber},
		{"zero quantity", 3, "0", ErrInvalidNumber},
		{"negative quantity", 3, "-2", ErrInvalidNumber},
		{"overflowing amount", 4, "9223372036854775808", ErrInvalidNumber},
		{"negative amount", 4, "-1", ErrInvalidNumber},
		{"decimal amount", 4, "100.50", ErrInvalidNumber},
		{"empty timestamp", 5, "", ErrMissingField},
		{"date without time", 5, "2026-10-30", ErrInvalidDate},
		{"garbage timestamp", 5, "yesterday", ErrInvalidDate},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := validRecord()
			rec[tt.column] = tt.value
			_, err := ParseTransaction(rec)
			if !errors.Is(err, tt.kind) {
				t.Fatalf("ParseTransaction() error = %v, want %v", err, tt.kind)
			}
			var fe *FieldError
			if !errors.As(err, &fe) {
				t.Fatalf("error %v is not a *FieldError", err)
			}
			if fe.Column != Header[tt.column] {
				t.Fatalf("Column = %q, want %q", fe.Column, Header[tt.column])
			}
			if !strings.Contains(err.Error(), Header[tt.column]) {
				t.Fatalf("Error() = %q, want it to name column %q", err.Error(), Header[tt.column])
			}
		})
	}
}

func TestParseTransactionWrongFieldCount(t *testing.T) {
	_, err := ParseTransaction([]string{"a", "b"})
	if !errors.Is(err, ErrFieldCount) {
		t.Fatalf("ParseTransaction() error = %v, want ErrFieldCount", err)
	}
}

func TestParseTransactionTruncatesValueInError(t *testing.T) {
	rec := validRecord()
	rec[3] = strings.Repeat("9", 1000)
	_, err := ParseTransaction(rec)
	var fe *FieldError
	if !errors.As(err, &fe) {
		t.Fatalf("error %v is not a *FieldError", err)
	}
	if len(fe.Value) > 100 || len(err.Error()) > 200 {
		t.Fatalf("value is %d bytes and message %d bytes, want both truncated", len(fe.Value), len(err.Error()))
	}
}
