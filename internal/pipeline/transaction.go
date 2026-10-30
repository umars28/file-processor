package pipeline

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

var Header = []string{"transaction_id", "outlet_id", "product_id", "quantity", "amount", "transacted_at"}

const (
	colID = iota
	colOutlet
	colProduct
	colQuantity
	colAmount
	colTransactedAt
)

type Transaction struct {
	ID           string
	OutletID     string
	ProductID    string
	Quantity     int64
	Amount       int64
	TransactedAt time.Time
}

func ParseTransaction(record []string) (Transaction, error) {
	if len(record) != len(Header) {
		return Transaction{}, fmt.Errorf("%w: got %d, want %d", ErrFieldCount, len(record), len(Header))
	}
	for _, col := range []int{colID, colOutlet, colProduct, colQuantity, colAmount, colTransactedAt} {
		if strings.TrimSpace(record[col]) == "" {
			return Transaction{}, fieldError(col, record[col], ErrMissingField)
		}
	}
	quantity, err := parseInt(record, colQuantity, 1)
	if err != nil {
		return Transaction{}, err
	}
	amount, err := parseInt(record, colAmount, 0)
	if err != nil {
		return Transaction{}, err
	}
	at, err := time.Parse(time.RFC3339, record[colTransactedAt])
	if err != nil {
		return Transaction{}, fieldError(colTransactedAt, record[colTransactedAt], ErrInvalidDate)
	}
	return Transaction{
		ID:           record[colID],
		OutletID:     record[colOutlet],
		ProductID:    record[colProduct],
		Quantity:     quantity,
		Amount:       amount,
		TransactedAt: at,
	}, nil
}

func parseInt(record []string, col int, minValue int64) (int64, error) {
	v, err := strconv.ParseInt(record[col], 10, 64)
	if err != nil {
		return 0, fieldError(col, record[col], ErrInvalidNumber)
	}
	if v < minValue {
		return 0, fieldError(col, record[col], fmt.Errorf("%w: must be at least %d", ErrInvalidNumber, minValue))
	}
	return v, nil
}

func fieldError(col int, value string, err error) *FieldError {
	return &FieldError{Column: Header[col], Value: truncate(value, maxValueInMessage), Err: err}
}
