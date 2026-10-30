package gendata

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"strconv"
	"time"
)

const header = "transaction_id,outlet_id,product_id,quantity,amount,transacted_at\n"

var invalidKinds = []string{"missing_field", "invalid_number", "invalid_date", "field_count", "csv_syntax"}

type Options struct {
	Rows        int
	Outlets     int
	Products    int
	Days        int
	Start       time.Time
	InvalidRate float64
	Seed        uint64
}

type Stats struct {
	Rows    int            `json:"rows"`
	Valid   int            `json:"valid"`
	Invalid map[string]int `json:"invalid"`
	Bytes   int64          `json:"bytes"`
}

func Defaults() Options {
	return Options{
		Rows:     1_000_000,
		Outlets:  50,
		Products: 500,
		Days:     30,
		Start:    time.Date(2026, 10, 1, 0, 0, 0, 0, time.FixedZone("WIB", 7*3600)),
		Seed:     1,
	}
}

func (o Options) Validate() error {
	var errs []error
	if o.Rows < 0 {
		errs = append(errs, fmt.Errorf("rows must not be negative, got %d", o.Rows))
	}
	if o.Outlets < 1 || o.Products < 1 || o.Days < 1 {
		errs = append(errs, fmt.Errorf("outlets, products, and days must be at least 1"))
	}
	if o.InvalidRate < 0 || o.InvalidRate > 1 {
		errs = append(errs, fmt.Errorf("invalid rate must be between 0 and 1, got %v", o.InvalidRate))
	}
	return errors.Join(errs...)
}

func Generate(w io.Writer, o Options) (Stats, error) {
	if err := o.Validate(); err != nil {
		return Stats{}, err
	}
	rng := rand.New(rand.NewPCG(o.Seed, o.Seed^0x9e3779b97f4a7c15))
	cw := &countingWriter{w: w}
	bw := bufio.NewWriterSize(cw, 64<<10)
	stats := Stats{Rows: o.Rows, Invalid: map[string]int{}}
	if _, err := bw.WriteString(header); err != nil {
		return stats, err
	}
	buf := make([]byte, 0, 128)
	for i := range o.Rows {
		buf = buf[:0]
		if o.InvalidRate > 0 && rng.Float64() < o.InvalidRate {
			kind := invalidKinds[rng.IntN(len(invalidKinds))]
			buf = appendInvalid(buf, kind, i, o, rng)
			stats.Invalid[kind]++
		} else {
			buf = appendValid(buf, i, o, rng)
			stats.Valid++
		}
		if _, err := bw.Write(buf); err != nil {
			return stats, err
		}
	}
	if err := bw.Flush(); err != nil {
		return stats, err
	}
	stats.Bytes = cw.n
	return stats, nil
}

func appendValid(buf []byte, i int, o Options, rng *rand.Rand) []byte {
	at := o.Start.Add(time.Duration(rng.IntN(o.Days*86400)) * time.Second)
	buf = append(buf, "trx-"...)
	buf = strconv.AppendInt(buf, int64(i), 10)
	buf = append(buf, ",outlet-"...)
	buf = strconv.AppendInt(buf, int64(rng.IntN(o.Outlets)), 10)
	buf = append(buf, ",product-"...)
	buf = strconv.AppendInt(buf, int64(rng.IntN(o.Products)), 10)
	buf = append(buf, ',')
	buf = strconv.AppendInt(buf, int64(rng.IntN(5)+1), 10)
	buf = append(buf, ',')
	buf = strconv.AppendInt(buf, int64((rng.IntN(200)+1)*500), 10)
	buf = append(buf, ',')
	buf = at.AppendFormat(buf, time.RFC3339)
	return append(buf, '\n')
}

func appendInvalid(buf []byte, kind string, i int, o Options, rng *rand.Rand) []byte {
	at := o.Start.Add(time.Duration(rng.IntN(o.Days*86400)) * time.Second).Format(time.RFC3339)
	outlet, product := rng.IntN(o.Outlets), rng.IntN(o.Products)
	switch kind {
	case "missing_field":
		return fmt.Appendf(buf, ",outlet-%d,product-%d,1,1000,%s\n", outlet, product, at)
	case "invalid_number":
		return fmt.Appendf(buf, "trx-%d,outlet-%d,product-%d,two,1000,%s\n", i, outlet, product, at)
	case "invalid_date":
		return fmt.Appendf(buf, "trx-%d,outlet-%d,product-%d,1,1000,yesterday\n", i, outlet, product)
	case "field_count":
		return fmt.Appendf(buf, "trx-%d,outlet-%d\n", i, outlet)
	default:
		return fmt.Appendf(buf, "trx-%d,out\"let-%d,product-%d,1,1000,%s\n", i, outlet, product, at)
	}
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
