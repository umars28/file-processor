package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"testing"
	"time"
)

var saleDates = []string{
	"2026-10-28T08:00:00+07:00",
	"2026-10-29T12:30:00+07:00",
	"2026-10-30T21:45:00+07:00",
}

func dailySource(id FileID, rows int) (Source, map[DailyKey]DailySales) {
	buf := []byte(headerLine)
	want := map[DailyKey]DailySales{}
	for i := range rows {
		outlet := fmt.Sprintf("outlet-%d", (int(id)+i)%4)
		product := fmt.Sprintf("product-%d", i%6)
		at := saleDates[i%len(saleDates)]
		q := int64(i%3 + 1)
		a := int64(1000 * (i%7 + 1))
		buf = fmt.Appendf(buf, "t-%d-%d,%s,%s,%d,%d,%s\n", id, i, outlet, product, q, a, at)
		k := DailyKey{Date: at[:10], OutletID: outlet, ProductID: product}
		d := want[k]
		d.DailyKey = k
		d.Transactions++
		d.Quantity += q
		d.Amount += a
		want[k] = d
	}
	return contentSource(id, string(buf)), want
}

func dailySources(n, rows int) ([]Source, map[DailyKey]DailySales) {
	var sources []Source
	want := map[DailyKey]DailySales{}
	for i := range n {
		s, w := dailySource(FileID(i), rows)
		sources = append(sources, s)
		for k, d := range w {
			got := want[k]
			got.DailyKey = k
			got.Transactions += d.Transactions
			got.Quantity += d.Quantity
			got.Amount += d.Amount
			want[k] = got
		}
	}
	return sources, want
}

func TestRunDailyMatchesIndependentSum(t *testing.T) {
	sources, want := dailySources(12, 300)
	for _, rw := range [][2]int{{1, 1}, {4, 8}} {
		t.Run(fmt.Sprintf("readers=%d workers=%d", rw[0], rw[1]), func(t *testing.T) {
			res, err := Run(t.Context(), runConfig(rw[0], rw[1]), sources)
			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			if len(res.Daily) != len(want) {
				t.Fatalf("got %d daily entries, want %d", len(res.Daily), len(want))
			}
			var sum Totals
			for _, d := range res.Daily {
				if d != want[d.DailyKey] {
					t.Fatalf("daily %+v, want %+v", d, want[d.DailyKey])
				}
				sum.Transactions += d.Transactions
				sum.Quantity += d.Quantity
				sum.Amount += d.Amount
			}
			if sum != res.Totals {
				t.Fatalf("daily entries sum to %+v, totals are %+v", sum, res.Totals)
			}
		})
	}
}

func TestRunDailyIsSortedAndDeterministic(t *testing.T) {
	sources, _ := dailySources(20, 200)
	first, err := Run(t.Context(), runConfig(4, 8), sources)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	for i := 1; i < len(first.Daily); i++ {
		a, b := first.Daily[i-1].DailyKey, first.Daily[i].DailyKey
		if compareDailyKeys(a, b) >= 0 {
			t.Fatalf("daily not sorted: %+v before %+v", a, b)
		}
	}
	for i := range 19 {
		got, err := Run(t.Context(), runConfig(4, 8), sources)
		if err != nil {
			t.Fatalf("run %d: %v", i+2, err)
		}
		if !reflect.DeepEqual(got.Daily, first.Daily) {
			t.Fatalf("run %d produced different daily entries", i+2)
		}
	}
}

func TestRunDailyUsesTheRecordedOffset(t *testing.T) {
	content := headerLine +
		"a,o1,p1,1,100,2026-10-30T23:59:59+07:00\n" +
		"b,o1,p1,1,100,2026-10-31T00:00:00+07:00\n" +
		"c,o2,p1,1,100,2026-10-31T01:00:00+07:00\n" +
		"d,o3,p1,1,100,2026-10-30T18:00:00Z\n"
	res, err := Run(t.Context(), runConfig(1, 1), []Source{contentSource(1, content)})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	got := map[string]string{}
	for _, d := range res.Daily {
		got[d.OutletID+"/"+d.Date] = d.Date
	}
	for _, want := range []string{"o1/2026-10-30", "o1/2026-10-31", "o2/2026-10-31", "o3/2026-10-30"} {
		if _, ok := got[want]; !ok {
			t.Fatalf("daily keys = %v, missing %s", got, want)
		}
	}
	if len(res.Daily) != 4 {
		t.Fatalf("got %d daily entries, want 4", len(res.Daily))
	}
}

func TestRunDailyIncludesPartialAndDropsOnCancel(t *testing.T) {
	partial := midFileIOErrorSource(1, 10, fmt.Errorf("disk on fire"))
	res, err := Run(t.Context(), runConfig(1, 1), []Source{partial})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	var tx int64
	for _, d := range res.Daily {
		tx += d.Transactions
	}
	if tx != 10 || fileResult(t, res, 1).Status != StatusPartial {
		t.Fatalf("daily holds %d transactions from the partial file, want 10", tx)
	}

	sources, _ := dailySources(5, 50)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sources = append(sources, Source{ID: 99, Open: func(ctx context.Context) (io.ReadCloser, error) {
		cancel()
		<-ctx.Done()
		return nil, ctx.Err()
	}})
	res, err = Run(ctx, runConfig(1, 2), sources)
	if err == nil {
		t.Fatal("Run() error = nil, want cancellation")
	}
	if len(res.Daily) != 0 {
		t.Fatalf("cancelled run returned %d daily entries, want none", len(res.Daily))
	}
}

type keyedGenerator struct {
	base, rows, next int
	buf              []byte
}

func (g *keyedGenerator) Read(p []byte) (int, error) {
	for len(g.buf) == 0 {
		if g.next > g.rows {
			return 0, io.EOF
		}
		if g.next == 0 {
			g.buf = append(g.buf, headerLine...)
		} else {
			n := g.base + g.next
			g.buf = fmt.Appendf(g.buf, "trx-%d,outlet-%d,product-%d,1,1000,2026-10-30T09:00:00+07:00\n", n, n%50, (n/50)%500)
		}
		g.next++
	}
	n := copy(p, g.buf)
	g.buf = g.buf[n:]
	return n, nil
}

func TestRunDailyMemoryGrowsWithKeysNotRows(t *testing.T) {
	if testing.Short() {
		t.Skip("processes two million rows")
	}
	const files, rows = 200, 10_000
	var sources []Source
	for i := range files {
		sources = append(sources, Source{ID: FileID(i), Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(&keyedGenerator{base: i * rows, rows: rows}), nil
		}})
	}
	stop := make(chan struct{})
	peak := make(chan uint64, 1)
	go func() {
		var highest uint64
		var ms runtime.MemStats
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				peak <- highest
				return
			case <-ticker.C:
				runtime.ReadMemStats(&ms)
				highest = max(highest, ms.HeapAlloc)
			}
		}
	}()
	res, err := Run(t.Context(), DefaultConfig(), sources)
	close(stop)
	highest := <-peak
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(res.Daily) != 25_000 {
		t.Fatalf("got %d daily keys, want 25000", len(res.Daily))
	}
	if highest > 64<<20 {
		t.Fatalf("peak HeapAlloc = %d MB, want below 64 MB", highest>>20)
	}
	t.Logf("peak HeapAlloc %.1f MB for %d keys", float64(highest)/(1<<20), len(res.Daily))
}

func TestRunStopsWhenDailyKeysExceedTheCap(t *testing.T) {
	var sources []Source
	for i := range 20 {
		sources = append(sources, Source{ID: FileID(i), Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(&keyedGenerator{base: i * 50_000, rows: 50_000}), nil
		}})
	}
	cfg := runConfig(4, 4)
	cfg.MaxDailyKeys = 1000
	start := time.Now()
	res, err := Run(t.Context(), cfg, sources)
	if !errors.Is(err, ErrTooManyKeys) {
		t.Fatalf("Run() error = %v, want ErrTooManyKeys", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Run took %v after hitting the cap, want it to stop early", d)
	}
	if res.Totals != (Totals{}) || len(res.Daily) != 0 {
		t.Fatalf("result after overflow = %+v totals, %d daily, want none", res.Totals, len(res.Daily))
	}

	cfg.MaxDailyKeys = 25_000
	if _, err := Run(t.Context(), cfg, sources[:1]); err != nil {
		t.Fatalf("Run() under the cap: %v", err)
	}
}
