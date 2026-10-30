package pipeline

import (
	"bytes"
	"context"
	"encoding/csv"
	"io"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/umars28/file-processor/internal/gendata"
)

func benchCSV(b testing.TB, rows int) string {
	b.Helper()
	o := gendata.Defaults()
	o.Rows = rows
	var buf bytes.Buffer
	if _, err := gendata.Generate(&buf, o); err != nil {
		b.Fatal(err)
	}
	return buf.String()
}

func benchRecords(b *testing.B, content string) [][]string {
	b.Helper()
	r := csv.NewReader(strings.NewReader(content))
	records, err := r.ReadAll()
	if err != nil {
		b.Fatal(err)
	}
	return records[1:]
}

func BenchmarkReadCSV(b *testing.B) {
	content := benchCSV(b, 100_000)
	cfg := DefaultConfig()
	b.SetBytes(int64(len(content)))
	b.ReportAllocs()
	for b.Loop() {
		out := make(chan Batch, 8)
		done := make(chan struct{})
		go func() {
			for range out {
			}
			close(done)
		}()
		if _, err := ReadCSV(context.Background(), 0, strings.NewReader(content), cfg, out); err != nil {
			b.Fatal(err)
		}
		close(out)
		<-done
	}
}

func BenchmarkParseTransaction(b *testing.B) {
	records := benchRecords(b, benchCSV(b, 10_000))
	b.SetBytes(avgRowBytes(records))
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if _, err := ParseTransaction(records[i%len(records)]); err != nil {
			b.Fatal(err)
		}
		i++
	}
}

func BenchmarkAggregate(b *testing.B) {
	records := benchRecords(b, benchCSV(b, 50_000))
	txs := make([]Transaction, len(records))
	for i, r := range records {
		tx, err := ParseTransaction(r)
		if err != nil {
			b.Fatal(err)
		}
		txs[i] = tx
	}
	b.SetBytes(avgRowBytes(records))
	b.ReportAllocs()
	s := newSums(0)
	i := 0
	for b.Loop() {
		s.add(txs[i%len(txs)])
		i++
	}
	b.ReportMetric(float64(len(s.daily)), "keys")
}

func BenchmarkAggregateNewKeys(b *testing.B) {
	records := benchRecords(b, benchCSV(b, 1000))
	base, err := ParseTransaction(records[0])
	if err != nil {
		b.Fatal(err)
	}
	ids := make([]string, 1<<20)
	for i := range ids {
		ids[i] = "product-" + strconv.Itoa(i)
	}
	b.SetBytes(avgRowBytes(records))
	b.ReportAllocs()
	s := newSums(0)
	i := 0
	for b.Loop() {
		if i == len(ids) {
			b.StopTimer()
			s = newSums(0)
			i = 0
			b.StartTimer()
		}
		tx := base
		tx.ProductID = ids[i]
		s.add(tx)
		i++
	}
}

func avgRowBytes(records [][]string) int64 {
	var size int
	for _, r := range records {
		size += len(strings.Join(r, ",")) + 1
	}
	return int64(size / len(records))
}

func BenchmarkRun(b *testing.B) {
	content := benchCSV(b, 250_000)
	sources := make([]Source, 4)
	for i := range sources {
		sources[i] = Source{ID: FileID(i), Size: int64(len(content)), Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(content)), nil
		}}
	}
	b.SetBytes(int64(len(content) * len(sources)))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Run(context.Background(), DefaultConfig(), sources); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(250_000*len(sources))*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
}

type uniqueKeyReader struct {
	source int
	next   int
	buf    []byte
}

func (u *uniqueKeyReader) Read(p []byte) (int, error) {
	for len(u.buf) < len(p) {
		if u.next == 0 {
			u.buf = append(u.buf, headerLine...)
		} else {
			u.buf = append(u.buf, "t,o"...)
			u.buf = strconv.AppendInt(u.buf, int64(u.source), 10)
			u.buf = append(u.buf, ",p"...)
			u.buf = strconv.AppendInt(u.buf, int64(u.next), 10)
			u.buf = append(u.buf, ",1,1000,2026-10-30T09:00:00+07:00\n"...)
		}
		u.next++
	}
	n := copy(p, u.buf)
	u.buf = u.buf[n:]
	return n, nil
}

func TestCancelLatencyWithManyKeys(t *testing.T) {
	if testing.Short() {
		t.Skip("measures 50 cancellations")
	}
	const runs, minKeys = 50, 100_000
	latencies := make([]time.Duration, 0, runs)
	for range runs {
		sources := make([]Source, 4)
		for i := range sources {
			sources[i] = Source{ID: FileID(i), Open: func(context.Context) (io.ReadCloser, error) {
				return io.NopCloser(&uniqueKeyReader{source: i}), nil
			}}
		}
		p := NewProgress(time.Millisecond)
		ctx, cancel := context.WithCancel(t.Context())
		returned := make(chan time.Time, 1)
		go func() {
			_, _ = RunWithProgress(ctx, DefaultConfig(), sources, p)
			returned <- time.Now()
		}()
		deadline := time.Now().Add(30 * time.Second)
		for s := p.Snapshot(); s == nil || s.Valid < minKeys; s = p.Snapshot() {
			if time.Now().After(deadline) {
				cancel()
				<-returned
				t.Fatal("did not reach the key count in time")
			}
			time.Sleep(time.Millisecond)
		}
		start := time.Now()
		cancel()
		latencies = append(latencies, (<-returned).Sub(start))
	}
	slices.Sort(latencies)
	median, worst := latencies[len(latencies)/2], latencies[len(latencies)-1]
	t.Logf("cancel-to-return over %d runs with at least %d distinct keys: median %v, worst %v", runs, minKeys, median, worst)
	if worst > 200*time.Millisecond {
		t.Fatalf("worst cancel-to-return %v exceeds the 200ms target", worst)
	}
}
