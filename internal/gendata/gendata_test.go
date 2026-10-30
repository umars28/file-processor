package gendata_test

import (
	"bytes"
	"context"
	"io"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/umars28/file-processor/internal/gendata"
	"github.com/umars28/file-processor/internal/pipeline"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func options(rows int, rate float64, seed uint64) gendata.Options {
	o := gendata.Defaults()
	o.Rows, o.InvalidRate, o.Seed = rows, rate, seed
	return o
}

func TestGenerateIsDeterministic(t *testing.T) {
	var a, b, c bytes.Buffer
	if _, err := gendata.Generate(&a, options(5000, 0.05, 7)); err != nil {
		t.Fatal(err)
	}
	if _, err := gendata.Generate(&b, options(5000, 0.05, 7)); err != nil {
		t.Fatal(err)
	}
	if _, err := gendata.Generate(&c, options(5000, 0.05, 8)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a.Bytes(), b.Bytes()) {
		t.Fatal("same seed produced different output")
	}
	if bytes.Equal(a.Bytes(), c.Bytes()) {
		t.Fatal("different seeds produced the same output")
	}
}

func TestGeneratedInvalidRowsMatchThePipeline(t *testing.T) {
	var buf bytes.Buffer
	stats, err := gendata.Generate(&buf, options(20_000, 0.1, 3))
	if err != nil {
		t.Fatal(err)
	}
	if stats.Bytes != int64(buf.Len()) || stats.Valid+sum(stats.Invalid) != stats.Rows {
		t.Fatalf("stats = %+v for %d bytes", stats, buf.Len())
	}
	if len(stats.Invalid) != 5 {
		t.Fatalf("invalid kinds = %v, want all 5", stats.Invalid)
	}
	content := buf.String()
	src := pipeline.Source{ID: 0, Size: int64(len(content)), Open: func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(content)), nil
	}}
	res, err := pipeline.Run(t.Context(), pipeline.DefaultConfig(), []pipeline.Source{src})
	if err != nil {
		t.Fatal(err)
	}
	f := res.Files[0]
	want := map[string]int64{}
	for k, v := range stats.Invalid {
		want[k] = int64(v)
	}
	if !reflect.DeepEqual(f.ErrorCounts, want) || f.Valid != int64(stats.Valid) {
		t.Fatalf("pipeline counted %v invalid and %d valid, generator wrote %v and %d", f.ErrorCounts, f.Valid, stats.Invalid, stats.Valid)
	}
}

func sum(m map[string]int) int {
	n := 0
	for _, v := range m {
		n += v
	}
	return n
}

func TestGenerateMemoryStaysBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("writes ten million rows")
	}
	stop := make(chan struct{})
	peak := make(chan uint64, 1)
	go func() {
		var highest uint64
		var ms runtime.MemStats
		ticker := time.NewTicker(5 * time.Millisecond)
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
	stats, err := gendata.Generate(io.Discard, options(10_000_000, 0.01, 1))
	close(stop)
	highest := <-peak
	if err != nil {
		t.Fatal(err)
	}
	if stats.Rows != 10_000_000 {
		t.Fatalf("rows = %d", stats.Rows)
	}
	if highest > 32<<20 {
		t.Fatalf("peak HeapAlloc = %d MB, want below 32 MB", highest>>20)
	}
	t.Logf("10M rows, %d MB written, peak HeapAlloc %.1f MB", stats.Bytes>>20, float64(highest)/(1<<20))
}

func TestOptionsValidate(t *testing.T) {
	for name, modify := range map[string]func(*gendata.Options){
		"negative rows": func(o *gendata.Options) { o.Rows = -1 },
		"no outlets":    func(o *gendata.Options) { o.Outlets = 0 },
		"rate above 1":  func(o *gendata.Options) { o.InvalidRate = 1.5 },
	} {
		t.Run(name, func(t *testing.T) {
			o := gendata.Defaults()
			modify(&o)
			if _, err := gendata.Generate(io.Discard, o); err == nil {
				t.Fatal("Generate() = nil error, want rejection")
			}
		})
	}
}
