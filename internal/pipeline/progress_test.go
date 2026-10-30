package pipeline

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func sizedSource(id FileID, rows int) (Source, Totals) {
	src, totals := generateSource(id, rows)
	rc, _ := src.Open(context.Background())
	content, _ := io.ReadAll(rc)
	return Source{ID: id, Size: int64(len(content)), Open: func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(string(content))), nil
	}}, totals
}

type slowReader struct {
	lines []string
	delay time.Duration
}

func (s *slowReader) Read(p []byte) (int, error) {
	if len(s.lines) == 0 {
		return 0, io.EOF
	}
	time.Sleep(s.delay)
	n := copy(p, s.lines[0])
	s.lines[0] = s.lines[0][n:]
	if s.lines[0] == "" {
		s.lines = s.lines[1:]
	}
	return n, nil
}

func slowSource(id FileID, rows int, delay time.Duration) Source {
	src, _ := sizedSource(id, rows)
	rc, _ := src.Open(context.Background())
	content, _ := io.ReadAll(rc)
	return Source{ID: id, Size: src.Size, Open: func(context.Context) (io.ReadCloser, error) {
		lines := strings.SplitAfter(string(content), "\n")
		return io.NopCloser(&slowReader{lines: lines, delay: delay}), nil
	}}
}

func collectSnapshots(p *Progress, stop <-chan struct{}) <-chan []*Snapshot {
	out := make(chan []*Snapshot, 1)
	go func() {
		var seen []*Snapshot
		var last *Snapshot
		for {
			select {
			case <-stop:
				out <- seen
				return
			default:
			}
			if s := p.Snapshot(); s != nil && s != last {
				seen = append(seen, s)
				last = s
			}
			time.Sleep(time.Millisecond)
		}
	}()
	return out
}

func checkConsistent(t *testing.T, s *Snapshot) {
	t.Helper()
	if err := consistencyError(s); err != "" {
		t.Fatal(err)
	}
}

func consistencyError(s *Snapshot) string {
	var valid, invalid int64
	done := 0
	for _, f := range s.Files {
		valid += f.Valid
		invalid += f.Invalid
		if f.Status != StatusPending && f.Status != StatusRunning {
			done++
		}
	}
	if valid != s.Valid || invalid != s.Invalid {
		return fmt.Sprintf("snapshot files sum to valid=%d invalid=%d, totals say %d and %d", valid, invalid, s.Valid, s.Invalid)
	}
	if done != s.FilesDone || len(s.Files) != s.FilesTotal {
		return fmt.Sprintf("FilesDone = %d of %d, files with a final status = %d of %d", s.FilesDone, s.FilesTotal, done, len(s.Files))
	}
	return ""
}

func TestProgressSnapshotBeforeRun(t *testing.T) {
	p := NewProgress(10 * time.Millisecond)
	if s := p.Snapshot(); s != nil {
		t.Fatalf("Snapshot() before any publish = %+v, want nil", s)
	}
}

func TestProgressByteBasedPercent(t *testing.T) {
	a, _ := sizedSource(1, 200)
	b, _ := sizedSource(2, 50)
	unsized, _ := generateSource(3, 30)
	p := NewProgress(10 * time.Millisecond)
	if _, err := RunWithProgress(t.Context(), runConfig(2, 2), []Source{a, b, unsized}, p); err != nil {
		t.Fatalf("RunWithProgress() error = %v", err)
	}
	s := p.Snapshot()
	if s == nil || !s.Final {
		t.Fatalf("final snapshot = %+v, want Final", s)
	}
	for _, f := range s.Files {
		pct, known := f.Percent()
		switch f.ID {
		case 1, 2:
			want := map[FileID]int64{1: a.Size, 2: b.Size}[f.ID]
			if f.Size != want || f.BytesRead != want || !known || pct != 100 {
				t.Fatalf("file %d = %+v (%.1f%%, known=%v), want all %d bytes read and 100%%", f.ID, f, pct, known, want)
			}
		case 3:
			if known {
				t.Fatalf("unsized file reports %.1f%%, want no percentage", pct)
			}
		}
	}
	if s.BytesTotal != a.Size+b.Size || s.BytesRead != s.BytesTotal {
		t.Fatalf("bytes = %d of %d, want %d of %d", s.BytesRead, s.BytesTotal, a.Size+b.Size, a.Size+b.Size)
	}
	if pct, known := s.Percent(); !known || pct != 100 {
		t.Fatalf("overall = %.1f%% (known=%v), want 100%%", pct, known)
	}
}

func TestProgressIntermediateSnapshotsNeverGoBackwards(t *testing.T) {
	const interval = 10 * time.Millisecond
	sources := []Source{slowSource(1, 60, time.Millisecond), slowSource(2, 60, time.Millisecond), slowSource(3, 60, time.Millisecond)}
	p := NewProgress(interval)
	stop := make(chan struct{})
	collected := collectSnapshots(p, stop)
	cfg := runConfig(2, 2)
	cfg.BatchSize = 4
	if _, err := RunWithProgress(t.Context(), cfg, sources, p); err != nil {
		t.Fatalf("RunWithProgress() error = %v", err)
	}
	close(stop)
	snaps := <-collected

	var intermediate []*Snapshot
	for _, s := range snaps {
		checkConsistent(t, s)
		if !s.Final {
			intermediate = append(intermediate, s)
		}
	}
	if len(intermediate) < 3 {
		t.Fatalf("got %d intermediate snapshots, want at least 3", len(intermediate))
	}
	for i := 1; i < len(intermediate); i++ {
		prev, cur := intermediate[i-1], intermediate[i]
		if cur.BytesRead < prev.BytesRead || cur.Valid < prev.Valid || cur.Invalid < prev.Invalid || cur.FilesDone < prev.FilesDone {
			t.Fatalf("snapshot %d went backwards: %+v after %+v", i, cur, prev)
		}
		if gap := cur.Elapsed - prev.Elapsed; gap < interval/2 {
			t.Fatalf("snapshots %d and %d published %v apart, want about %v", i-1, i, gap, interval)
		}
	}
}

func TestProgressSnapshotsConsistentUnderConcurrentReads(t *testing.T) {
	sources, _ := generateSources(40, 300)
	p := NewProgress(time.Millisecond)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				if s := p.Snapshot(); s != nil {
					if err := consistencyError(s); err != "" {
						t.Error(err)
						return
					}
				}
			}
		})
	}
	_, err := RunWithProgress(t.Context(), runConfig(4, 8), sources, p)
	close(stop)
	wg.Wait()
	if err != nil {
		t.Fatalf("RunWithProgress() error = %v", err)
	}
}

func assertFinalMatchesResult(t *testing.T, s *Snapshot, res Result) {
	t.Helper()
	if s == nil || !s.Final {
		t.Fatalf("final snapshot = %+v, want Final", s)
	}
	checkConsistent(t, s)
	if s.Totals != res.Totals || len(s.Files) != len(res.Files) {
		t.Fatalf("snapshot totals %+v over %d files, result %+v over %d", s.Totals, len(s.Files), res.Totals, len(res.Files))
	}
	for i, f := range s.Files {
		r := res.Files[i]
		if f.ID != r.ID || f.Status != r.Status || f.Valid != r.Valid || f.Invalid != r.Invalid {
			t.Fatalf("snapshot file %+v, result file %+v", f, r)
		}
	}
}

func TestProgressFinalSnapshotMatchesResult(t *testing.T) {
	sources, _ := generateSources(10, 100)
	sources = append(sources, contentSource(10, invalidRows(5)), contentSource(11, "nope\n"))
	p := NewProgress(5 * time.Millisecond)
	res, err := RunWithProgress(t.Context(), runConfig(3, 4), sources, p)
	if err != nil {
		t.Fatalf("RunWithProgress() error = %v", err)
	}
	assertFinalMatchesResult(t, p.Snapshot(), res)
}

func TestProgressFinalSnapshotMatchesCancelledResult(t *testing.T) {
	sources, _ := generateSources(10, 100)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sources = append(sources, Source{ID: 10, Open: func(ctx context.Context) (io.ReadCloser, error) {
		cancel()
		<-ctx.Done()
		return nil, ctx.Err()
	}})
	p := NewProgress(5 * time.Millisecond)
	res, err := RunWithProgress(ctx, runConfig(1, 2), sources, p)
	if err == nil {
		t.Fatal("RunWithProgress() error = nil, want cancellation")
	}
	assertFinalMatchesResult(t, p.Snapshot(), res)
	if p.Snapshot().Totals != (Totals{}) {
		t.Fatalf("cancelled final snapshot totals = %+v, want zero like the result", p.Snapshot().Totals)
	}
}

func TestProgressAddsNoGoroutines(t *testing.T) {
	sources, _ := generateSources(500, 20)
	cfg := runConfig(4, 8)
	var mu sync.Mutex
	highest := 0
	sampled := make([]Source, len(sources))
	for i, s := range sources {
		sampled[i] = Source{ID: s.ID, Open: func(ctx context.Context) (io.ReadCloser, error) {
			n := runtime.NumGoroutine()
			mu.Lock()
			highest = max(highest, n)
			mu.Unlock()
			return s.Open(ctx)
		}}
	}
	base := runtime.NumGoroutine()
	if _, err := RunWithProgress(t.Context(), cfg, sampled, NewProgress(time.Millisecond)); err != nil {
		t.Fatalf("RunWithProgress() error = %v", err)
	}
	if limit := base + cfg.Readers + cfg.Workers + 4; highest > limit {
		t.Fatalf("peak goroutines = %d with progress, want at most %d", highest, limit)
	}
}

func TestProgressWrongSizeStaysCoherent(t *testing.T) {
	half, _ := sizedSource(1, 100)
	half.Size /= 2
	double, _ := sizedSource(2, 100)
	double.Size *= 2
	failedEarly, _ := sizedSource(3, 100)
	failedEarly.Open = func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("nope\n" + strings.Repeat("x", 4000))), nil
	}

	p := NewProgress(time.Millisecond)
	stop := make(chan struct{})
	collected := collectSnapshots(p, stop)
	if _, err := RunWithProgress(t.Context(), runConfig(2, 2), []Source{half, double, failedEarly}, p); err != nil {
		t.Fatalf("RunWithProgress() error = %v", err)
	}
	close(stop)
	snaps := append(<-collected, p.Snapshot())
	for _, s := range snaps {
		var sum int64
		for _, f := range s.Files {
			if f.BytesRead > f.Size {
				t.Fatalf("file %d read %d bytes of a declared %d", f.ID, f.BytesRead, f.Size)
			}
			if f.Status == StatusPending && f.BytesRead > 0 {
				t.Fatalf("file %d is pending with %d bytes read", f.ID, f.BytesRead)
			}
			sum += f.BytesRead
		}
		if sum != s.BytesRead {
			t.Fatalf("per-file bytes sum to %d, snapshot says %d", sum, s.BytesRead)
		}
	}
	final := p.Snapshot()
	if pct, ok := final.Percent(); !ok || pct != 100 {
		t.Fatalf("final overall = %.1f%%, want 100%% once every file finished", pct)
	}
	for _, f := range final.Files {
		if pct, _ := f.Percent(); pct != 100 {
			t.Fatalf("final file %+v at %.1f%%, want 100%%", f, pct)
		}
	}
}
