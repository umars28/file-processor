package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const cancelDeadline = 200 * time.Millisecond

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func assertCancelledResult(t *testing.T, res Result) {
	t.Helper()
	if res.Totals != (Totals{}) {
		t.Fatalf("Totals = %+v, want zero for a cancelled run", res.Totals)
	}
	for _, f := range res.Files {
		if isContextError(f.Err) {
			t.Fatalf("file %d carries a raw context error: %v", f.ID, f.Err)
		}
		if f.Done == f.Cancelled {
			t.Fatalf("file %d = %+v, want exactly one of Done and Cancelled", f.ID, f)
		}
	}
}

func waitForCancel(id FileID) Source {
	return Source{ID: id, Open: func(ctx context.Context) (io.ReadCloser, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
}

func TestRunCancelAtRandomPoints(t *testing.T) {
	sources, _ := generateSources(100, 1000)
	sources = append(sources, waitForCancel(100))
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range 200 {
		tracker := &openTracker{}
		wrapped := make([]Source, len(sources))
		for j, s := range sources {
			wrapped[j] = tracker.wrap(s)
		}
		ctx, cancel := context.WithCancel(t.Context())
		delay := time.Duration(rng.IntN(5000)) * time.Microsecond
		cancelledAt := make(chan time.Time, 1)
		timer := time.AfterFunc(delay, func() {
			cancelledAt <- time.Now()
			cancel()
		})

		type outcome struct {
			res Result
			err error
			at  time.Time
		}
		out := make(chan outcome, 1)
		go func() {
			res, err := Run(ctx, runConfig(4, 8), wrapped)
			out <- outcome{res, err, time.Now()}
		}()

		var got outcome
		select {
		case got = <-out:
		case <-time.After(5 * time.Second):
			t.Fatalf("run %d: Run did not return", i)
		}
		timer.Stop()
		cancel()

		if !errors.Is(got.err, context.Canceled) {
			t.Fatalf("run %d (cancel after %v): Run() error = %v, want context.Canceled", i, delay, got.err)
		}
		if d := got.at.Sub(<-cancelledAt); d > cancelDeadline {
			t.Fatalf("run %d returned %v after cancel, want within %v", i, d, cancelDeadline)
		}
		assertCancelledResult(t, got.res)
		tracker.mu.Lock()
		opened, closed := tracker.opened, tracker.closed
		tracker.mu.Unlock()
		if opened != closed {
			t.Fatalf("run %d: opened %d sources but closed %d", i, opened, closed)
		}
	}
}

func TestRunReturnsPromptlyAfterCancel(t *testing.T) {
	sources, _ := generateSources(100, 1000)
	for i := range 20 {
		ctx, cancel := context.WithCancel(t.Context())
		out := make(chan time.Time, 1)
		go func() {
			_, _ = Run(ctx, runConfig(4, 8), sources)
			out <- time.Now()
		}()
		time.Sleep(time.Duration(i%5+1) * time.Millisecond)
		cancelledAt := time.Now()
		cancel()
		select {
		case returnedAt := <-out:
			if d := returnedAt.Sub(cancelledAt); d > cancelDeadline {
				t.Fatalf("run %d returned %v after cancel, want within %v", i, d, cancelDeadline)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("run %d: Run did not return", i)
		}
	}
}

func TestRunDeadlineBehavesLikeCancel(t *testing.T) {
	sources, _ := generateSources(100, 1000)
	sources = append(sources, waitForCancel(100))
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Millisecond)
	defer cancel()
	res, err := Run(ctx, runConfig(4, 8), sources)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want context.DeadlineExceeded", err)
	}
	assertCancelledResult(t, res)
	cancelled := 0
	for _, f := range res.Files {
		if f.Cancelled {
			cancelled++
		}
	}
	if cancelled == 0 {
		t.Fatal("no file marked cancelled after the deadline")
	}
}

func TestRunKeepsReportsOfFilesFinishedBeforeCancel(t *testing.T) {
	first, _ := generateSource(0, 50)
	badHeader := contentSource(1, "nope\n")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	opened := make(chan struct{})
	blocking := Source{ID: 2, Open: func(ctx context.Context) (io.ReadCloser, error) {
		close(opened)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	never, _ := generateSource(3, 50)

	go func() {
		<-opened
		cancel()
	}()
	res, err := Run(ctx, runConfig(1, 2), []Source{first, badHeader, blocking, never})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	assertCancelledResult(t, res)

	if f := fileResult(t, res, 0); !f.Done || f.Cancelled || f.Valid != 50 || f.Err != nil {
		t.Fatalf("finished file = %+v, want done with 50 valid rows", f)
	}
	if f := fileResult(t, res, 1); !f.Done || !errors.Is(f.Err, ErrHeaderMismatch) {
		t.Fatalf("failed file = %+v, want done with its header error kept", f)
	}
	for _, id := range []FileID{2, 3} {
		if f := fileResult(t, res, id); !f.Cancelled || f.Done || f.Valid != 0 {
			t.Fatalf("file %d = %+v, want cancelled with no counts", id, f)
		}
	}
}

func TestRunBlockingOpenDoesNotHoldUpCancel(t *testing.T) {
	block := func(ctx context.Context) (io.ReadCloser, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	sources := []Source{{ID: 1, Open: block}, {ID: 2, Open: block}}
	ctx, cancel := context.WithCancel(t.Context())
	time.AfterFunc(20*time.Millisecond, cancel)
	start := time.Now()
	res, err := Run(ctx, runConfig(2, 2), sources)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	if d := time.Since(start); d > 20*time.Millisecond+cancelDeadline {
		t.Fatalf("Run returned after %v, want within %v of cancel", d, cancelDeadline)
	}
	for _, f := range res.Files {
		if !f.Cancelled || f.Err != nil {
			t.Fatalf("file = %+v, want cancelled without a raw context error", f)
		}
	}
}

type cancelOnLastClose struct {
	io.ReadCloser
	remaining *atomic.Int64
	cancel    context.CancelFunc
	once      sync.Once
}

func (c *cancelOnLastClose) Close() error {
	c.once.Do(func() {
		if c.remaining.Add(-1) == 0 {
			c.cancel()
		}
	})
	return c.ReadCloser.Close()
}

func TestRunCancelAfterEveryFileFinished(t *testing.T) {
	sources, want := generateSources(20, 50)
	for _, readers := range []int{1, 4} {
		for i := range 50 {
			ctx, cancel := context.WithCancel(t.Context())
			remaining := &atomic.Int64{}
			remaining.Store(int64(len(sources)))
			wrapped := make([]Source, len(sources))
			for j, s := range sources {
				wrapped[j] = Source{ID: s.ID, Open: func(ctx context.Context) (io.ReadCloser, error) {
					rc, err := s.Open(ctx)
					if err != nil {
						return nil, err
					}
					return &cancelOnLastClose{ReadCloser: rc, remaining: remaining, cancel: cancel}, nil
				}}
			}
			res, err := Run(ctx, runConfig(readers, 4), wrapped)
			if ctx.Err() == nil {
				t.Fatalf("readers=%d run %d: context was never cancelled", readers, i)
			}
			cancel()
			if err != nil {
				t.Fatalf("readers=%d run %d: Run() error = %v, want nil when every file finished", readers, i, err)
			}
			if res.Totals != want {
				t.Fatalf("readers=%d run %d: Totals = %+v, want %+v", readers, i, res.Totals, want)
			}
			for _, f := range res.Files {
				if f.Cancelled || !f.Done {
					t.Fatalf("readers=%d run %d: file = %+v, want done and not cancelled", readers, i, f)
				}
			}
		}
	}
}

func TestRunSourceDeadlineIsAFileErrorNotACancel(t *testing.T) {
	errDial := fmt.Errorf("dial s3: %w", context.DeadlineExceeded)
	good, want := generateSources(5, 60)
	_, partial := generateSource(11, 34)
	want.Transactions += partial.Transactions
	want.Quantity += partial.Quantity
	want.Amount += partial.Amount
	sources := append(good,
		Source{ID: 10, Open: func(context.Context) (io.ReadCloser, error) { return nil, errDial }},
		midFileIOErrorSource(11, 34, fmt.Errorf("read s3: %w", context.DeadlineExceeded)),
	)

	res, err := Run(t.Context(), runConfig(2, 3), sources)
	if err != nil {
		t.Fatalf("Run() error = %v, want nil: the run itself was never cancelled", err)
	}
	if res.Totals != want {
		t.Fatalf("Totals = %+v, want %+v", res.Totals, want)
	}
	if f := fileResult(t, res, 10); f.Cancelled || !f.Done || !errors.Is(f.Err, errDial) {
		t.Fatalf("open failure = %+v, want a done file keeping its own error", f)
	}
	if f := fileResult(t, res, 11); f.Cancelled || !f.Done || f.Valid != 34 || !errors.Is(f.Err, context.DeadlineExceeded) {
		t.Fatalf("mid-read failure = %+v, want a done file with 34 rows and its own error", f)
	}
}

type blockingRead struct {
	closed chan struct{}
	once   sync.Once
}

func (b *blockingRead) Read([]byte) (int, error) {
	<-b.closed
	return 0, errors.New("read on closed source")
}

func (b *blockingRead) Close() error {
	b.once.Do(func() { close(b.closed) })
	return nil
}

func TestRunBlockingReadDoesNotHoldUpCancel(t *testing.T) {
	good, _ := generateSource(1, 20)
	stuck := Source{ID: 2, Open: func(context.Context) (io.ReadCloser, error) {
		return &blockingRead{closed: make(chan struct{})}, nil
	}}
	ctx, cancel := context.WithCancel(t.Context())
	time.AfterFunc(20*time.Millisecond, cancel)
	start := time.Now()
	res, err := Run(ctx, runConfig(2, 2), []Source{good, stuck})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	if d := time.Since(start); d > 20*time.Millisecond+cancelDeadline {
		t.Fatalf("Run returned after %v, want within %v of cancel", d, cancelDeadline)
	}
	if f := fileResult(t, res, 2); !f.Cancelled || f.Err != nil {
		t.Fatalf("stuck file = %+v, want cancelled without an error", f)
	}
}

type eofAfterClose struct {
	head   io.Reader
	closed chan struct{}
	once   sync.Once
	mu     sync.Mutex
}

func (e *eofAfterClose) Read(p []byte) (int, error) {
	e.mu.Lock()
	n, err := e.head.Read(p)
	e.mu.Unlock()
	if n > 0 || !errors.Is(err, io.EOF) {
		return n, err
	}
	<-e.closed
	return 0, io.EOF
}

func (e *eofAfterClose) Close() error {
	e.once.Do(func() { close(e.closed) })
	return nil
}

func TestRunSourceCutShortByCancelIsCancelled(t *testing.T) {
	head, _ := generateSource(1, 64)
	cut := Source{ID: 1, Open: func(ctx context.Context) (io.ReadCloser, error) {
		rc, err := head.Open(ctx)
		if err != nil {
			return nil, err
		}
		return &eofAfterClose{head: rc, closed: make(chan struct{})}, nil
	}}
	ctx, cancel := context.WithCancel(t.Context())
	time.AfterFunc(20*time.Millisecond, cancel)
	res, err := Run(ctx, runConfig(1, 2), []Source{cut})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled for a file cut short by cancel", err)
	}
	assertCancelledResult(t, res)
	if f := fileResult(t, res, 1); !f.Cancelled || f.Done {
		t.Fatalf("file = %+v, want cancelled", f)
	}
}
