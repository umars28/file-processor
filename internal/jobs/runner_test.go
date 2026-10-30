package jobs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/umars28/file-processor/internal/pipeline"
	"github.com/umars28/file-processor/internal/report"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

const header = "transaction_id,outlet_id,product_id,quantity,amount,transacted_at\n"

type fakeStore struct {
	mu         sync.Mutex
	next       int
	status     map[string]string
	reasons    map[string]string
	docs       map[string]report.Document
	order      []string
	running    int
	maxRunning int
	gate       chan struct{}
	saveErr    error
	runErr     error
	finished   chan string
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		status:   map[string]string{},
		reasons:  map[string]string{},
		docs:     map[string]report.Document{},
		finished: make(chan string, 100),
	}
}

func (s *fakeStore) CreateJob(context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.next++
	id := fmt.Sprintf("job-%d", s.next)
	s.status[id] = "queued"
	return id, nil
}

func (s *fakeStore) MarkRunning(_ context.Context, id string) error {
	s.mu.Lock()
	if s.runErr != nil {
		s.mu.Unlock()
		return s.runErr
	}
	s.status[id] = "running"
	s.order = append(s.order, id)
	s.running++
	s.maxRunning = max(s.maxRunning, s.running)
	gate := s.gate
	s.mu.Unlock()
	if gate != nil {
		<-gate
	}
	return nil
}

func (s *fakeStore) finish(id, status string) {
	s.mu.Lock()
	if s.status[id] == "running" {
		s.running--
	}
	s.status[id] = status
	s.mu.Unlock()
	s.finished <- id
}

func (s *fakeStore) MarkFailed(_ context.Context, id, reason string) error {
	s.mu.Lock()
	s.reasons[id] = reason
	s.mu.Unlock()
	s.finish(id, "failed")
	return nil
}

func (s *fakeStore) SaveResult(_ context.Context, id string, doc report.Document) error {
	s.mu.Lock()
	err := s.saveErr
	if err == nil {
		s.docs[id] = doc
	}
	s.mu.Unlock()
	if err != nil {
		return err
	}
	s.finish(id, doc.Status)
	return nil
}

func (s *fakeStore) waitFinished(t *testing.T, n int) {
	t.Helper()
	for range n {
		select {
		case <-s.finished:
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for %d jobs to finish", n)
		}
	}
}

func (s *fakeStore) get(id string) (string, string, report.Document) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status[id], s.reasons[id], s.docs[id]
}

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func testOptions(dir string) Options {
	cfg := pipeline.DefaultConfig()
	cfg.Readers, cfg.Workers = 2, 2
	return Options{DataDir: dir, Pipeline: cfg, MaxJobs: 2, QueueSize: 10, ProgressInterval: 5 * time.Millisecond}
}

func startRunner(t *testing.T, store Store, opts Options) (*Runner, context.CancelFunc) {
	t.Helper()
	r, err := New(store, opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.Start(ctx)
	t.Cleanup(func() {
		cancel()
		r.Wait(context.Background())
	})
	return r, cancel
}

func TestSubmitValidatesPaths(t *testing.T) {
	dir := t.TempDir()
	outside := t.TempDir()
	writeFile(t, dir, "ok.csv", header)
	writeFile(t, outside, "secret.csv", header)
	if err := os.Symlink(filepath.Join(outside, "secret.csv"), filepath.Join(dir, "escape.csv")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o750); err != nil {
		t.Fatal(err)
	}
	r, _ := startRunner(t, newFakeStore(), testOptions(dir))

	tooMany := make([]string, MaxFiles+1)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("f%d.csv", i)
	}
	tests := map[string][]string{
		"empty list":     {},
		"too many files": tooMany,
		"empty path":     {""},
		"absolute path":  {filepath.Join(dir, "ok.csv")},
		"parent escape":  {"../x.csv"},
		"dot dot inside": {"sub/../ok.csv"},
		"symlink escape": {"escape.csv"},
		"directory":      {"sub"},
		"duplicate":      {"ok.csv", "./ok.csv"},
	}
	for name, files := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := r.Submit(t.Context(), files); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("Submit(%q) = %v, want ErrInvalidRequest", files, err)
			}
		})
	}
	if _, err := r.Submit(t.Context(), []string{"ok.csv", "missing.csv"}); err != nil {
		t.Fatalf("Submit with a missing file = %v, want the job accepted", err)
	}
}

func TestJobsRunBoundedAndInOrder(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a.csv", header+"t1,o1,p1,1,100,2026-10-30T09:00:00Z\n")
	store := newFakeStore()
	store.gate = make(chan struct{})
	opts := testOptions(dir)
	r, _ := startRunner(t, store, opts)

	var ids []string
	for range 6 {
		id, err := r.Submit(t.Context(), []string{"a.csv"})
		if err != nil {
			t.Fatalf("Submit: %v", err)
		}
		ids = append(ids, id)
	}
	for range 6 {
		store.gate <- struct{}{}
	}
	store.waitFinished(t, 6)

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.maxRunning != opts.MaxJobs {
		t.Fatalf("max jobs running at once = %d, want %d", store.maxRunning, opts.MaxJobs)
	}
	for i := 0; i+opts.MaxJobs <= len(ids); i += opts.MaxJobs {
		got, want := append([]string{}, store.order[i:i+opts.MaxJobs]...), ids[i:i+opts.MaxJobs]
		if strings.Join(sortedCopy(got), ",") != strings.Join(sortedCopy(want), ",") {
			t.Fatalf("start order %v, want jobs started in submission order %v", store.order, ids)
		}
	}
	for _, id := range ids {
		if store.status[id] != "completed" {
			t.Fatalf("job %s = %s, want completed", id, store.status[id])
		}
	}
}

func sortedCopy(s []string) []string {
	out := slices.Clone(s)
	slices.Sort(out)
	return out
}

func TestSubmitRejectsWhenQueueIsFull(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a.csv", header)
	store := newFakeStore()
	store.gate = make(chan struct{})
	opts := testOptions(dir)
	opts.MaxJobs, opts.QueueSize = 1, 2
	r, _ := startRunner(t, store, opts)

	if _, err := r.Submit(t.Context(), []string{"a.csv"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		store.mu.Lock()
		started := len(store.order)
		store.mu.Unlock()
		if started == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first job never started")
		}
		time.Sleep(time.Millisecond)
	}
	for range 2 {
		if _, err := r.Submit(t.Context(), []string{"a.csv"}); err != nil {
			t.Fatalf("Submit while the queue has room: %v", err)
		}
	}
	if _, err := r.Submit(t.Context(), []string{"a.csv"}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("Submit on a full queue = %v, want ErrQueueFull", err)
	}
	for range 3 {
		store.gate <- struct{}{}
	}
	store.waitFinished(t, 3)
}

func TestJobResultUsesRelativePaths(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "in/a.csv", header+"t1,o1,p1,2,500,2026-10-30T09:00:00Z\n")
	store := newFakeStore()
	r, _ := startRunner(t, store, testOptions(dir))
	id, err := r.Submit(t.Context(), []string{"in/a.csv", "missing.csv"})
	if err != nil {
		t.Fatal(err)
	}
	store.waitFinished(t, 1)
	status, _, doc := store.get(id)
	if status != "completed" || doc.Totals.Transactions != 1 {
		t.Fatalf("job = %s with %+v, want completed with 1 transaction", status, doc.Totals)
	}
	missing := doc.Files[1]
	if doc.Files[0].Path != filepath.Join("in", "a.csv") || missing.Path != "missing.csv" || missing.Status != "failed" {
		t.Fatalf("files = %+v", doc.Files)
	}
	if strings.Contains(missing.Error, dir) {
		t.Fatalf("file error %q exposes the data directory %q", missing.Error, dir)
	}
}

func TestSaveFailureMarksJobFailed(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a.csv", header)
	store := newFakeStore()
	store.saveErr = errors.New("database is down")
	r, _ := startRunner(t, store, testOptions(dir))
	id, _ := r.Submit(t.Context(), []string{"a.csv"})
	store.waitFinished(t, 1)
	if status, reason, _ := store.get(id); status != "failed" || reason != "could not save the result" {
		t.Fatalf("job = %s (%q), want failed with the save error", status, reason)
	}
}

func TestTooManyKeysFailsTheJob(t *testing.T) {
	dir := t.TempDir()
	content := []byte(header)
	for i := range 50 {
		content = fmt.Appendf(content, "t%d,o1,p%d,1,100,2026-10-30T09:00:00Z\n", i, i)
	}
	writeFile(t, dir, "many.csv", string(content))
	store := newFakeStore()
	opts := testOptions(dir)
	opts.Pipeline.MaxDailyKeys = 10
	r, _ := startRunner(t, store, opts)
	id, _ := r.Submit(t.Context(), []string{"many.csv"})
	store.waitFinished(t, 1)
	if status, reason, _ := store.get(id); status != "failed" || !strings.Contains(reason, "too many distinct daily keys") {
		t.Fatalf("job = %s (%q), want failed for too many keys", status, reason)
	}
}

func pipedRunner(t *testing.T, store Store, opts Options) (*Runner, *io.PipeWriter, context.CancelFunc) {
	t.Helper()
	writeFile(t, opts.DataDir, "live.csv", "")
	r, err := New(store, opts)
	if err != nil {
		t.Fatal(err)
	}
	pr, pw := io.Pipe()
	r.open = func(name string) (io.ReadCloser, error) {
		if name == "live.csv" {
			return pr, nil
		}
		return r.root.Open(name)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.Start(ctx)
	t.Cleanup(func() {
		cancel()
		_ = pw.Close()
		r.Wait(context.Background())
	})
	return r, pw, cancel
}

func feed(t *testing.T, pw *io.PipeWriter) {
	t.Helper()
	go func() { _, _ = io.WriteString(pw, header+"t1,o1,p1,1,100,2026-10-30T09:00:00Z\n") }()
}

func waitReading(t *testing.T, r *Runner, id string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for s := r.Progress(id); s == nil || s.Files[0].BytesRead == 0; s = r.Progress(id) {
		if time.Now().After(deadline) {
			t.Fatal("job never started reading")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestProgressWhileRunning(t *testing.T) {
	store := newFakeStore()
	r, pw, cancel := pipedRunner(t, store, testOptions(t.TempDir()))
	id, err := r.Submit(t.Context(), []string{"live.csv"})
	if err != nil {
		t.Fatal(err)
	}
	feed(t, pw)
	waitReading(t, r, id)
	if s := r.Progress(id); s.FilesTotal != 1 || s.Final {
		t.Fatalf("live snapshot = %+v", s)
	}
	cancel()
	store.waitFinished(t, 1)
	if r.Progress(id) != nil {
		t.Fatal("progress still reported after the job finished")
	}
}

func TestShutdownCancelsRunningAndFailsQueued(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a.csv", header)
	store := newFakeStore()
	opts := testOptions(dir)
	opts.MaxJobs = 1
	r, pw, cancel := pipedRunner(t, store, opts)

	running, _ := r.Submit(t.Context(), []string{"live.csv"})
	feed(t, pw)
	queued1, _ := r.Submit(t.Context(), []string{"a.csv"})
	queued2, _ := r.Submit(t.Context(), []string{"a.csv"})
	waitReading(t, r, running)

	cancel()
	done := make(chan struct{})
	go func() {
		r.Wait(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after shutdown")
	}

	if status, _, doc := store.get(running); status != "cancelled" || doc.Status != report.StatusCancelled || len(doc.Files) != 1 {
		t.Fatalf("running job = %s %+v, want saved as cancelled", status, doc)
	}
	for _, id := range []string{queued1, queued2} {
		if status, reason, _ := store.get(id); status != "failed" || !strings.Contains(reason, "shut down") {
			t.Fatalf("queued job %s = %s (%q), want failed because of the shutdown", id, status, reason)
		}
	}
}

func TestSubmitRejectsSpecialFiles(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses a named pipe")
	}
	dir := t.TempDir()
	if err := syscall.Mkfifo(filepath.Join(dir, "pipe.csv"), 0o600); err != nil {
		t.Fatal(err)
	}
	r, _ := startRunner(t, newFakeStore(), testOptions(dir))
	if _, err := r.Submit(t.Context(), []string{"pipe.csv"}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Submit(fifo) = %v, want ErrInvalidRequest: a read from it could block a worker forever", err)
	}
}

func TestNewRejectsBadOptions(t *testing.T) {
	dir := t.TempDir()
	for name, opts := range map[string]Options{
		"no workers":       {DataDir: dir, Pipeline: pipeline.DefaultConfig(), MaxJobs: 0, QueueSize: 1},
		"no queue":         {DataDir: dir, Pipeline: pipeline.DefaultConfig(), MaxJobs: 1, QueueSize: 0},
		"missing data dir": {DataDir: filepath.Join(dir, "nope"), Pipeline: pipeline.DefaultConfig(), MaxJobs: 1, QueueSize: 1},
		"bad pipeline":     {DataDir: dir, Pipeline: pipeline.Config{}, MaxJobs: 1, QueueSize: 1},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := New(newFakeStore(), opts); err == nil {
				t.Fatal("New() = nil error, want rejection")
			}
		})
	}
}

func TestFIFOCreatedAfterSubmitDoesNotHang(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses a named pipe")
	}
	dir := t.TempDir()
	store := newFakeStore()
	store.gate = make(chan struct{})
	r, cancel := startRunner(t, store, testOptions(dir))
	id, err := r.Submit(t.Context(), []string{"late.csv"})
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(dir, "late.csv"), 0o600); err != nil {
		t.Fatal(err)
	}
	store.gate <- struct{}{}
	store.waitFinished(t, 1)
	status, _, doc := store.get(id)
	if status != "completed" || doc.Files[0].Status != "failed" || !strings.Contains(doc.Files[0].Error, "not a regular file") {
		t.Fatalf("job = %s, file %+v; want the FIFO reported as a failed file", status, doc.Files[0])
	}
	cancel()
}

func TestSubmitAfterShutdownIsRefused(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a.csv", header)
	r, err := New(newFakeStore(), testOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.Start(ctx)
	cancel()
	r.Wait(context.Background())
	if _, err := r.Submit(t.Context(), []string{"a.csv"}); !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("Submit after shutdown = %v, want ErrShuttingDown", err)
	}
}

func TestStartFailureMarksJobFailed(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a.csv", header)
	store := newFakeStore()
	store.runErr = errors.New("connection reset by peer")
	r, _ := startRunner(t, store, testOptions(dir))
	id, _ := r.Submit(t.Context(), []string{"a.csv"})
	store.waitFinished(t, 1)
	if status, reason, _ := store.get(id); status != "failed" || reason != "could not start the job" {
		t.Fatalf("job = %s (%q), want failed with a generic reason", status, reason)
	}
}
