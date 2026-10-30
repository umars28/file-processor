package jobs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/umars28/file-processor/internal/pipeline"
	"github.com/umars28/file-processor/internal/report"
)

const MaxFiles = 100

var (
	ErrInvalidRequest = errors.New("invalid job request")
	ErrQueueFull      = errors.New("job queue is full")
	ErrShuttingDown   = errors.New("server is shutting down")
	errNotRegular     = errors.New("not a regular file")
)

type Store interface {
	CreateJob(ctx context.Context) (string, error)
	MarkRunning(ctx context.Context, id string) error
	MarkFailed(ctx context.Context, id, reason string) error
	SaveResult(ctx context.Context, id string, doc report.Document) error
}

type Options struct {
	DataDir          string
	Pipeline         pipeline.Config
	MaxJobs          int
	QueueSize        int
	ProgressInterval time.Duration
	SaveTimeout      time.Duration
	Logger           *slog.Logger
}

type queuedJob struct {
	id    string
	files []string
}

type Runner struct {
	store Store
	root  *os.Root
	open  func(name string) (io.ReadCloser, error)
	opts  Options
	queue chan queuedJob
	slots chan struct{}
	wg    sync.WaitGroup

	mu   sync.Mutex
	live map[string]*pipeline.Progress

	gate   sync.RWMutex
	closed bool
}

func New(store Store, opts Options) (*Runner, error) {
	if opts.MaxJobs < 1 {
		return nil, fmt.Errorf("max jobs must be at least 1, got %d", opts.MaxJobs)
	}
	if opts.QueueSize < 1 {
		return nil, fmt.Errorf("queue size must be at least 1, got %d", opts.QueueSize)
	}
	if err := opts.Pipeline.Validate(); err != nil {
		return nil, err
	}
	if opts.SaveTimeout <= 0 {
		opts.SaveTimeout = 10 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	root, err := os.OpenRoot(opts.DataDir)
	if err != nil {
		return nil, fmt.Errorf("open data directory: %w", err)
	}
	r := &Runner{
		store: store,
		root:  root,
		opts:  opts,
		queue: make(chan queuedJob, opts.QueueSize),
		slots: make(chan struct{}, opts.QueueSize),
		live:  map[string]*pipeline.Progress{},
	}
	r.open = r.openRegular
	return r, nil
}

func (r *Runner) Start(ctx context.Context) {
	for range r.opts.MaxJobs {
		r.wg.Go(func() { r.work(ctx) })
	}
}

func (r *Runner) Wait(ctx context.Context) {
	r.wg.Wait()
	r.gate.Lock()
	r.closed = true
	r.gate.Unlock()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.opts.SaveTimeout)
	defer cancel()
	for {
		select {
		case j := <-r.queue:
			<-r.slots
			r.fail(ctx, j.id, "server shut down before the job started", nil)
		default:
			_ = r.root.Close()
			return
		}
	}
}

func (r *Runner) Submit(ctx context.Context, files []string) (string, error) {
	r.gate.RLock()
	defer r.gate.RUnlock()
	if r.closed {
		return "", ErrShuttingDown
	}
	names, err := r.validate(files)
	if err != nil {
		return "", err
	}
	select {
	case r.slots <- struct{}{}:
	default:
		return "", ErrQueueFull
	}
	id, err := r.store.CreateJob(ctx)
	if err != nil {
		<-r.slots
		return "", err
	}
	r.queue <- queuedJob{id: id, files: names}
	return id, nil
}

func (r *Runner) Progress(id string) *pipeline.Snapshot {
	r.mu.Lock()
	p := r.live[id]
	r.mu.Unlock()
	if p == nil {
		return nil
	}
	return p.Snapshot()
}

func (r *Runner) validate(files []string) ([]string, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("%w: files must not be empty", ErrInvalidRequest)
	}
	if len(files) > MaxFiles {
		return nil, fmt.Errorf("%w: at most %d files per job, got %d", ErrInvalidRequest, MaxFiles, len(files))
	}
	names := make([]string, 0, len(files))
	for _, f := range files {
		name, err := r.checkPath(f)
		if err != nil {
			return nil, err
		}
		if slices.Contains(names, name) {
			return nil, fmt.Errorf("%w: duplicate file %q", ErrInvalidRequest, f)
		}
		names = append(names, name)
	}
	return names, nil
}

func (r *Runner) checkPath(f string) (string, error) {
	if f == "" || filepath.IsAbs(f) || !filepath.IsLocal(f) || slices.Contains(strings.Split(filepath.ToSlash(f), "/"), "..") {
		return "", fmt.Errorf("%w: %q must be a relative path inside the data directory", ErrInvalidRequest, f)
	}
	name := filepath.Clean(f)
	info, err := r.root.Stat(name)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return name, nil
	case err != nil:
		return "", fmt.Errorf("%w: %q is not inside the data directory", ErrInvalidRequest, f)
	case !info.Mode().IsRegular():
		return "", fmt.Errorf("%w: %q is not a regular file", ErrInvalidRequest, f)
	}
	return name, nil
}

func (r *Runner) work(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case j := <-r.queue:
			<-r.slots
			r.run(ctx, j)
		}
	}
}

func (r *Runner) run(ctx context.Context, j queuedJob) {
	storeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.opts.SaveTimeout)
	defer cancel()
	if err := r.store.MarkRunning(storeCtx, j.id); err != nil {
		r.fail(storeCtx, j.id, "could not start the job", err)
		return
	}

	p := pipeline.NewProgress(r.opts.ProgressInterval)
	r.mu.Lock()
	r.live[j.id] = p
	r.mu.Unlock()
	res, err := pipeline.RunWithProgress(ctx, r.opts.Pipeline, r.sources(j.files), p)
	r.mu.Lock()
	delete(r.live, j.id)
	r.mu.Unlock()

	cancelled := errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
	saveCtx, cancelSave := context.WithTimeout(context.WithoutCancel(ctx), r.opts.SaveTimeout)
	defer cancelSave()
	if err != nil && !cancelled {
		r.fail(saveCtx, j.id, publicReason(err), err)
		return
	}
	doc := report.Build(res, cancelled, func(id pipeline.FileID) string { return j.files[id] })
	if err := r.store.SaveResult(saveCtx, j.id, doc); err != nil {
		r.fail(saveCtx, j.id, "could not save the result", err)
		return
	}
	r.opts.Logger.Info("job finished", "job", j.id, "status", doc.Status)
}

func (r *Runner) fail(ctx context.Context, id, reason string, cause error) {
	if err := r.store.MarkFailed(ctx, id, reason); err != nil {
		r.opts.Logger.Error("mark job failed", "job", id, "err", err)
		return
	}
	r.opts.Logger.Warn("job failed", "job", id, "reason", reason, "cause", cause)
}

func publicReason(err error) string {
	if errors.Is(err, pipeline.ErrTooManyKeys) {
		return err.Error()
	}
	return "processing failed"
}

func (r *Runner) openRegular(name string) (io.ReadCloser, error) {
	f, err := r.root.OpenFile(name, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() {
		_ = f.Close()
		return nil, &fs.PathError{Op: "open", Path: name, Err: errNotRegular}
	}
	return f, nil
}

func (r *Runner) sources(files []string) []pipeline.Source {
	sources := make([]pipeline.Source, len(files))
	for i, name := range files {
		var size int64
		if info, err := r.root.Stat(name); err == nil {
			size = info.Size()
		}
		sources[i] = pipeline.Source{
			ID:   pipeline.FileID(i),
			Size: size,
			Open: func(context.Context) (io.ReadCloser, error) { return r.open(name) },
		}
	}
	return sources
}
