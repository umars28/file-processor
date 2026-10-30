package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/umars28/file-processor/internal/config"
	"github.com/umars28/file-processor/internal/httpapi"
	"github.com/umars28/file-processor/internal/jobs"
	"github.com/umars28/file-processor/internal/pipeline"
	"github.com/umars28/file-processor/internal/report"
	"github.com/umars28/file-processor/internal/store"
)

const (
	exitOK        = 0
	exitError     = 1
	exitUsage     = 2
	exitCancelled = 130
)

const usage = `usage: fileproc <subcommand> [flags]

subcommands:
  run      process CSV files and print results
  serve    start the HTTP API
  migrate  apply database migrations

run "fileproc <subcommand> -h" for subcommand flags`

func Run(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, usage)
		return exitUsage
	}
	switch args[0] {
	case "run":
		return runCmd(ctx, args[1:], stdout, stderr)
	case "serve":
		return serveCmd(ctx, args[1:], stderr, getenv)
	case "migrate":
		return migrateCmd(ctx, args[1:], stderr, getenv)
	case "-h", "--help", "help":
		fmt.Fprintln(stdout, usage)
		return exitOK
	default:
		fmt.Fprintf(stderr, "fileproc: unknown subcommand %q\n\n%s\n", args[0], usage)
		return exitUsage
	}
}

func newFlagSet(name, synopsis string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintf(stderr, "usage: fileproc %s %s\n", name, synopsis)
		fs.PrintDefaults()
	}
	return fs
}

func parseFlags(fs *flag.FlagSet, args []string) (code int, ok bool) {
	err := fs.Parse(args)
	switch {
	case errors.Is(err, flag.ErrHelp):
		return exitOK, false
	case err != nil:
		return exitUsage, false
	default:
		return exitOK, true
	}
}

const runExitCodes = `
exit codes:
  0    every file is ok or ok_with_errors
  1    at least one file is partial or failed
  2    invalid usage or flags
  130  the run was cancelled`

func runCmd(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	cfg := pipeline.DefaultConfig()
	fs := newFlagSet("run", "[flags] <file.csv>...", stderr)
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: fileproc run [flags] <file.csv>...")
		fs.PrintDefaults()
		fmt.Fprintln(stderr, runExitCodes)
	}
	fs.IntVar(&cfg.Readers, "readers", cfg.Readers, "maximum number of files read at the same time")
	fs.IntVar(&cfg.Workers, "workers", cfg.Workers, "number of workers converting and validating rows")
	fs.IntVar(&cfg.BatchSize, "batch-size", cfg.BatchSize, "rows per batch sent between pipeline stages")
	fs.IntVar(&cfg.MaxErrorsPerFile, "max-errors-per-file", cfg.MaxErrorsPerFile, "row errors kept per file in the report")
	fs.IntVar(&cfg.MaxRecordBytes, "max-record-bytes", cfg.MaxRecordBytes, "approximate largest CSV record, in bytes, before the file fails")
	fs.IntVar(&cfg.MaxDailyKeys, "max-daily-keys", cfg.MaxDailyKeys, "distinct (date, outlet, product) keys allowed before the run fails; 0 means no limit")
	showProgress := fs.Bool("progress", false, "print progress lines to stderr while files are processed")
	if code, ok := parseFlags(fs, args); !ok {
		return code
	}
	paths := fs.Args()
	if len(paths) == 0 {
		fmt.Fprintln(stderr, "run: at least one CSV file is required")
		fs.Usage()
		return exitUsage
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(stderr, "run: %v\n", err)
		return exitUsage
	}

	var interval time.Duration
	if *showProgress {
		interval = pipeline.DefaultProgressInterval
	}
	res, err := runPipeline(ctx, cfg, fileSources(paths), interval, stderr)
	for _, f := range res.Files {
		printFileSummary(stderr, paths[f.ID], f)
	}
	cancelled := errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
	if err == nil || cancelled {
		doc := report.Build(res, cancelled, func(id pipeline.FileID) string { return paths[id] })
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if encErr := enc.Encode(doc); encErr != nil {
			fmt.Fprintf(stderr, "run: write results: %v\n", encErr)
			if cancelled {
				return exitCancelled
			}
			return exitError
		}
	}
	switch {
	case cancelled:
		fmt.Fprintln(stderr, "run: cancelled")
		return exitCancelled
	case err != nil:
		fmt.Fprintf(stderr, "run: %v\n", err)
		return exitError
	}
	for _, f := range res.Files {
		if f.Status == pipeline.StatusPartial || f.Status == pipeline.StatusFailed {
			return exitError
		}
	}
	return exitOK
}

func runPipeline(ctx context.Context, cfg pipeline.Config, sources []pipeline.Source, interval time.Duration, stderr io.Writer) (pipeline.Result, error) {
	if interval <= 0 {
		return pipeline.Run(ctx, cfg, sources)
	}
	p := pipeline.NewProgress(interval)
	stop := make(chan struct{})
	var printer sync.WaitGroup
	printer.Go(func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		var last *pipeline.Snapshot
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if s := p.Snapshot(); s != nil && s != last && !s.Final {
					printProgress(stderr, s)
					last = s
				}
			}
		}
	})
	res, err := pipeline.RunWithProgress(ctx, cfg, sources, p)
	close(stop)
	printer.Wait()
	if s := p.Snapshot(); s != nil {
		printProgress(stderr, s)
	}
	return res, err
}

func printProgress(w io.Writer, s *pipeline.Snapshot) {
	label := "progress:"
	if s.Final {
		label = "progress (final):"
	}
	pct := "?"
	if v, ok := s.Percent(); ok {
		pct = fmt.Sprintf("%.1f%%", v)
	}
	fmt.Fprintf(w, "%s files %d/%d, %s, valid %d, invalid %d, elapsed %s\n",
		label, s.FilesDone, s.FilesTotal, pct, s.Valid, s.Invalid, s.Elapsed.Round(time.Millisecond))
}

func fileSources(paths []string) []pipeline.Source {
	sources := make([]pipeline.Source, len(paths))
	for i, p := range paths {
		path := filepath.Clean(p)
		var size int64
		if info, err := os.Stat(path); err == nil {
			size = info.Size()
		}
		sources[i] = pipeline.Source{
			ID:   pipeline.FileID(i),
			Size: size,
			Open: func(context.Context) (io.ReadCloser, error) { return os.Open(path) },
		}
	}
	return sources
}

func printFileSummary(w io.Writer, path string, f pipeline.FileResult) {
	fmt.Fprintf(w, "%s: %s valid=%d invalid=%d", path, f.Status, f.Valid, f.Invalid)
	if f.Err != nil {
		fmt.Fprintf(w, " last_line=%d error=%q", f.LastLine, report.Describe(f.Err))
	}
	if f.CloseErr != nil {
		fmt.Fprintf(w, " close_error=%q", report.Describe(f.CloseErr))
	}
	if len(f.Errors) > 0 {
		e := f.Errors[0]
		where := fmt.Sprintf("line %d", e.Line)
		if e.Column != "" {
			where += " column " + e.Column
		}
		fmt.Fprintf(w, " first_error=%q", where+": "+report.Describe(e.Err))
	}
	fmt.Fprintln(w)
}

func serveCmd(ctx context.Context, args []string, stderr io.Writer, getenv func(string) string) int {
	pcfg := pipeline.DefaultConfig()
	pcfg.MaxDailyKeys = 1_000_000
	fs := newFlagSet("serve", "[flags]", stderr)
	addr := fs.String("addr", "", "listen address (default $HTTP_ADDR or :8080)")
	maxJobs := fs.Int("max-jobs", 2, "jobs processed at the same time")
	queueSize := fs.Int("queue-size", 100, "jobs waiting to start before new submissions get 503")
	fs.IntVar(&pcfg.MaxDailyKeys, "max-daily-keys", pcfg.MaxDailyKeys, "distinct (date, outlet, product) keys a job may produce before it fails; 0 means no limit")
	if code, ok := parseFlags(fs, args); !ok {
		return code
	}
	cfg, err := config.Load(getenv)
	if err != nil {
		fmt.Fprintf(stderr, "serve: %v\n", err)
		return exitError
	}
	if *addr != "" {
		cfg.HTTPAddr = *addr
	}
	logger := slog.New(slog.NewTextHandler(stderr, nil))

	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.HTTPAddr)
	if err != nil {
		fmt.Fprintf(stderr, "serve: %v\n", err)
		return exitError
	}
	st, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		_ = ln.Close()
		fmt.Fprintf(stderr, "serve: %v\n", err)
		return exitError
	}
	defer st.Close()
	if n, err := st.FailUnfinishedJobs(ctx, "server restarted before the job finished"); err != nil {
		_ = ln.Close()
		fmt.Fprintf(stderr, "serve: %v\n", err)
		return exitError
	} else if n > 0 {
		logger.Warn("marked unfinished jobs as failed", "jobs", n)
	}
	runner, err := jobs.New(st, jobs.Options{
		DataDir:          cfg.DataDir,
		Pipeline:         pcfg,
		MaxJobs:          *maxJobs,
		QueueSize:        *queueSize,
		ProgressInterval: pipeline.DefaultProgressInterval,
		Logger:           logger,
	})
	if err != nil {
		_ = ln.Close()
		fmt.Fprintf(stderr, "serve: %v\n", err)
		return exitError
	}
	runCtx, stopRunner := context.WithCancel(ctx)
	defer stopRunner()
	runner.Start(runCtx)

	logger.Info("listening", "addr", ln.Addr().String(), "data_dir", cfg.DataDir)
	handler := httpapi.NewHandler(&httpapi.API{Jobs: runner, Store: st, DataDir: cfg.DataDir, Logger: logger})
	serveErr := httpapi.Serve(ctx, ln, handler)
	stopRunner()
	runner.Wait(ctx)
	if serveErr != nil {
		fmt.Fprintf(stderr, "serve: %v\n", serveErr)
		return exitError
	}
	logger.Info("stopped")
	return exitOK
}

func migrateCmd(ctx context.Context, args []string, stderr io.Writer, getenv func(string) string) int {
	fs := newFlagSet("migrate", "[flags]", stderr)
	if code, ok := parseFlags(fs, args); !ok {
		return code
	}
	cfg, err := config.Load(getenv)
	if err != nil {
		fmt.Fprintf(stderr, "migrate: %v\n", err)
		return exitError
	}
	if err := store.Migrate(ctx, cfg.DatabaseURL, stderr); err != nil {
		fmt.Fprintf(stderr, "migrate: %v\n", err)
		return exitError
	}
	return exitOK
}
