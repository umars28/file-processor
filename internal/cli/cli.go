package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"

	"github.com/umars28/file-processor/internal/config"
	"github.com/umars28/file-processor/internal/httpapi"
	"github.com/umars28/file-processor/internal/pipeline"
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

var errNotImplemented = errors.New("not implemented yet")

func Run(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, usage)
		return exitUsage
	}
	switch args[0] {
	case "run":
		return runCmd(ctx, args[1:], stderr)
	case "serve":
		return serveCmd(ctx, args[1:], stderr, getenv)
	case "migrate":
		return migrateCmd(args[1:], stderr, getenv)
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

func runCmd(ctx context.Context, args []string, stderr io.Writer) int {
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

	res, err := pipeline.Run(ctx, cfg, fileSources(paths))
	for _, f := range res.Files {
		printFileSummary(stderr, paths[f.ID], f)
	}
	switch {
	case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
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

func fileSources(paths []string) []pipeline.Source {
	sources := make([]pipeline.Source, len(paths))
	for i, p := range paths {
		path := filepath.Clean(p)
		sources[i] = pipeline.Source{
			ID:   pipeline.FileID(i),
			Open: func(context.Context) (io.ReadCloser, error) { return os.Open(path) },
		}
	}
	return sources
}

func printFileSummary(w io.Writer, path string, f pipeline.FileResult) {
	fmt.Fprintf(w, "%s: %s valid=%d invalid=%d", path, f.Status, f.Valid, f.Invalid)
	if f.Err != nil {
		fmt.Fprintf(w, " last_line=%d error=%q", f.LastLine, describe(f.Err))
	}
	if f.CloseErr != nil {
		fmt.Fprintf(w, " close_error=%q", describe(f.CloseErr))
	}
	if len(f.Errors) > 0 {
		e := f.Errors[0]
		where := fmt.Sprintf("line %d", e.Line)
		if e.Column != "" {
			where += " column " + e.Column
		}
		fmt.Fprintf(w, " first_error=%q", where+": "+describe(e.Err))
	}
	fmt.Fprintln(w)
}

func describe(err error) string {
	var fileErr *pipeline.FileError
	if errors.As(err, &fileErr) {
		err = fileErr.Err
	}
	var fieldErr *pipeline.FieldError
	if errors.As(err, &fieldErr) {
		return fmt.Sprintf("%v (value %q)", fieldErr.Err, fieldErr.Value)
	}
	return err.Error()
}

func serveCmd(ctx context.Context, args []string, stderr io.Writer, getenv func(string) string) int {
	fs := newFlagSet("serve", "[flags]", stderr)
	addr := fs.String("addr", "", "listen address (default $HTTP_ADDR or :8080)")
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
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.HTTPAddr)
	if err != nil {
		fmt.Fprintf(stderr, "serve: %v\n", err)
		return exitError
	}
	logger := slog.New(slog.NewTextHandler(stderr, nil))
	logger.Info("listening", "addr", ln.Addr().String())
	if err := httpapi.Serve(ctx, ln, httpapi.NewHandler()); err != nil {
		fmt.Fprintf(stderr, "serve: %v\n", err)
		return exitError
	}
	logger.Info("stopped")
	return exitOK
}

func migrateCmd(args []string, stderr io.Writer, getenv func(string) string) int {
	fs := newFlagSet("migrate", "[flags]", stderr)
	if code, ok := parseFlags(fs, args); !ok {
		return code
	}
	if _, err := config.Load(getenv); err != nil {
		fmt.Fprintf(stderr, "migrate: %v\n", err)
		return exitError
	}
	fmt.Fprintf(stderr, "migrate: %v\n", errNotImplemented)
	return exitError
}
