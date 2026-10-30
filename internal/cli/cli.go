package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"

	"github.com/umars28/file-processor/internal/config"
	"github.com/umars28/file-processor/internal/httpapi"
	"github.com/umars28/file-processor/internal/pipeline"
)

const (
	exitOK    = 0
	exitError = 1
	exitUsage = 2
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
		return runCmd(args[1:], stderr)
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

func runCmd(args []string, stderr io.Writer) int {
	cfg := pipeline.DefaultConfig()
	fs := newFlagSet("run", "[flags] <file.csv>...", stderr)
	fs.IntVar(&cfg.Readers, "readers", cfg.Readers, "maximum number of files read at the same time")
	fs.IntVar(&cfg.Workers, "workers", cfg.Workers, "number of workers converting and validating rows")
	fs.IntVar(&cfg.BatchSize, "batch-size", cfg.BatchSize, "rows per batch sent between pipeline stages")
	fs.IntVar(&cfg.MaxErrorsPerFile, "max-errors-per-file", cfg.MaxErrorsPerFile, "row errors kept per file in the report")
	if code, ok := parseFlags(fs, args); !ok {
		return code
	}
	if fs.NArg() == 0 {
		fmt.Fprintln(stderr, "run: at least one CSV file is required")
		fs.Usage()
		return exitUsage
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(stderr, "run: %v\n", err)
		return exitUsage
	}
	fmt.Fprintf(stderr, "run: %v\n", errNotImplemented)
	return exitError
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
