package cli

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func env(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

var withDB = env(map[string]string{"DATABASE_URL": "postgres://user@localhost/db"})

func run(t *testing.T, ctx context.Context, getenv func(string) string, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := Run(ctx, args, &stdout, &stderr, getenv)
	return code, stdout.String(), stderr.String()
}

func TestNoArgumentsPrintsUsage(t *testing.T) {
	code, _, stderr := run(t, t.Context(), env(nil))
	if code != exitUsage {
		t.Fatalf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr, "usage: fileproc") {
		t.Fatalf("stderr = %q, want usage", stderr)
	}
}

func TestUnknownSubcommand(t *testing.T) {
	code, _, stderr := run(t, t.Context(), env(nil), "bogus")
	if code != exitUsage {
		t.Fatalf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr, `unknown subcommand "bogus"`) || !strings.Contains(stderr, "usage: fileproc") {
		t.Fatalf("stderr = %q, want unknown subcommand and usage", stderr)
	}
}

func TestTopLevelHelp(t *testing.T) {
	for _, arg := range []string{"-h", "--help", "help"} {
		t.Run(arg, func(t *testing.T) {
			code, stdout, _ := run(t, t.Context(), env(nil), arg)
			if code != exitOK {
				t.Fatalf("exit = %d, want %d", code, exitOK)
			}
			if !strings.Contains(stdout, "usage: fileproc") {
				t.Fatalf("stdout = %q, want usage", stdout)
			}
		})
	}
}

func TestSubcommandHelpListsFlags(t *testing.T) {
	tests := map[string][]string{
		"run":     {"-readers", "-workers", "-batch-size", "-max-errors-per-file"},
		"serve":   {"-addr"},
		"migrate": {"usage: fileproc migrate"},
	}
	for sub, want := range tests {
		t.Run(sub, func(t *testing.T) {
			code, _, stderr := run(t, t.Context(), env(nil), sub, "-h")
			if code != exitOK {
				t.Fatalf("exit = %d, want %d", code, exitOK)
			}
			for _, w := range want {
				if !strings.Contains(stderr, w) {
					t.Errorf("stderr missing %q:\n%s", w, stderr)
				}
			}
		})
	}
}

func TestSubcommandBadFlag(t *testing.T) {
	for _, sub := range []string{"run", "serve", "migrate"} {
		t.Run(sub, func(t *testing.T) {
			code, _, _ := run(t, t.Context(), withDB, sub, "-no-such-flag")
			if code != exitUsage {
				t.Fatalf("exit = %d, want %d", code, exitUsage)
			}
		})
	}
}

func TestRunRequiresFiles(t *testing.T) {
	code, _, stderr := run(t, t.Context(), env(nil), "run")
	if code != exitUsage {
		t.Fatalf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr, "at least one CSV file") {
		t.Fatalf("stderr = %q", stderr)
	}
}

func TestRunRejectsInvalidConfig(t *testing.T) {
	code, _, stderr := run(t, t.Context(), env(nil), "run", "-workers=0", "a.csv")
	if code != exitUsage {
		t.Fatalf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr, "workers") {
		t.Fatalf("stderr = %q, want mention of workers", stderr)
	}
}

func TestRunNotImplementedYet(t *testing.T) {
	code, _, stderr := run(t, t.Context(), env(nil), "run", "a.csv")
	if code != exitError {
		t.Fatalf("exit = %d, want %d", code, exitError)
	}
	if !strings.Contains(stderr, "not implemented") {
		t.Fatalf("stderr = %q", stderr)
	}
}

func TestMissingDatabaseURL(t *testing.T) {
	for _, sub := range []string{"serve", "migrate"} {
		t.Run(sub, func(t *testing.T) {
			code, _, stderr := run(t, t.Context(), env(nil), sub)
			if code != exitError {
				t.Fatalf("exit = %d, want %d", code, exitError)
			}
			if !strings.Contains(stderr, "DATABASE_URL") {
				t.Fatalf("stderr = %q, want DATABASE_URL", stderr)
			}
		})
	}
}

func TestMigrateNotImplementedYet(t *testing.T) {
	code, _, stderr := run(t, t.Context(), withDB, "migrate")
	if code != exitError {
		t.Fatalf("exit = %d, want %d", code, exitError)
	}
	if !strings.Contains(stderr, "not implemented") {
		t.Fatalf("stderr = %q", stderr)
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 5s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestServeStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan int, 1)
	stderr := &syncBuffer{}
	go func() {
		done <- Run(ctx, []string{"serve", "-addr", "127.0.0.1:0"}, &bytes.Buffer{}, stderr, withDB)
	}()
	waitFor(t, func() bool { return strings.Contains(stderr.String(), "listening") })
	cancel()

	select {
	case code := <-done:
		if code != exitOK {
			t.Fatalf("exit = %d, want %d; stderr: %s", code, exitOK, stderr.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not stop after cancel")
	}
}

func TestServeInvalidAddress(t *testing.T) {
	code, _, stderr := run(t, t.Context(), withDB, "serve", "-addr", "not-an-address")
	if code != exitError {
		t.Fatalf("exit = %d, want %d", code, exitError)
	}
	if !strings.Contains(stderr, "serve:") {
		t.Fatalf("stderr = %q", stderr)
	}
}
