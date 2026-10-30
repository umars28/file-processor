package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
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
		"run":     {"-readers", "-workers", "-batch-size", "-max-errors-per-file", "-max-record-bytes"},
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

const csvHeader = "transaction_id,outlet_id,product_id,quantity,amount,transacted_at\n"

func writeCSV(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func TestRunProcessesFiles(t *testing.T) {
	dir := t.TempDir()
	good := writeCSV(t, dir, "good.csv", csvHeader+"t1,o1,p1,2,5000,2026-10-30T09:00:00Z\n")
	messy := writeCSV(t, dir, "messy.csv", csvHeader+
		"t2,o1,p1,1,1000,2026-10-30T09:00:00Z\n"+
		"t3,o1,p1,x,1000,2026-10-30T09:00:00Z\n")

	code, _, stderr := run(t, t.Context(), env(nil), "run", good, messy)
	if code != exitOK {
		t.Fatalf("exit = %d, want %d; stderr: %s", code, exitOK, stderr)
	}
	for _, want := range []string{
		good + ": ok valid=1 invalid=0",
		messy + ": ok_with_errors valid=1 invalid=1",
		`first_error="line 3 column quantity: invalid number (value \"x\")"`,
	} {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr missing %q:\n%s", want, stderr)
		}
	}
}

func TestRunExitsOneOnFailedOrPartialFile(t *testing.T) {
	dir := t.TempDir()
	good := writeCSV(t, dir, "good.csv", csvHeader+"t1,o1,p1,2,5000,2026-10-30T09:00:00Z\n")
	badHeader := writeCSV(t, dir, "bad.csv", "nope\n")
	missing := filepath.Join(dir, "missing.csv")

	partial := writeCSV(t, dir, "partial.csv", csvHeader+"t1,o1,p1,2,5000,2026-10-30T09:00:00Z\n"+strings.Repeat("x", 100<<10)+"\n")

	for name, tc := range map[string]struct {
		paths  []string
		status string
	}{
		"missing path": {[]string{good, missing}, ": failed "},
		"bad header":   {[]string{good, badHeader}, ": failed "},
		"partial file": {[]string{good, partial}, ": partial "},
	} {
		t.Run(name, func(t *testing.T) {
			code, _, stderr := run(t, t.Context(), env(nil), append([]string{"run"}, tc.paths...)...)
			if code != exitError {
				t.Fatalf("exit = %d, want %d; stderr: %s", code, exitError, stderr)
			}
			if !strings.Contains(stderr, tc.status) || !strings.Contains(stderr, good+": ok ") {
				t.Fatalf("stderr = %s, want %q and the healthy file reported", stderr, tc.status)
			}
		})
	}
}

func TestRunExitsOneThirtyWhenCancelled(t *testing.T) {
	dir := t.TempDir()
	good := writeCSV(t, dir, "good.csv", csvHeader+"t1,o1,p1,2,5000,2026-10-30T09:00:00Z\n")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	code, _, stderr := run(t, ctx, env(nil), "run", good)
	if code != exitCancelled {
		t.Fatalf("exit = %d, want %d; stderr: %s", code, exitCancelled, stderr)
	}
	if !strings.Contains(stderr, ": cancelled ") {
		t.Fatalf("stderr = %s, want the file reported as cancelled", stderr)
	}
}

func TestRunHelpListsExitCodes(t *testing.T) {
	code, _, stderr := run(t, t.Context(), env(nil), "run", "-h")
	if code != exitOK {
		t.Fatalf("exit = %d, want %d", code, exitOK)
	}
	for _, want := range []string{"exit codes:", "  0 ", "  1 ", "  2 ", "  130 "} {
		if !strings.Contains(stderr, want) {
			t.Errorf("help missing %q:\n%s", want, stderr)
		}
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
