package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/umars28/file-processor/internal/pipeline"
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
		"run":     {"-readers", "-workers", "-batch-size", "-max-errors-per-file", "-max-record-bytes", "-progress"},
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

func TestRunProgressFlag(t *testing.T) {
	dir := t.TempDir()
	good := writeCSV(t, dir, "good.csv", csvHeader+"t1,o1,p1,2,5000,2026-10-30T09:00:00Z\n")
	other := writeCSV(t, dir, "other.csv", csvHeader+"t2,o1,p1,1,1000,2026-10-30T09:00:00Z\nt3,o1,p1,x,1,2026-10-30T09:00:00Z\n")

	code, _, stderr := run(t, t.Context(), env(nil), "run", "-progress", good, other)
	if code != exitOK {
		t.Fatalf("exit = %d, want %d; stderr: %s", code, exitOK, stderr)
	}
	var final []string
	for _, line := range strings.Split(stderr, "\n") {
		if strings.HasPrefix(line, "progress (final):") {
			final = append(final, line)
		}
	}
	if len(final) != 1 {
		t.Fatalf("got %d final progress lines, want 1:\n%s", len(final), stderr)
	}
	for _, want := range []string{"files 2/2", "100.0%", "valid 2", "invalid 1", "elapsed "} {
		if !strings.Contains(final[0], want) {
			t.Errorf("final progress line %q missing %q", final[0], want)
		}
	}

	_, _, quiet := run(t, t.Context(), env(nil), "run", good)
	if strings.Contains(quiet, "progress") {
		t.Fatalf("stderr without -progress = %q, want no progress lines", quiet)
	}
}

func progressLines(stderr string) []string {
	var lines []string
	for _, line := range strings.Split(stderr, "\n") {
		if strings.HasPrefix(line, "progress") {
			lines = append(lines, line)
		}
	}
	return lines
}

func TestRunPipelinePrintsFinalLineOnceAndLast(t *testing.T) {
	dir := t.TempDir()
	var buf []byte
	buf = append(buf, csvHeader...)
	for i := range 2000 {
		buf = fmt.Appendf(buf, "t%d,o1,p1,1,100,2026-10-30T09:00:00Z\n", i)
	}
	path := writeCSV(t, dir, "many.csv", string(buf))
	cfg := pipeline.DefaultConfig()
	cfg.BatchSize = 8
	for i := range 200 {
		var stderr bytes.Buffer
		if _, err := runPipeline(t.Context(), cfg, fileSources([]string{path}), 50*time.Microsecond, &stderr); err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
		lines := progressLines(stderr.String())
		finals := 0
		for _, l := range lines {
			if strings.HasPrefix(l, "progress (final):") {
				finals++
			}
		}
		if finals != 1 || !strings.HasPrefix(lines[len(lines)-1], "progress (final):") {
			t.Fatalf("run %d: %d final lines, last line %q, want exactly one final line at the end", i, finals, lines[len(lines)-1])
		}
	}
}

func TestRunProgressOnCancelledRun(t *testing.T) {
	dir := t.TempDir()
	good := writeCSV(t, dir, "good.csv", csvHeader+"t1,o1,p1,2,5000,2026-10-30T09:00:00Z\n")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	code, _, stderr := run(t, ctx, env(nil), "run", "-progress", good)
	if code != exitCancelled {
		t.Fatalf("exit = %d, want %d", code, exitCancelled)
	}
	lines := progressLines(stderr)
	if len(lines) == 0 || !strings.HasPrefix(lines[len(lines)-1], "progress (final): files 1/1") {
		t.Fatalf("progress lines = %q, want a final line counting the cancelled file", lines)
	}
}

type runJSON struct {
	Status string `json:"status"`
	Totals struct {
		Transactions int64 `json:"transactions"`
		Quantity     int64 `json:"quantity"`
		Amount       int64 `json:"amount"`
	} `json:"totals"`
	Daily []struct {
		Date         string `json:"date"`
		OutletID     string `json:"outlet_id"`
		ProductID    string `json:"product_id"`
		Transactions int64  `json:"transactions"`
		Quantity     int64  `json:"quantity"`
		Amount       int64  `json:"amount"`
	} `json:"daily"`
	Files []struct {
		Path        string           `json:"path"`
		Status      string           `json:"status"`
		Valid       int64            `json:"valid"`
		Invalid     int64            `json:"invalid"`
		ErrorCounts map[string]int64 `json:"error_counts"`
		Errors      []struct {
			Line    int    `json:"line"`
			Column  string `json:"column"`
			Kind    string `json:"kind"`
			Message string `json:"message"`
		} `json:"errors"`
		Error    string `json:"error"`
		LastLine int    `json:"last_line"`
	} `json:"files"`
}

func decodeRunJSON(t *testing.T, stdout string) runJSON {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(stdout))
	dec.DisallowUnknownFields()
	var doc runJSON
	if err := dec.Decode(&doc); err != nil {
		t.Fatalf("stdout is not the expected JSON: %v\n%s", err, stdout)
	}
	if dec.More() {
		t.Fatalf("stdout holds more than one JSON document:\n%s", stdout)
	}
	return doc
}

func TestRunWritesJSONResults(t *testing.T) {
	dir := t.TempDir()
	a := writeCSV(t, dir, "a.csv", csvHeader+
		"t1,o1,p1,2,5000,2026-10-30T09:00:00+07:00\n"+
		"t2,o1,p1,1,2500,2026-10-30T20:00:00+07:00\n"+
		"t3,o1,p1,x,1,2026-10-30T09:00:00+07:00\n")
	b := writeCSV(t, dir, "b.csv", csvHeader+"t4,o2,p9,3,9000,2026-10-31T01:00:00+07:00\n")

	code, stdout, stderr := run(t, t.Context(), env(nil), "run", a, b)
	if code != exitOK {
		t.Fatalf("exit = %d; stderr: %s", code, stderr)
	}
	if !strings.Contains(stderr, a+": ok_with_errors") {
		t.Fatalf("stderr summary changed: %s", stderr)
	}
	doc := decodeRunJSON(t, stdout)
	if doc.Status != "completed" || doc.Totals.Transactions != 3 || doc.Totals.Quantity != 6 || doc.Totals.Amount != 16500 {
		t.Fatalf("status/totals = %q %+v", doc.Status, doc.Totals)
	}
	if len(doc.Daily) != 2 || doc.Daily[0].OutletID != "o1" || doc.Daily[0].Transactions != 2 || doc.Daily[0].Amount != 7500 ||
		doc.Daily[1].Date != "2026-10-31" || doc.Daily[1].ProductID != "p9" {
		t.Fatalf("daily = %+v", doc.Daily)
	}
	if len(doc.Files) != 2 || doc.Files[0].Path != a || doc.Files[0].Status != "ok_with_errors" || doc.Files[0].ErrorCounts["invalid_number"] != 1 {
		t.Fatalf("files = %+v", doc.Files)
	}
	e := doc.Files[0].Errors
	if len(e) != 1 || e[0].Line != 4 || e[0].Column != "quantity" || e[0].Kind != "invalid_number" || !strings.Contains(e[0].Message, `"x"`) {
		t.Fatalf("errors = %+v", e)
	}
	if !strings.Contains(stdout, `"amount": 16500`) {
		t.Fatalf("amounts are not JSON integers:\n%s", stdout)
	}
}

func TestRunJSONOnFailureAndCancel(t *testing.T) {
	dir := t.TempDir()
	good := writeCSV(t, dir, "good.csv", csvHeader+"t1,o1,p1,2,5000,2026-10-30T09:00:00Z\n")
	missing := filepath.Join(dir, "missing.csv")

	code, stdout, _ := run(t, t.Context(), env(nil), "run", good, missing)
	if code != exitError {
		t.Fatalf("exit = %d, want %d", code, exitError)
	}
	doc := decodeRunJSON(t, stdout)
	if doc.Status != "completed" || doc.Files[1].Status != "failed" || doc.Files[1].Error == "" || len(doc.Daily) != 1 {
		t.Fatalf("failure document = %+v", doc)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	code, stdout, _ = run(t, ctx, env(nil), "run", good)
	if code != exitCancelled {
		t.Fatalf("exit = %d, want %d", code, exitCancelled)
	}
	doc = decodeRunJSON(t, stdout)
	if doc.Status != "cancelled" || doc.Daily == nil || len(doc.Daily) != 0 || doc.Files[0].Status != "cancelled" {
		t.Fatalf("cancelled document = %+v", doc)
	}
	if !strings.Contains(stdout, `"daily": []`) {
		t.Fatalf("cancelled daily is not an empty array:\n%s", stdout)
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
