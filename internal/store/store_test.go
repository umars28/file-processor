package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/umars28/file-processor/internal/report"
	"go.uber.org/goleak"
)

var testURL = os.Getenv("TEST_DATABASE_URL")

func TestMain(m *testing.M) {
	if testURL != "" {
		var log bytes.Buffer
		if err := Migrate(context.Background(), testURL, &log); err != nil {
			fmt.Fprintf(os.Stderr, "migrate test database: %v\n%s", err, log.String())
			os.Exit(1)
		}
	}
	goleak.VerifyTestMain(m)
}

func openStore(t *testing.T) *Store {
	t.Helper()
	if testURL == "" {
		t.Skip("TEST_DATABASE_URL is not set; run `make test-integration` with the Compose database up")
	}
	s, err := Open(t.Context(), testURL)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

func runningJob(t *testing.T, s *Store) string {
	t.Helper()
	id, err := s.CreateJob(t.Context())
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := s.MarkRunning(t.Context(), id); err != nil {
		t.Fatalf("MarkRunning: %v", err)
	}
	return id
}

func sampleDocument() report.Document {
	return report.Document{
		Status: report.StatusCompleted,
		Totals: report.Totals{Transactions: 4, Quantity: 9, Amount: math.MaxInt64 - 10},
		Daily: []report.Daily{
			{Date: "2026-10-30", OutletID: "o1", ProductID: "p1", Transactions: 3, Quantity: 7, Amount: math.MaxInt64 - 20},
			{Date: "2026-10-31", OutletID: "o2", ProductID: "p9", Transactions: 1, Quantity: 2, Amount: 10},
		},
		Files: []report.File{
			{
				Path: "a.csv", Status: "ok_with_errors", Valid: 3, Invalid: 2, LastLine: 6,
				ErrorCounts: map[string]int64{"invalid_number": 1, "csv_syntax": 1},
				Errors: []report.RowError{
					{Line: 3, Column: "quantity", Kind: "invalid_number", Message: `invalid number (value "x")`},
					{Line: 5, Kind: "csv_syntax", Message: "bare quote"},
				},
			},
			{
				Path: "b.csv", Status: "partial", Valid: 1, Invalid: 0, LastLine: 2,
				ErrorCounts: map[string]int64{}, Errors: []report.RowError{},
				Error: "disk on fire", CloseError: "close: flush failed",
			},
			{
				Path: "c.csv", Status: "failed", ErrorCounts: map[string]int64{}, Errors: []report.RowError{},
				Error: "open c.csv: no such file or directory",
			},
		},
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	openStore(t)
	var log bytes.Buffer
	if err := Migrate(t.Context(), testURL, &log); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
}

func TestSchemaHasConstraints(t *testing.T) {
	s := openStore(t)
	rows, err := s.pool.Query(t.Context(), `
		SELECT conrelid::regclass::text, contype::text
		FROM pg_constraint
		WHERE conrelid::regclass::text IN ('jobs', 'job_files', 'row_errors', 'sales_daily')`)
	if err != nil {
		t.Fatalf("query constraints: %v", err)
	}
	defer rows.Close()
	got := map[string]map[string]int{}
	for rows.Next() {
		var table, kind string
		if err := rows.Scan(&table, &kind); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if got[table] == nil {
			got[table] = map[string]int{}
		}
		got[table][kind]++
	}
	want := map[string][]string{
		"jobs":        {"p", "c"},
		"job_files":   {"p", "f", "u", "c"},
		"row_errors":  {"p", "f"},
		"sales_daily": {"p", "f", "c"},
	}
	for table, kinds := range want {
		for _, k := range kinds {
			if got[table][k] == 0 {
				t.Errorf("table %s has no constraint of type %q (got %v)", table, k, got[table])
			}
		}
	}
	if got["job_files"]["u"] < 2 {
		t.Errorf("job_files unique constraints = %d, want (job_id, position) and (job_id, path)", got["job_files"]["u"])
	}
}

func TestSaveResultRoundTrip(t *testing.T) {
	s := openStore(t)
	id := runningJob(t, s)
	doc := sampleDocument()
	if err := s.SaveResult(t.Context(), id, doc); err != nil {
		t.Fatalf("SaveResult: %v", err)
	}

	job, err := s.Job(t.Context(), id)
	if err != nil {
		t.Fatalf("Job: %v", err)
	}
	if job.Status != JobCompleted || job.StartedAt == nil || job.FinishedAt == nil || job.Error != "" {
		t.Fatalf("job = %+v, want completed with start and finish times", job)
	}
	files, err := s.Report(t.Context(), id)
	if err != nil {
		t.Fatalf("Report: %v", err)
	}
	if !reflect.DeepEqual(files, doc.Files) {
		t.Fatalf("Report() =\n%+v\nwant\n%+v", files, doc.Files)
	}
	totals, daily, err := s.Results(t.Context(), id)
	if err != nil {
		t.Fatalf("Results: %v", err)
	}
	if totals != doc.Totals || !reflect.DeepEqual(daily, doc.Daily) {
		t.Fatalf("Results() = %+v %+v, want %+v %+v", totals, daily, doc.Totals, doc.Daily)
	}
}

func countJobRows(t *testing.T, s *Store, id string) (files, errs, daily int) {
	t.Helper()
	if err := s.pool.QueryRow(t.Context(), `
		SELECT
			(SELECT count(*) FROM job_files WHERE job_id = $1),
			(SELECT count(*) FROM row_errors r JOIN job_files f ON f.id = r.job_file_id WHERE f.job_id = $1),
			(SELECT count(*) FROM sales_daily WHERE job_id = $1)`, id).Scan(&files, &errs, &daily); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return files, errs, daily
}

func TestSaveResultIsAtomic(t *testing.T) {
	tests := map[string]func(*report.Document){
		"job_files insert": func(d *report.Document) { d.Files[2].Path = d.Files[0].Path },
		"row_errors copy":  func(d *report.Document) { d.Files[0].Errors[1].Line = d.Files[0].Errors[0].Line },
		"sales_daily copy": func(d *report.Document) { d.Daily[1] = d.Daily[0] },
	}
	s := openStore(t)
	for name, breakIt := range tests {
		t.Run(name, func(t *testing.T) {
			id := runningJob(t, s)
			doc := sampleDocument()
			breakIt(&doc)
			if err := s.SaveResult(t.Context(), id, doc); err == nil {
				t.Fatal("SaveResult() = nil, want an error")
			}
			job, err := s.Job(t.Context(), id)
			if err != nil {
				t.Fatalf("Job: %v", err)
			}
			if job.Status != JobRunning {
				t.Fatalf("job status = %q after a failed save, want running", job.Status)
			}
			if files, errs, daily := countJobRows(t, s, id); files != 0 || errs != 0 || daily != 0 {
				t.Fatalf("after a failed save: %d files, %d errors, %d daily rows; want none", files, errs, daily)
			}
		})
	}
}

func TestSaveResultAcceptsNilErrorCounts(t *testing.T) {
	s := openStore(t)
	id := runningJob(t, s)
	doc := sampleDocument()
	doc.Files[1].ErrorCounts = nil
	if err := s.SaveResult(t.Context(), id, doc); err != nil {
		t.Fatalf("SaveResult() with nil error counts: %v", err)
	}
	files, _ := s.Report(t.Context(), id)
	if files[1].ErrorCounts == nil || len(files[1].ErrorCounts) != 0 {
		t.Fatalf("error counts = %#v, want an empty map", files[1].ErrorCounts)
	}
}

func TestResultsDetectsOverflow(t *testing.T) {
	s := openStore(t)
	id := runningJob(t, s)
	doc := sampleDocument()
	doc.Daily[0].Amount = math.MaxInt64
	doc.Daily[1].Amount = 1
	if err := s.SaveResult(t.Context(), id, doc); err != nil {
		t.Fatalf("SaveResult: %v", err)
	}
	if _, _, err := s.Results(t.Context(), id); !errors.Is(err, ErrOverflow) {
		t.Fatalf("Results() error = %v, want ErrOverflow", err)
	}
}

func TestReportDuringSaveSeesBeforeOrAfter(t *testing.T) {
	s := openStore(t)
	for range 20 {
		id := runningJob(t, s)
		doc := sampleDocument()
		stop := make(chan struct{})
		failures := make(chan string, 1)
		go func() {
			defer close(failures)
			for {
				select {
				case <-stop:
					return
				default:
				}
				files, err := s.Report(context.Background(), id)
				if err != nil {
					failures <- err.Error()
					return
				}
				if len(files) != 0 && (len(files) != 3 || len(files[0].Errors) != 2) {
					failures <- fmt.Sprintf("half-saved report: %+v", files)
					return
				}
			}
		}()
		err := s.SaveResult(t.Context(), id, doc)
		close(stop)
		if msg, ok := <-failures; ok {
			t.Fatalf("Report while saving: %s", msg)
		}
		if err != nil {
			t.Fatalf("SaveResult: %v", err)
		}
	}
}

func TestSaveResultRejectsJobNotRunning(t *testing.T) {
	s := openStore(t)
	id, err := s.CreateJob(t.Context())
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := s.SaveResult(t.Context(), id, sampleDocument()); !errors.Is(err, ErrWrongState) {
		t.Fatalf("SaveResult() on a queued job = %v, want ErrWrongState", err)
	}
}

func TestSaveCancelledResult(t *testing.T) {
	s := openStore(t)
	id := runningJob(t, s)
	doc := sampleDocument()
	doc.Status = report.StatusCancelled
	doc.Totals = report.Totals{}
	doc.Daily = []report.Daily{}
	doc.Files[2].Status = "cancelled"
	doc.Files[2].Error = ""
	if err := s.SaveResult(t.Context(), id, doc); err != nil {
		t.Fatalf("SaveResult: %v", err)
	}
	job, _ := s.Job(t.Context(), id)
	if job.Status != JobCancelled {
		t.Fatalf("job status = %q, want cancelled", job.Status)
	}
	files, _ := s.Report(t.Context(), id)
	if len(files) != 3 || len(files[0].Errors) != 2 {
		t.Fatalf("cancelled report = %+v, want all files and their errors", files)
	}
	totals, daily, _ := s.Results(t.Context(), id)
	if totals != (report.Totals{}) || len(daily) != 0 {
		t.Fatalf("cancelled results = %+v %+v, want none", totals, daily)
	}
}

func TestFailUnfinishedJobs(t *testing.T) {
	s := openStore(t)
	queued, _ := s.CreateJob(t.Context())
	running := runningJob(t, s)
	done := runningJob(t, s)
	if err := s.SaveResult(t.Context(), done, sampleDocument()); err != nil {
		t.Fatalf("SaveResult: %v", err)
	}

	n, err := s.FailUnfinishedJobs(t.Context(), "server restarted before the job finished")
	if err != nil {
		t.Fatalf("FailUnfinishedJobs: %v", err)
	}
	if n < 2 {
		t.Fatalf("changed %d jobs, want at least the 2 created here", n)
	}
	for _, id := range []string{queued, running} {
		job, _ := s.Job(t.Context(), id)
		if job.Status != JobFailed || job.Error != "server restarted before the job finished" || job.FinishedAt == nil {
			t.Fatalf("unfinished job = %+v, want failed with the reason", job)
		}
	}
	if job, _ := s.Job(t.Context(), done); job.Status != JobCompleted {
		t.Fatalf("finished job = %+v, want it untouched", job)
	}
}

func TestMarkFailed(t *testing.T) {
	s := openStore(t)
	id := runningJob(t, s)
	if err := s.MarkFailed(t.Context(), id, "save failed"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	if job, _ := s.Job(t.Context(), id); job.Status != JobFailed || job.Error != "save failed" {
		t.Fatalf("job = %+v, want failed", job)
	}
}

func TestUnknownJob(t *testing.T) {
	s := openStore(t)
	const missing = "00000000-0000-0000-0000-000000000000"
	if _, err := s.Job(t.Context(), missing); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Job(missing) = %v, want ErrNotFound", err)
	}
	if err := s.MarkRunning(t.Context(), missing); !errors.Is(err, ErrNotFound) {
		t.Fatalf("MarkRunning(missing) = %v, want ErrNotFound", err)
	}
	if _, err := s.Job(t.Context(), "not-a-uuid"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Job(not-a-uuid) = %v, want ErrNotFound", err)
	}
}

func TestSaveResultScale(t *testing.T) {
	s := openStore(t)
	id := runningJob(t, s)
	doc := report.Document{Status: report.StatusCompleted, Daily: make([]report.Daily, 0, 25_000)}
	for i := range 25_000 {
		doc.Daily = append(doc.Daily, report.Daily{
			Date: "2026-10-30", OutletID: fmt.Sprintf("o%d", i%50), ProductID: fmt.Sprintf("p%d", i/50),
			Transactions: 1, Quantity: 2, Amount: 2500,
		})
		doc.Totals.Transactions++
		doc.Totals.Quantity += 2
		doc.Totals.Amount += 2500
	}
	for f := range 100 {
		file := report.File{Path: fmt.Sprintf("f%03d.csv", f), Status: "ok_with_errors", Invalid: 100, LastLine: 201, ErrorCounts: map[string]int64{"invalid_number": 100}}
		for e := range 100 {
			file.Errors = append(file.Errors, report.RowError{Line: e*2 + 2, Column: "quantity", Kind: "invalid_number", Message: "invalid number"})
		}
		doc.Files = append(doc.Files, file)
	}
	start := time.Now()
	if err := s.SaveResult(t.Context(), id, doc); err != nil {
		t.Fatalf("SaveResult: %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("SaveResult took %v, want under 2s", d)
	} else {
		t.Logf("saved 25,000 daily rows and 10,000 row errors in %v", d)
	}
	totals, daily, err := s.Results(t.Context(), id)
	if err != nil || totals != doc.Totals || len(daily) != 25_000 {
		t.Fatalf("Results() = %+v, %d rows, %v", totals, len(daily), err)
	}
}
