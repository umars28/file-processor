package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/umars28/file-processor/docs"
	"github.com/umars28/file-processor/internal/jobs"
	"github.com/umars28/file-processor/internal/pipeline"
	"github.com/umars28/file-processor/internal/report"
	"github.com/umars28/file-processor/internal/store"
)

const dataDir = "/srv/fileproc/data"

type fakeQueue struct {
	err      error
	files    []string
	progress map[string]*pipeline.Snapshot
}

func (q *fakeQueue) Submit(_ context.Context, files []string) (string, error) {
	q.files = files
	if q.err != nil {
		return "", q.err
	}
	return "11111111-1111-1111-1111-111111111111", nil
}

func (q *fakeQueue) Progress(id string) *pipeline.Snapshot { return q.progress[id] }

type fakeJobStore struct {
	jobs  map[string]store.Job
	files []report.File
	err   error
}

func (s *fakeJobStore) Job(_ context.Context, id string) (store.Job, error) {
	if s.err != nil {
		return store.Job{}, s.err
	}
	j, ok := s.jobs[id]
	if !ok {
		return store.Job{}, store.ErrNotFound
	}
	return j, nil
}

func (s *fakeJobStore) Report(context.Context, string) ([]report.File, error) {
	return append([]report.File{}, s.files...), nil
}

func (s *fakeJobStore) Results(context.Context, string) (report.Totals, []report.Daily, error) {
	return report.Totals{Transactions: 2, Quantity: 3, Amount: 7500},
		[]report.Daily{{Date: "2026-10-30", OutletID: "o1", ProductID: "p1", Transactions: 2, Quantity: 3, Amount: 7500}}, nil
}

func newTestAPI() (*API, *fakeQueue, *fakeJobStore) {
	now := time.Date(2026, 10, 30, 9, 0, 0, 0, time.UTC)
	q := &fakeQueue{progress: map[string]*pipeline.Snapshot{}}
	s := &fakeJobStore{jobs: map[string]store.Job{
		"queued":  {ID: "queued", Status: store.JobQueued, CreatedAt: now},
		"running": {ID: "running", Status: store.JobRunning, CreatedAt: now, StartedAt: &now},
		"done":    {ID: "done", Status: store.JobCompleted, CreatedAt: now, StartedAt: &now, FinishedAt: &now},
		"failed":  {ID: "failed", Status: store.JobFailed, CreatedAt: now, FinishedAt: &now, Error: "open " + dataDir + "/x.csv: denied"},
	}}
	return &API{Jobs: q, Store: s, DataDir: dataDir}, q, s
}

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func assertProblem(t *testing.T, rec *httptest.ResponseRecorder, status int) problem {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d; body %s", rec.Code, status, rec.Body)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("Content-Type = %q, want application/problem+json", ct)
	}
	var p problem
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil || p.Status != status || p.Title == "" {
		t.Fatalf("problem body = %s (%v)", rec.Body, err)
	}
	return p
}

func TestCreateJobAccepted(t *testing.T) {
	api, q, _ := newTestAPI()
	rec := do(t, NewHandler(api), http.MethodPost, "/jobs", `{"files": ["a.csv", "in/b.csv"]}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}
	if loc := rec.Header().Get("Location"); loc != "/jobs/11111111-1111-1111-1111-111111111111" {
		t.Fatalf("Location = %q", loc)
	}
	var resp createJobResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || resp.Status != "queued" || resp.ID == "" {
		t.Fatalf("body = %s (%v)", rec.Body, err)
	}
	if strings.Join(q.files, ",") != "a.csv,in/b.csv" {
		t.Fatalf("submitted %v", q.files)
	}
}

func TestCreateJobRejectsBadRequests(t *testing.T) {
	tests := map[string]string{
		"not json":         `files=a.csv`,
		"unknown field":    `{"files": ["a.csv"], "priority": 1}`,
		"wrong type":       `{"files": "a.csv"}`,
		"two objects":      `{"files": ["a.csv"]} {"files": ["b.csv"]}`,
		"trailing bracket": `{"files": ["a.csv"]} ]`,
		"wrong key case":   `{"FILES": ["a.csv"]}`,
		"null files":       `{"files": null}`,
		"array body":       `["a.csv"]`,
		"empty body":       ``,
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			api, _, _ := newTestAPI()
			assertProblem(t, do(t, NewHandler(api), http.MethodPost, "/jobs", body), http.StatusBadRequest)
		})
	}
}

func TestCreateJobInvalidFilesIsRedacted(t *testing.T) {
	api, q, _ := newTestAPI()
	q.err = fmt.Errorf("%w: %q must be a relative path inside the data directory", jobs.ErrInvalidRequest, filepath.Join(dataDir, "a.csv"))
	rec := do(t, NewHandler(api), http.MethodPost, "/jobs", `{"files": ["/srv/fileproc/data/a.csv"]}`)
	p := assertProblem(t, rec, http.StatusBadRequest)
	if strings.Contains(rec.Body.String(), dataDir) || !strings.Contains(p.Detail, "a.csv") {
		t.Fatalf("detail = %q, want the data directory redacted", p.Detail)
	}
}

func TestCreateJobBodyTooLarge(t *testing.T) {
	api, _, _ := newTestAPI()
	body := `{"files": ["` + strings.Repeat("a", maxBodyBytes) + `"]}`
	assertProblem(t, do(t, NewHandler(api), http.MethodPost, "/jobs", body), http.StatusRequestEntityTooLarge)
}

func TestCreateJobTrailingWhitespaceOverLimit(t *testing.T) {
	api, _, _ := newTestAPI()
	body := `{"files": ["a.csv"]}` + strings.Repeat(" ", maxBodyBytes)
	assertProblem(t, do(t, NewHandler(api), http.MethodPost, "/jobs", body), http.StatusRequestEntityTooLarge)
}

func TestCreateJobWhileShuttingDown(t *testing.T) {
	api, q, _ := newTestAPI()
	q.err = jobs.ErrShuttingDown
	rec := do(t, NewHandler(api), http.MethodPost, "/jobs", `{"files": ["a.csv"]}`)
	assertProblem(t, rec, http.StatusServiceUnavailable)
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("503 without Retry-After")
	}
}

func TestCreateJobQueueFull(t *testing.T) {
	api, q, _ := newTestAPI()
	q.err = jobs.ErrQueueFull
	rec := do(t, NewHandler(api), http.MethodPost, "/jobs", `{"files": ["a.csv"]}`)
	assertProblem(t, rec, http.StatusServiceUnavailable)
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("503 without Retry-After")
	}
}

func TestGetJob(t *testing.T) {
	api, q, _ := newTestAPI()
	q.progress["running"] = &pipeline.Snapshot{FilesDone: 1, FilesTotal: 2, BytesRead: 50, BytesTotal: 200, Valid: 10, Invalid: 1, Elapsed: 1500 * time.Millisecond}
	h := NewHandler(api)

	rec := do(t, h, http.MethodGet, "/jobs/running", "")
	var got jobResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s (%v)", rec.Code, rec.Body, err)
	}
	p := got.Progress
	if got.Status != "running" || p == nil || p.FilesDone != 1 || p.FilesTotal != 2 || p.Percent == nil || *p.Percent != 25 || p.Valid != 10 || p.ElapsedMS != 1500 {
		t.Fatalf("job = %+v progress %+v", got, p)
	}

	rec = do(t, h, http.MethodGet, "/jobs/done", "")
	if strings.Contains(rec.Body.String(), `"progress"`) {
		t.Fatalf("finished job carries progress: %s", rec.Body)
	}
	rec = do(t, h, http.MethodGet, "/jobs/failed", "")
	if strings.Contains(rec.Body.String(), dataDir) || !strings.Contains(rec.Body.String(), "x.csv") {
		t.Fatalf("failed job error not redacted: %s", rec.Body)
	}
	assertProblem(t, do(t, h, http.MethodGet, "/jobs/nope", ""), http.StatusNotFound)
}

func TestReportAndResultsWaitForTheJob(t *testing.T) {
	api, _, _ := newTestAPI()
	h := NewHandler(api)
	for _, id := range []string{"queued", "running"} {
		for _, suffix := range []string{"/report", "/results"} {
			assertProblem(t, do(t, h, http.MethodGet, "/jobs/"+id+suffix, ""), http.StatusConflict)
		}
	}
	for _, suffix := range []string{"/report", "/results"} {
		assertProblem(t, do(t, h, http.MethodGet, "/jobs/nope"+suffix, ""), http.StatusNotFound)
	}
}

func TestReportRedactsDataDirectory(t *testing.T) {
	api, _, s := newTestAPI()
	s.files = []report.File{{
		Path:        filepath.Join(dataDir, "in", "a.csv"),
		Status:      "failed",
		ErrorCounts: map[string]int64{},
		Errors:      []report.RowError{{Line: 2, Kind: "other", Message: "read " + dataDir + "/in/a.csv: io"}},
		Error:       "open " + dataDir + "/in/a.csv: permission denied",
		CloseError:  "close " + dataDir + ": bad",
	}}
	rec := do(t, NewHandler(api), http.MethodGet, "/jobs/done/report", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), dataDir) {
		t.Fatalf("report exposes the data directory: %s", rec.Body)
	}
	var got reportResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || got.Files[0].Path != filepath.Join("in", "a.csv") || got.JobID != "done" {
		t.Fatalf("report = %+v (%v)", got, err)
	}
}

func TestResults(t *testing.T) {
	api, _, _ := newTestAPI()
	rec := do(t, NewHandler(api), http.MethodGet, "/jobs/done/results", "")
	var got resultsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}
	if got.Totals.Amount != 7500 || len(got.Daily) != 1 || got.Status != "completed" {
		t.Fatalf("results = %+v", got)
	}
}

func TestStoreFailureIsAGenericServerError(t *testing.T) {
	api, _, s := newTestAPI()
	s.err = errors.New("dial tcp 10.0.0.5:5432: connection refused")
	rec := do(t, NewHandler(api), http.MethodGet, "/jobs/done", "")
	assertProblem(t, rec, http.StatusInternalServerError)
	if strings.Contains(rec.Body.String(), "10.0.0.5") {
		t.Fatalf("500 leaks internal detail: %s", rec.Body)
	}
}

func TestUnknownRoutesAndMethodsAreProblems(t *testing.T) {
	api, _, _ := newTestAPI()
	h := NewHandler(api)
	assertProblem(t, do(t, h, http.MethodGet, "/nope", ""), http.StatusNotFound)
	assertProblem(t, do(t, h, http.MethodGet, "/jobs/done/nope", ""), http.StatusNotFound)
	for path, allow := range map[string]string{
		"/jobs":             "POST",
		"/jobs/done":        "GET",
		"/jobs/done/report": "GET",
		"/healthz":          "GET",
	} {
		rec := do(t, h, http.MethodDelete, path, "")
		assertProblem(t, rec, http.StatusMethodNotAllowed)
		if got := rec.Header().Get("Allow"); got != allow {
			t.Errorf("DELETE %s: Allow = %q, want %q", path, got, allow)
		}
	}
	if rec := do(t, h, http.MethodHead, "/healthz", ""); rec.Code != http.StatusOK {
		t.Fatalf("HEAD /healthz = %d, want 200 like GET", rec.Code)
	}
}

func TestOpenAPIServedAndCoversEveryRoute(t *testing.T) {
	api, _, _ := newTestAPI()
	rec := do(t, NewHandler(api), http.MethodGet, "/openapi.yaml", "")
	if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), docs.OpenAPI) {
		t.Fatalf("GET /openapi.yaml = %d, %d bytes", rec.Code, rec.Body.Len())
	}
	spec := string(docs.OpenAPI)
	for _, r := range routes {
		block := specPathBlock(spec, r.pattern)
		if block == "" {
			t.Errorf("route %s %s is missing from docs/openapi.yaml", r.method, r.pattern)
			continue
		}
		if !strings.Contains(block, "\n    "+strings.ToLower(r.method)+":") {
			t.Errorf("route %s %s: path is documented but the method is not", r.method, r.pattern)
		}
	}
}

func TestOpenAPIDocumentsOnlyServedPaths(t *testing.T) {
	served := map[string]bool{}
	for _, r := range routes {
		served[r.pattern] = true
	}
	inPaths := false
	for _, line := range strings.Split(string(docs.OpenAPI), "\n") {
		switch {
		case line == "paths:":
			inPaths = true
		case inPaths && line != "" && !strings.HasPrefix(line, " "):
			inPaths = false
		case inPaths && strings.HasPrefix(line, "  /") && !strings.HasPrefix(line, "   "):
			path := strings.TrimSuffix(strings.TrimSpace(line), ":")
			if !served[path] {
				t.Errorf("docs/openapi.yaml documents %s, which no route serves", path)
			}
		}
	}
}

func TestOpenAPIDocumentsServerErrors(t *testing.T) {
	for _, r := range routes {
		if !strings.HasPrefix(r.pattern, "/jobs") {
			continue
		}
		if !strings.Contains(specPathBlock(string(docs.OpenAPI), r.pattern), `"500":`) {
			t.Errorf("%s %s does not document a 500 response", r.method, r.pattern)
		}
	}
}

func specPathBlock(spec, pattern string) string {
	start := strings.Index(spec, "\n  "+pattern+":\n")
	if start < 0 {
		return ""
	}
	rest := spec[start+1:]
	lines := strings.SplitAfter(rest, "\n")
	var b strings.Builder
	b.WriteString(lines[0])
	for _, line := range lines[1:] {
		if strings.HasPrefix(line, "  ") && !strings.HasPrefix(line, "   ") {
			break
		}
		if line != "" && !strings.HasPrefix(line, " ") {
			break
		}
		b.WriteString(line)
	}
	return "\n" + b.String()
}
