package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/umars28/file-processor/docs"
	"github.com/umars28/file-processor/internal/jobs"
	"github.com/umars28/file-processor/internal/pipeline"
	"github.com/umars28/file-processor/internal/report"
	"github.com/umars28/file-processor/internal/store"
)

const maxBodyBytes = 1 << 20

type JobQueue interface {
	Submit(ctx context.Context, files []string) (string, error)
	Progress(id string) *pipeline.Snapshot
}

type JobStore interface {
	Job(ctx context.Context, id string) (store.Job, error)
	Report(ctx context.Context, id string) ([]report.File, error)
	Results(ctx context.Context, id string) (report.Totals, []report.Daily, error)
}

type API struct {
	Jobs    JobQueue
	Store   JobStore
	DataDir string
	Logger  *slog.Logger
}

type route struct {
	method  string
	pattern string
	handler func(*API, http.ResponseWriter, *http.Request)
}

var routes = []route{
	{http.MethodGet, "/healthz", func(_ *API, w http.ResponseWriter, r *http.Request) { healthz(w, r) }},
	{http.MethodGet, "/openapi.yaml", func(_ *API, w http.ResponseWriter, r *http.Request) { openAPI(w, r) }},
	{http.MethodPost, "/jobs", (*API).createJob},
	{http.MethodGet, "/jobs/{id}", (*API).getJob},
	{http.MethodGet, "/jobs/{id}/report", (*API).getReport},
	{http.MethodGet, "/jobs/{id}/results", (*API).getResults},
}

func NewHandler(api *API) http.Handler {
	mux := http.NewServeMux()
	for _, r := range routes {
		if api == nil && strings.HasPrefix(r.pattern, "/jobs") {
			continue
		}
		h := r.handler
		mux.HandleFunc(r.method+" "+r.pattern, func(w http.ResponseWriter, req *http.Request) { h(api, w, req) })
	}
	return problemMux{mux}
}

type problemMux struct {
	mux *http.ServeMux
}

func (m problemMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if _, pattern := m.mux.Handler(r); pattern != "" {
		m.mux.ServeHTTP(w, r)
		return
	}
	if allow := m.allowedMethods(r); len(allow) > 0 {
		w.Header().Set("Allow", strings.Join(allow, ", "))
		writeProblem(w, http.StatusMethodNotAllowed, "Method not allowed", r.Method+" is not supported on "+r.URL.Path)
		return
	}
	writeProblem(w, http.StatusNotFound, "Not found", "")
}

func (m problemMux) allowedMethods(r *http.Request) []string {
	var allow []string
	for _, rt := range routes {
		probe := r.Clone(r.Context())
		probe.Method = rt.method
		if _, pattern := m.mux.Handler(probe); pattern == rt.method+" "+rt.pattern {
			allow = append(allow, rt.method)
		}
	}
	return allow
}

func healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"status":"ok"}`)
}

func openAPI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml")
	_, _ = w.Write(docs.OpenAPI)
}

type createJobRequest struct {
	Files []string `json:"files"`
}

type createJobResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type progressResponse struct {
	FilesDone  int      `json:"files_done"`
	FilesTotal int      `json:"files_total"`
	Percent    *float64 `json:"percent"`
	BytesRead  int64    `json:"bytes_read"`
	BytesTotal int64    `json:"bytes_total"`
	Valid      int64    `json:"valid"`
	Invalid    int64    `json:"invalid"`
	ElapsedMS  int64    `json:"elapsed_ms"`
}

type jobResponse struct {
	ID         string            `json:"id"`
	Status     string            `json:"status"`
	CreatedAt  time.Time         `json:"created_at"`
	StartedAt  *time.Time        `json:"started_at"`
	FinishedAt *time.Time        `json:"finished_at"`
	Error      string            `json:"error,omitempty"`
	Progress   *progressResponse `json:"progress,omitempty"`
}

type reportResponse struct {
	JobID  string        `json:"job_id"`
	Status string        `json:"status"`
	Files  []report.File `json:"files"`
}

type resultsResponse struct {
	JobID  string         `json:"job_id"`
	Status string         `json:"status"`
	Totals report.Totals  `json:"totals"`
	Daily  []report.Daily `json:"daily"`
}

type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail,omitempty"`
}

func (a *API) createJob(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	req, err := decodeCreateJob(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeProblem(w, http.StatusRequestEntityTooLarge, "Request body too large", "the body must be at most 1 MiB")
			return
		}
		writeProblem(w, http.StatusBadRequest, "Malformed request body", "expected a single JSON object with only a \"files\" array of strings")
		return
	}
	id, err := a.Jobs.Submit(r.Context(), req.Files)
	switch {
	case errors.Is(err, jobs.ErrInvalidRequest):
		writeProblem(w, http.StatusBadRequest, "Invalid job request", a.redact(err.Error()))
		return
	case errors.Is(err, jobs.ErrQueueFull):
		w.Header().Set("Retry-After", "5")
		writeProblem(w, http.StatusServiceUnavailable, "Job queue is full", "try again later")
		return
	case errors.Is(err, jobs.ErrShuttingDown):
		w.Header().Set("Retry-After", "30")
		writeProblem(w, http.StatusServiceUnavailable, "Server is shutting down", "try again later")
		return
	case err != nil:
		a.internalError(w, "submit job", err)
		return
	}
	w.Header().Set("Location", "/jobs/"+id)
	writeJSON(w, http.StatusAccepted, createJobResponse{ID: id, Status: string(store.JobQueued)})
}

func decodeCreateJob(body io.Reader) (createJobRequest, error) {
	dec := json.NewDecoder(body)
	var raw map[string]json.RawMessage
	if err := dec.Decode(&raw); err != nil {
		return createJobRequest{}, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return createJobRequest{}, err
		}
		return createJobRequest{}, errors.New("trailing data after the JSON object")
	}
	files, ok := raw["files"]
	if !ok || len(raw) != 1 {
		return createJobRequest{}, errors.New(`the object must have exactly one key, "files"`)
	}
	var req createJobRequest
	if err := json.Unmarshal(files, &req.Files); err != nil || req.Files == nil {
		return createJobRequest{}, errors.New(`"files" must be an array of strings`)
	}
	return req, nil
}

func (a *API) getJob(w http.ResponseWriter, r *http.Request) {
	job, ok := a.loadJob(w, r)
	if !ok {
		return
	}
	resp := jobResponse{
		ID:         job.ID,
		Status:     string(job.Status),
		CreatedAt:  job.CreatedAt,
		StartedAt:  job.StartedAt,
		FinishedAt: job.FinishedAt,
		Error:      a.redact(job.Error),
	}
	if s := a.Jobs.Progress(job.ID); s != nil {
		resp.Progress = progressFrom(s)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (a *API) getReport(w http.ResponseWriter, r *http.Request) {
	job, ok := a.loadFinishedJob(w, r)
	if !ok {
		return
	}
	files, err := a.Store.Report(r.Context(), job.ID)
	if err != nil {
		a.storeError(w, "load report", err)
		return
	}
	for i := range files {
		files[i].Path = a.redact(files[i].Path)
		files[i].Error = a.redact(files[i].Error)
		files[i].CloseError = a.redact(files[i].CloseError)
		for j := range files[i].Errors {
			files[i].Errors[j].Message = a.redact(files[i].Errors[j].Message)
		}
	}
	writeJSON(w, http.StatusOK, reportResponse{JobID: job.ID, Status: string(job.Status), Files: files})
}

func (a *API) getResults(w http.ResponseWriter, r *http.Request) {
	job, ok := a.loadFinishedJob(w, r)
	if !ok {
		return
	}
	totals, daily, err := a.Store.Results(r.Context(), job.ID)
	if err != nil {
		a.storeError(w, "load results", err)
		return
	}
	writeJSON(w, http.StatusOK, resultsResponse{JobID: job.ID, Status: string(job.Status), Totals: totals, Daily: daily})
}

func (a *API) loadJob(w http.ResponseWriter, r *http.Request) (store.Job, bool) {
	job, err := a.Store.Job(r.Context(), r.PathValue("id"))
	if err != nil {
		a.storeError(w, "load job", err)
		return store.Job{}, false
	}
	return job, true
}

func (a *API) loadFinishedJob(w http.ResponseWriter, r *http.Request) (store.Job, bool) {
	job, ok := a.loadJob(w, r)
	if !ok {
		return store.Job{}, false
	}
	if job.Status == store.JobQueued || job.Status == store.JobRunning {
		writeProblem(w, http.StatusConflict, "Job not finished", "the job is "+string(job.Status)+"; poll GET /jobs/"+job.ID+" until it has finished")
		return store.Job{}, false
	}
	return job, true
}

func (a *API) storeError(w http.ResponseWriter, op string, err error) {
	if errors.Is(err, store.ErrNotFound) {
		writeProblem(w, http.StatusNotFound, "Job not found", "")
		return
	}
	a.internalError(w, op, err)
}

func (a *API) internalError(w http.ResponseWriter, op string, err error) {
	if a.Logger != nil {
		a.Logger.Error(op, "err", err)
	}
	writeProblem(w, http.StatusInternalServerError, "Internal server error", "")
}

func (a *API) redact(s string) string {
	if s == "" || a.DataDir == "" {
		return s
	}
	abs, err := filepath.Abs(a.DataDir)
	if err != nil || abs == string(os.PathSeparator) {
		return s
	}
	s = strings.ReplaceAll(s, abs+string(os.PathSeparator), "")
	return strings.ReplaceAll(s, abs, ".")
}

func progressFrom(s *pipeline.Snapshot) *progressResponse {
	p := &progressResponse{
		FilesDone:  s.FilesDone,
		FilesTotal: s.FilesTotal,
		BytesRead:  s.BytesRead,
		BytesTotal: s.BytesTotal,
		Valid:      s.Valid,
		Invalid:    s.Invalid,
		ElapsedMS:  s.Elapsed.Milliseconds(),
	}
	if pct, ok := s.Percent(); ok {
		p.Percent = &pct
	}
	return p
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeProblem(w http.ResponseWriter, status int, title, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problem{Type: "about:blank", Title: title, Status: status, Detail: detail})
}
