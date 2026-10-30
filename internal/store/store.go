package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/umars28/file-processor/internal/report"
	"github.com/umars28/file-processor/migrations"
)

type JobStatus string

const (
	JobQueued    JobStatus = "queued"
	JobRunning   JobStatus = "running"
	JobCompleted JobStatus = "completed"
	JobCancelled JobStatus = "cancelled"
	JobFailed    JobStatus = "failed"
)

var (
	ErrNotFound   = errors.New("job not found")
	ErrWrongState = errors.New("job is not in the expected state")
	ErrOverflow   = errors.New("summed totals overflow int64")
)

type Job struct {
	ID         string
	Status     JobStatus
	CreatedAt  time.Time
	StartedAt  *time.Time
	FinishedAt *time.Time
	Error      string
}

type Store struct {
	pool *pgxpool.Pool
}

func Migrate(ctx context.Context, url string, log io.Writer) error {
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		return fmt.Errorf("parse database url: %w", err)
	}
	db := stdlib.OpenDB(*cfg)
	defer func() { _ = db.Close() }()
	return migrate(ctx, db, log)
}

func migrate(ctx context.Context, db *sql.DB, log io.Writer) error {
	provider, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS,
		goose.WithSlog(slog.New(slog.NewTextHandler(log, nil))))
	if err != nil {
		return fmt.Errorf("load migrations: %w", err)
	}
	results, err := provider.Up(ctx)
	if err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	logger := slog.New(slog.NewTextHandler(log, nil))
	for _, r := range results {
		logger.Info("applied migration", "file", r.Source.Path, "duration", r.Duration.Round(time.Millisecond))
	}
	if len(results) == 0 {
		logger.Info("schema is up to date")
	}
	return nil
}

func Open(ctx context.Context, url string) (*Store, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) CreateJob(ctx context.Context) (string, error) {
	var id string
	err := s.pool.QueryRow(ctx, `INSERT INTO jobs (status) VALUES ('queued') RETURNING id::text`).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("create job: %w", err)
	}
	return id, nil
}

func (s *Store) MarkRunning(ctx context.Context, id string) error {
	return s.transition(ctx, `
		UPDATE jobs SET status = 'running', started_at = now()
		WHERE id = $1 AND status = 'queued'`, id)
}

func (s *Store) MarkFailed(ctx context.Context, id, reason string) error {
	return s.transition(ctx, `
		UPDATE jobs SET status = 'failed', finished_at = now(), error = $2
		WHERE id = $1 AND status IN ('queued', 'running')`, id, reason)
}

func (s *Store) transition(ctx context.Context, query, id string, args ...any) error {
	if !validUUID(id) {
		return ErrNotFound
	}
	tag, err := s.pool.Exec(ctx, query, append([]any{id}, args...)...)
	if err != nil {
		return fmt.Errorf("update job %s: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		if _, err := s.Job(ctx, id); err != nil {
			return err
		}
		return fmt.Errorf("update job %s: %w", id, ErrWrongState)
	}
	return nil
}

func (s *Store) FailUnfinishedJobs(ctx context.Context, reason string) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs SET status = 'failed', finished_at = now(), error = $1
		WHERE status IN ('queued', 'running')`, reason)
	if err != nil {
		return 0, fmt.Errorf("fail unfinished jobs: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (s *Store) SaveResult(ctx context.Context, id string, doc report.Document) error {
	if !validUUID(id) {
		return ErrNotFound
	}
	status := JobCompleted
	if doc.Status == report.StatusCancelled {
		status = JobCancelled
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, `
		UPDATE jobs SET status = $2, finished_at = now(), error = NULL
		WHERE id = $1 AND status = 'running'`, id, status)
	if err != nil {
		return fmt.Errorf("finish job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("save job %s: %w", id, ErrWrongState)
	}
	fileIDs, err := insertFiles(ctx, tx, id, doc.Files)
	if err != nil {
		return err
	}
	if err := copyRowErrors(ctx, tx, fileIDs, doc.Files); err != nil {
		return err
	}
	if status == JobCompleted {
		if err := copyDaily(ctx, tx, id, doc.Daily); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func insertFiles(ctx context.Context, tx pgx.Tx, jobID string, files []report.File) ([]int64, error) {
	batch := &pgx.Batch{}
	for i, f := range files {
		batch.Queue(`
			INSERT INTO job_files (job_id, position, path, status, valid_rows, invalid_rows, last_line, error, close_error, error_counts)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			RETURNING id`,
			jobID, i, f.Path, f.Status, f.Valid, f.Invalid, f.LastLine, nullable(f.Error), nullable(f.CloseError), errorCounts(f.ErrorCounts))
	}
	results := tx.SendBatch(ctx, batch)
	ids := make([]int64, len(files))
	for i := range files {
		if err := results.QueryRow().Scan(&ids[i]); err != nil {
			_ = results.Close()
			return nil, fmt.Errorf("insert file %q: %w", files[i].Path, err)
		}
	}
	if err := results.Close(); err != nil {
		return nil, fmt.Errorf("insert files: %w", err)
	}
	return ids, nil
}

func copyRowErrors(ctx context.Context, tx pgx.Tx, fileIDs []int64, files []report.File) error {
	var rows [][]any
	for i, f := range files {
		for _, e := range f.Errors {
			rows = append(rows, []any{fileIDs[i], e.Line, nullable(e.Column), e.Kind, e.Message})
		}
	}
	if len(rows) == 0 {
		return nil
	}
	_, err := tx.CopyFrom(ctx, pgx.Identifier{"row_errors"},
		[]string{"job_file_id", "line", "column_name", "kind", "message"}, pgx.CopyFromRows(rows))
	if err != nil {
		return fmt.Errorf("copy row errors: %w", err)
	}
	return nil
}

func copyDaily(ctx context.Context, tx pgx.Tx, jobID string, daily []report.Daily) error {
	if len(daily) == 0 {
		return nil
	}
	i := 0
	var convErr error
	src := pgx.CopyFromFunc(func() ([]any, error) {
		if i == len(daily) {
			return nil, nil
		}
		d := daily[i]
		i++
		date, err := time.Parse(time.DateOnly, d.Date)
		if err != nil {
			convErr = fmt.Errorf("daily row %d: %w", i, err)
			return nil, convErr
		}
		return []any{jobID, date, d.OutletID, d.ProductID, d.Transactions, d.Quantity, d.Amount}, nil
	})
	_, err := tx.CopyFrom(ctx, pgx.Identifier{"sales_daily"},
		[]string{"job_id", "sale_date", "outlet_id", "product_id", "transactions", "quantity", "amount"}, src)
	if err != nil {
		return fmt.Errorf("copy daily sales: %w", errors.Join(convErr, err))
	}
	return nil
}

func (s *Store) Job(ctx context.Context, id string) (Job, error) {
	if !validUUID(id) {
		return Job{}, ErrNotFound
	}
	var j Job
	var jobErr *string
	err := s.pool.QueryRow(ctx, `
		SELECT id::text, status, created_at, started_at, finished_at, error
		FROM jobs WHERE id = $1`, id).Scan(&j.ID, &j.Status, &j.CreatedAt, &j.StartedAt, &j.FinishedAt, &jobErr)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("load job %s: %w", id, err)
	}
	if jobErr != nil {
		j.Error = *jobErr
	}
	return j, nil
}

func (s *Store) Report(ctx context.Context, id string) ([]report.File, error) {
	if !validUUID(id) {
		return nil, ErrNotFound
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("begin report: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM jobs WHERE id = $1)`, id).Scan(&exists); err != nil {
		return nil, fmt.Errorf("load job %s: %w", id, err)
	}
	if !exists {
		return nil, ErrNotFound
	}
	files, index, err := loadFiles(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	if err := loadRowErrors(ctx, tx, id, files, index); err != nil {
		return nil, err
	}
	return files, nil
}

func loadFiles(ctx context.Context, tx pgx.Tx, id string) ([]report.File, map[int64]int, error) {
	rows, err := tx.Query(ctx, `
		SELECT f.id, f.path, f.status, f.valid_rows, f.invalid_rows, f.last_line,
		       coalesce(f.error, ''), coalesce(f.close_error, ''), f.error_counts
		FROM job_files f WHERE f.job_id = $1 ORDER BY f.position`, id)
	if err != nil {
		return nil, nil, fmt.Errorf("load files: %w", err)
	}
	defer rows.Close()
	files := []report.File{}
	index := map[int64]int{}
	for rows.Next() {
		var fileID int64
		f := report.File{Errors: []report.RowError{}}
		if err := rows.Scan(&fileID, &f.Path, &f.Status, &f.Valid, &f.Invalid, &f.LastLine, &f.Error, &f.CloseError, &f.ErrorCounts); err != nil {
			return nil, nil, fmt.Errorf("scan file: %w", err)
		}
		index[fileID] = len(files)
		files = append(files, f)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("load files: %w", err)
	}
	return files, index, nil
}

func loadRowErrors(ctx context.Context, tx pgx.Tx, id string, files []report.File, index map[int64]int) error {
	rows, err := tx.Query(ctx, `
		SELECT r.job_file_id, r.line, coalesce(r.column_name, ''), r.kind, r.message
		FROM row_errors r JOIN job_files f ON f.id = r.job_file_id
		WHERE f.job_id = $1 ORDER BY f.position, r.line`, id)
	if err != nil {
		return fmt.Errorf("load row errors: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var fileID int64
		var e report.RowError
		if err := rows.Scan(&fileID, &e.Line, &e.Column, &e.Kind, &e.Message); err != nil {
			return fmt.Errorf("scan row error: %w", err)
		}
		i, ok := index[fileID]
		if !ok {
			return fmt.Errorf("row error for unknown file %d in job %s", fileID, id)
		}
		files[i].Errors = append(files[i].Errors, e)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("load row errors: %w", err)
	}
	return nil
}

func (s *Store) Results(ctx context.Context, id string) (report.Totals, []report.Daily, error) {
	if _, err := s.Job(ctx, id); err != nil {
		return report.Totals{}, nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT to_char(sale_date, 'YYYY-MM-DD'), outlet_id, product_id, transactions, quantity, amount
		FROM sales_daily WHERE job_id = $1
		ORDER BY sale_date, outlet_id, product_id`, id)
	if err != nil {
		return report.Totals{}, nil, fmt.Errorf("load daily sales: %w", err)
	}
	defer rows.Close()
	var totals report.Totals
	daily := []report.Daily{}
	for rows.Next() {
		var d report.Daily
		if err := rows.Scan(&d.Date, &d.OutletID, &d.ProductID, &d.Transactions, &d.Quantity, &d.Amount); err != nil {
			return report.Totals{}, nil, fmt.Errorf("scan daily sales: %w", err)
		}
		var ok1, ok2, ok3 bool
		totals.Transactions, ok1 = addInt64(totals.Transactions, d.Transactions)
		totals.Quantity, ok2 = addInt64(totals.Quantity, d.Quantity)
		totals.Amount, ok3 = addInt64(totals.Amount, d.Amount)
		if !ok1 || !ok2 || !ok3 {
			return report.Totals{}, nil, fmt.Errorf("results for job %s: %w", id, ErrOverflow)
		}
		daily = append(daily, d)
	}
	if err := rows.Err(); err != nil {
		return report.Totals{}, nil, fmt.Errorf("load daily sales: %w", err)
	}
	return totals, daily, nil
}

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func validUUID(id string) bool {
	if len(id) != 36 {
		return false
	}
	for i, c := range id {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !isHex(c) {
				return false
			}
		}
	}
	return true
}

func isHex(c rune) bool {
	return '0' <= c && c <= '9' || 'a' <= c && c <= 'f' || 'A' <= c && c <= 'F'
}

func errorCounts(m map[string]int64) map[string]int64 {
	if m == nil {
		return map[string]int64{}
	}
	return m
}

func addInt64(a, b int64) (int64, bool) {
	sum := a + b
	if (b > 0 && sum < a) || (b < 0 && sum > a) {
		return 0, false
	}
	return sum, true
}
