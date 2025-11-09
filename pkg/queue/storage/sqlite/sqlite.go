package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/andreademasi/goku/pkg/queue/storage"
)

const jobColumns = "id, type, payload, status, priority, max_retries, retry_count, scheduled_at, claimed_at, completed_at, error, created_at"

// SQLiteStorage implements storage using SQLite.
type SQLiteStorage struct {
	db   *sql.DB
	path string
}

func New(path string) (*SQLiteStorage, error) {
	if path == "" {
		return nil, fmt.Errorf("sqlite: path is required")
	}

	connStr := path
	if !strings.Contains(path, "?") {
		connStr += "?"
	} else {
		connStr += "&"
	}
	connStr += "_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=ON"

	db, err := sql.Open("sqlite3", connStr)
	if err != nil {
		return nil, fmt.Errorf("sqlite: failed to open database: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite: failed to ping database: %w", err)
	}

	store := &SQLiteStorage{
		db:   db,
		path: path,
	}

	if err := store.initPragmas(); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite: failed to initialize pragmas: %w", err)
	}

	return store, nil
}

func (s *SQLiteStorage) initPragmas() error {
	pragmas := []string{
		"PRAGMA synchronous=NORMAL",
		"PRAGMA cache_size=-64000",
		"PRAGMA temp_store=MEMORY",
		"PRAGMA mmap_size=268435456",
	}

	for _, pragma := range pragmas {
		if _, err := s.db.Exec(pragma); err != nil {
			return fmt.Errorf("failed to set pragma %s: %w", pragma, err)
		}
	}

	return nil
}

// Enqueue inserts a new job into the queue.
func (s *SQLiteStorage) Enqueue(ctx context.Context, job *storage.Job) (string, error) {
	if job == nil {
		return "", fmt.Errorf("sqlite: enqueue: job is nil")
	}
	if job.Type == "" {
		return "", fmt.Errorf("sqlite: enqueue: job type is required")
	}
	return insertJob(ctx, s.db, job)
}

// Dequeue atomically claims the next available job.
//
// Uses a transaction-based approach that works on all SQLite versions.
// The transaction ensures atomicity even without SKIP LOCKED support.
func (s *SQLiteStorage) Dequeue(ctx context.Context) (*storage.Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return nil, fmt.Errorf("sqlite: dequeue: failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	query := `
		SELECT ` + jobColumns + `
		FROM jobs
		WHERE status = ? 
		  AND datetime(scheduled_at) <= datetime('now')
		ORDER BY priority DESC, id ASC
		LIMIT 1
	`

	row := tx.QueryRowContext(ctx, query, storage.StatusPending)
	job, err := scanJob(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("sqlite: dequeue: failed to query job: %w", err)
	}

	updateQuery := `
		UPDATE jobs
		SET status = ?,
		    claimed_at = datetime('now')
		WHERE id = ? AND status = ?
	`

	result, err := tx.ExecContext(ctx, updateQuery, storage.StatusRunning, job.ID, storage.StatusPending)
	if err != nil {
		return nil, fmt.Errorf("sqlite: dequeue: failed to claim job: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("sqlite: dequeue: failed to check rows affected: %w", err)
	}

	if rows == 0 {
		return nil, nil
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("sqlite: dequeue: failed to commit transaction: %w", err)
	}

	job.Status = storage.StatusRunning
	now := time.Now().UTC()
	job.ClaimedAt = &now

	return job, nil
}

// Complete marks the job as completed successfully.
func (s *SQLiteStorage) Complete(ctx context.Context, jobID string) error {
	id, err := parseJobID(jobID)
	if err != nil {
		return fmt.Errorf("sqlite: complete: %w", err)
	}

	const query = `
		UPDATE jobs
		SET status = ?,
		    completed_at = datetime('now'),
		    error = NULL
		WHERE id = ?
	`

	res, err := s.db.ExecContext(ctx, query, storage.StatusCompleted, id)
	if err != nil {
		return fmt.Errorf("sqlite: complete: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlite: complete: rows affected: %w", err)
	}
	if rows == 0 {
		return storage.ErrJobNotFound
	}

	return nil
}

// Fail marks the job as failed with the provided error message.
func (s *SQLiteStorage) Fail(ctx context.Context, jobID, errMsg string) error {
	id, err := parseJobID(jobID)
	if err != nil {
		return fmt.Errorf("sqlite: fail: %w", err)
	}

	const query = `
		UPDATE jobs
		SET status = ?,
		    error = ?,
		    completed_at = datetime('now')
		WHERE id = ?
	`

	res, err := s.db.ExecContext(ctx, query, storage.StatusFailed, errMsg, id)
	if err != nil {
		return fmt.Errorf("sqlite: fail: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlite: fail: rows affected: %w", err)
	}
	if rows == 0 {
		return storage.ErrJobNotFound
	}

	return nil
}

// Retry resets the job state for another attempt.
func (s *SQLiteStorage) Retry(ctx context.Context, jobID string) error {
	id, err := parseJobID(jobID)
	if err != nil {
		return fmt.Errorf("sqlite: retry: %w", err)
	}

	const query = `
		UPDATE jobs
		SET status = ?,
		    retry_count = retry_count + 1,
		    scheduled_at = datetime('now'),
		    claimed_at = NULL,
		    completed_at = NULL,
		    error = NULL
		WHERE id = ?
	`

	res, err := s.db.ExecContext(ctx, query, storage.StatusPending, id)
	if err != nil {
		return fmt.Errorf("sqlite: retry: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlite: retry: rows affected: %w", err)
	}
	if rows == 0 {
		return storage.ErrJobNotFound
	}

	return nil
}

// GetJob retrieves a job by its identifier.
func (s *SQLiteStorage) GetJob(ctx context.Context, jobID string) (*storage.Job, error) {
	id, err := parseJobID(jobID)
	if err != nil {
		// Invalid job ID format is treated as "not found"
		return nil, storage.ErrJobNotFound
	}

	query := `SELECT ` + jobColumns + ` FROM jobs WHERE id = ?`
	row := s.db.QueryRowContext(ctx, query, id)

	job, err := scanJob(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, storage.ErrJobNotFound
		}
		return nil, fmt.Errorf("sqlite: get job: %w", err)
	}

	return job, nil
}

// ListJobs returns jobs matching the provided filter criteria.
func (s *SQLiteStorage) ListJobs(ctx context.Context, filter storage.JobFilter) ([]*storage.Job, error) {
	query := `SELECT ` + jobColumns + ` FROM jobs`
	var conditions []string
	var args []interface{}

	// Build WHERE clause
	if len(filter.IDs) > 0 {
		placeholders := make([]string, len(filter.IDs))
		for i, id := range filter.IDs {
			placeholders[i] = "?"
			args = append(args, id)
		}
		conditions = append(conditions, fmt.Sprintf("id IN (%s)", strings.Join(placeholders, ",")))
	}

	if len(filter.Types) > 0 {
		placeholders := make([]string, len(filter.Types))
		for i, t := range filter.Types {
			placeholders[i] = "?"
			args = append(args, t)
		}
		conditions = append(conditions, fmt.Sprintf("type IN (%s)", strings.Join(placeholders, ",")))
	}

	if len(filter.Statuses) > 0 {
		placeholders := make([]string, len(filter.Statuses))
		for i, s := range filter.Statuses {
			placeholders[i] = "?"
			args = append(args, s)
		}
		conditions = append(conditions, fmt.Sprintf("status IN (%s)", strings.Join(placeholders, ",")))
	}

	if filter.ScheduledBefore != nil {
		conditions = append(conditions, "datetime(scheduled_at) <= datetime(?)")
		args = append(args, filter.ScheduledBefore.Format("2006-01-02 15:04:05"))
	}

	if filter.ScheduledAfter != nil {
		conditions = append(conditions, "datetime(scheduled_at) >= datetime(?)")
		args = append(args, filter.ScheduledAfter.Format("2006-01-02 15:04:05"))
	}

	if filter.CompletedBefore != nil {
		conditions = append(conditions, "datetime(completed_at) <= datetime(?)")
		args = append(args, filter.CompletedBefore.Format("2006-01-02 15:04:05"))
	}

	if filter.CompletedAfter != nil {
		conditions = append(conditions, "datetime(completed_at) >= datetime(?)")
		args = append(args, filter.CompletedAfter.Format("2006-01-02 15:04:05"))
	}

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	// Add ordering
	query += " ORDER BY priority DESC, id ASC"

	if filter.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, filter.Limit)
	}

	if filter.Offset > 0 {
		query += " OFFSET ?"
		args = append(args, filter.Offset)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*storage.Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("sqlite: list jobs: scan: %w", err)
		}
		jobs = append(jobs, job)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: list jobs: rows: %w", err)
	}

	return jobs, nil
}

// DeleteJobs removes jobs matching the provided filter.
func (s *SQLiteStorage) DeleteJobs(ctx context.Context, filter storage.JobFilter) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	query := `DELETE FROM jobs WHERE 1=1`
	args := []interface{}{}

	if len(filter.IDs) > 0 {
		placeholders := make([]string, len(filter.IDs))
		for i, id := range filter.IDs {
			placeholders[i] = "?"
			args = append(args, id)
		}
		query += fmt.Sprintf(" AND id IN (%s)", strings.Join(placeholders, ","))
	}

	if len(filter.Types) > 0 {
		placeholders := make([]string, len(filter.Types))
		for i, t := range filter.Types {
			placeholders[i] = "?"
			args = append(args, t)
		}
		query += fmt.Sprintf(" AND type IN (%s)", strings.Join(placeholders, ","))
	}

	if len(filter.Statuses) > 0 {
		placeholders := make([]string, len(filter.Statuses))
		for i, s := range filter.Statuses {
			placeholders[i] = "?"
			args = append(args, string(s))
		}
		query += fmt.Sprintf(" AND status IN (%s)", strings.Join(placeholders, ","))
	}

	if filter.ScheduledBefore != nil {
		query += " AND scheduled_at < datetime(?)"
		args = append(args, filter.ScheduledBefore.Format("2006-01-02 15:04:05"))
	}

	if filter.ScheduledAfter != nil {
		query += " AND scheduled_at > datetime(?)"
		args = append(args, filter.ScheduledAfter.Format("2006-01-02 15:04:05"))
	}

	if filter.CompletedBefore != nil {
		query += " AND completed_at < datetime(?)"
		args = append(args, filter.CompletedBefore.Format("2006-01-02 15:04:05"))
	}

	if filter.CompletedAfter != nil {
		query += " AND completed_at > datetime(?)"
		args = append(args, filter.CompletedAfter.Format("2006-01-02 15:04:05"))
	}

	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("sqlite: delete jobs: %w", err)
	}

	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("sqlite: get rows affected: %w", err)
	}

	return deleted, nil
}

// RecoverStaleJobs resets jobs that have been running for too long.
func (s *SQLiteStorage) RecoverStaleJobs(ctx context.Context, staleDuration time.Duration) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	staleThreshold := time.Now().Add(-staleDuration)

	query := `
		UPDATE jobs
		SET status = ?,
		    claimed_at = NULL,
		    error = NULL
		WHERE status = ?
		  AND claimed_at < datetime(?)
	`

	result, err := s.db.ExecContext(
		ctx, query,
		storage.StatusPending,
		storage.StatusRunning,
		staleThreshold.Format("2006-01-02 15:04:05"),
	)
	if err != nil {
		return 0, fmt.Errorf("sqlite: recover stale jobs: %w", err)
	}

	recovered, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("sqlite: get rows affected: %w", err)
	}

	return recovered, nil
}

// Close releases all resources held by the storage backend.
func (s *SQLiteStorage) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Ping verifies connectivity to the storage backend.
func (s *SQLiteStorage) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Transaction support

type sqliteTx struct {
	tx *sql.Tx
}

func (s *SQLiteStorage) BeginTx(ctx context.Context) (storage.Tx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("sqlite: begin transaction: %w", err)
	}
	return &sqliteTx{tx: tx}, nil
}

func (t *sqliteTx) Enqueue(ctx context.Context, job *storage.Job) (string, error) {
	if job == nil {
		return "", fmt.Errorf("sqlite: tx enqueue: job is nil")
	}
	if job.Type == "" {
		return "", fmt.Errorf("sqlite: tx enqueue: job type is required")
	}
	return insertJob(ctx, t.tx, job)
}

func (t *sqliteTx) Commit() error {
	if err := t.tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: commit transaction: %w", err)
	}
	return nil
}

func (t *sqliteTx) Rollback() error {
	if err := t.tx.Rollback(); err != nil {
		return fmt.Errorf("sqlite: rollback transaction: %w", err)
	}
	return nil
}

type queryable interface {
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

func insertJob(ctx context.Context, q queryable, job *storage.Job) (string, error) {
	if err := storage.ValidatePriority(job.Priority); err != nil {
		return "", fmt.Errorf("sqlite: enqueue: %w", err)
	}

	if len(job.Payload) > 0 {
		if err := storage.ValidatePayloadSize(job.Payload); err != nil {
			return "", fmt.Errorf("sqlite: enqueue: %w", err)
		}
	}

	if job.Status == "" {
		job.Status = storage.StatusPending
	}

	if job.ScheduledAt.IsZero() {
		job.ScheduledAt = time.Now().UTC()
	}

	switch job.MaxRetries {
	case 0:
		job.MaxRetries = 3
	case -1:
		job.MaxRetries = 0
	}

	if len(job.Payload) == 0 {
		job.Payload = []byte("{}")
	}

	scheduledAt := job.ScheduledAt.UTC()

	const query = `
		INSERT INTO jobs (type, payload, status, priority, max_retries, retry_count, scheduled_at, created_at)
		VALUES (?, ?, ?, ?, ?, ?, datetime(?), datetime('now'))
		RETURNING id
	`

	var id int64
	err := q.QueryRowContext(
		ctx, query,
		job.Type,
		job.Payload,
		job.Status,
		job.Priority,
		job.MaxRetries,
		job.RetryCount,
		scheduledAt.Format("2006-01-02 15:04:05"),
	).Scan(&id)

	if err != nil {
		return "", fmt.Errorf("sqlite: insert job: %w", err)
	}

	return strconv.FormatInt(id, 10), nil
}

type scannable interface {
	Scan(dest ...interface{}) error
}

func scanJob(row scannable) (*storage.Job, error) {
	var job storage.Job
	var scheduledAt, claimedAt, completedAt, errorMsg, createdAt sql.NullString

	err := row.Scan(
		&job.ID,
		&job.Type,
		&job.Payload,
		&job.Status,
		&job.Priority,
		&job.MaxRetries,
		&job.RetryCount,
		&scheduledAt,
		&claimedAt,
		&completedAt,
		&errorMsg,
		&createdAt,
	)

	if err != nil {
		return nil, err
	}

	if scheduledAt.Valid {
		job.ScheduledAt = parseDateTime(scheduledAt.String)
	}

	if claimedAt.Valid {
		t := parseDateTime(claimedAt.String)
		job.ClaimedAt = &t
	}

	if completedAt.Valid {
		t := parseDateTime(completedAt.String)
		job.CompletedAt = &t
	}

	if errorMsg.Valid {
		job.Error = &errorMsg.String
	}

	if createdAt.Valid {
		job.CreatedAt = parseDateTime(createdAt.String)
	}

	return &job, nil
}

func parseDateTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}

	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05.999999",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05.999999Z",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t.UTC()
		}
	}

	return time.Time{}
}

func parseJobID(jobID string) (int64, error) {
	if jobID == "" {
		return 0, fmt.Errorf("job ID is empty")
	}

	id, err := strconv.ParseInt(jobID, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid job ID %q: %w", jobID, err)
	}

	return id, nil
}
