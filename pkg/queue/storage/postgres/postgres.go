package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/andreademasi/goku/pkg/queue/storage"
)

const jobColumns = "id, type, payload, status, priority, max_retries, retry_count, scheduled_at, claimed_at, completed_at, error, created_at"

// PostgresStorage implements the storage interfaces using PostgreSQL as the backend.
type PostgresStorage struct {
	db *sql.DB
}

// New creates a new PostgresStorage backed by the provided sql.DB connection.
func New(db *sql.DB) (*PostgresStorage, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres: db is nil")
	}
	return &PostgresStorage{db: db}, nil
}

// Enqueue inserts a new job into the queue.
func (s *PostgresStorage) Enqueue(ctx context.Context, job *storage.Job) (string, error) {
	if job == nil {
		return "", fmt.Errorf("postgres: enqueue: job is nil")
	}
	if job.Type == "" {
		return "", fmt.Errorf("postgres: enqueue: job type is required")
	}
	return insertJob(ctx, s.db, job)
}

// Dequeue atomically claims the next available job.
func (s *PostgresStorage) Dequeue(ctx context.Context) (*storage.Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	const query = `
        UPDATE jobs
        SET status = $1,
            claimed_at = NOW()
        WHERE id = (
            SELECT id FROM jobs
            WHERE status = $2 AND scheduled_at <= NOW()
            ORDER BY priority DESC, id ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING ` + jobColumns

	row := s.db.QueryRowContext(ctx, query, storage.StatusRunning, storage.StatusPending)
	job, err := scanJob(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("postgres: dequeue: %w", err)
	}

	return job, nil
}

// Complete marks the job as completed successfully.
func (s *PostgresStorage) Complete(ctx context.Context, jobID string) error {
	id, err := parseJobID(jobID)
	if err != nil {
		return fmt.Errorf("postgres: complete: %w", err)
	}

	const query = `
        UPDATE jobs
        SET status = $1,
            completed_at = NOW(),
            error = NULL
        WHERE id = $2
    `

	res, err := s.db.ExecContext(ctx, query, storage.StatusCompleted, id)
	if err != nil {
		return fmt.Errorf("postgres: complete: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("postgres: complete rows affected: %w", err)
	}
	if rows == 0 {
		return storage.ErrJobNotFound
	}

	return nil
}

// Fail marks the job as failed with the provided error message.
func (s *PostgresStorage) Fail(ctx context.Context, jobID, errMsg string) error {
	id, err := parseJobID(jobID)
	if err != nil {
		return fmt.Errorf("postgres: fail: %w", err)
	}

	const query = `
        UPDATE jobs
        SET status = $1,
            error = $2,
            completed_at = NOW()
        WHERE id = $3
    `

	res, err := s.db.ExecContext(ctx, query, storage.StatusFailed, errMsg, id)
	if err != nil {
		return fmt.Errorf("postgres: fail: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("postgres: fail rows affected: %w", err)
	}
	if rows == 0 {
		return storage.ErrJobNotFound
	}

	return nil
}

// Retry resets the job state for another attempt.
func (s *PostgresStorage) Retry(ctx context.Context, jobID string) error {
	id, err := parseJobID(jobID)
	if err != nil {
		return fmt.Errorf("postgres: retry: %w", err)
	}

	const query = `
        UPDATE jobs
        SET status = $1,
            retry_count = retry_count + 1,
            scheduled_at = NOW(),
            claimed_at = NULL,
            completed_at = NULL,
            error = NULL
        WHERE id = $2
    `

	res, err := s.db.ExecContext(ctx, query, storage.StatusPending, id)
	if err != nil {
		return fmt.Errorf("postgres: retry: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("postgres: retry rows affected: %w", err)
	}
	if rows == 0 {
		return storage.ErrJobNotFound
	}

	return nil
}

func (s *PostgresStorage) GetJob(ctx context.Context, jobID string) (*storage.Job, error) {
	id, err := parseJobID(jobID)
	if err != nil {
		return nil, storage.ErrJobNotFound
	}

	query := "SELECT " + jobColumns + " FROM jobs WHERE id = $1"
	row := s.db.QueryRowContext(ctx, query, id)
	job, err := scanJob(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, storage.ErrJobNotFound
		}
		return nil, fmt.Errorf("postgres: get job: %w", err)
	}

	return job, nil
}

func (s *PostgresStorage) ListJobs(ctx context.Context, filter storage.JobFilter) ([]*storage.Job, error) {
	var (
		clauses []string
		args    []any
		nextArg = 1
	)

	if len(filter.IDs) > 0 {
		ids := make([]int64, 0, len(filter.IDs))
		for _, idStr := range filter.IDs {
			id, err := strconv.ParseInt(idStr, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("postgres: list jobs: invalid id %q: %w", idStr, err)
			}
			ids = append(ids, id)
		}
		clauses = append(clauses, fmt.Sprintf("id = ANY($%d)", nextArg))
		args = append(args, pq.Array(ids))
		nextArg++
	}

	if len(filter.Types) > 0 {
		clauses = append(clauses, fmt.Sprintf("type = ANY($%d)", nextArg))
		args = append(args, pq.Array(filter.Types))
		nextArg++
	}

	if len(filter.Statuses) > 0 {
		statuses := make([]string, len(filter.Statuses))
		for i, s := range filter.Statuses {
			statuses[i] = string(s)
		}
		clauses = append(clauses, fmt.Sprintf("status = ANY($%d)", nextArg))
		args = append(args, pq.Array(statuses))
		nextArg++
	}

	if filter.ScheduledBefore != nil {
		clauses = append(clauses, fmt.Sprintf("scheduled_at <= $%d", nextArg))
		args = append(args, *filter.ScheduledBefore)
		nextArg++
	}

	if filter.ScheduledAfter != nil {
		clauses = append(clauses, fmt.Sprintf("scheduled_at >= $%d", nextArg))
		args = append(args, *filter.ScheduledAfter)
		nextArg++
	}

	if filter.CompletedBefore != nil {
		clauses = append(clauses, fmt.Sprintf("completed_at <= $%d", nextArg))
		args = append(args, *filter.CompletedBefore)
		nextArg++
	}

	if filter.CompletedAfter != nil {
		clauses = append(clauses, fmt.Sprintf("completed_at >= $%d", nextArg))
		args = append(args, *filter.CompletedAfter)
		nextArg++
	}

	var builder strings.Builder
	builder.WriteString("SELECT ")
	builder.WriteString(jobColumns)
	builder.WriteString(" FROM jobs")

	if len(clauses) > 0 {
		builder.WriteString(" WHERE ")
		builder.WriteString(strings.Join(clauses, " AND "))
	}

	builder.WriteString(" ORDER BY priority DESC, id ASC")

	if filter.Limit > 0 {
		builder.WriteString(fmt.Sprintf(" LIMIT $%d", nextArg))
		args = append(args, filter.Limit)
		nextArg++
	}

	if filter.Offset > 0 {
		builder.WriteString(fmt.Sprintf(" OFFSET $%d", nextArg))
		args = append(args, filter.Offset)
		nextArg++
	}

	rows, err := s.db.QueryContext(ctx, builder.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: list jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*storage.Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("postgres: list jobs scan: %w", err)
		}
		jobs = append(jobs, job)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list jobs rows: %w", err)
	}

	return jobs, nil
}

func (s *PostgresStorage) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("postgres: close: %w", err)
	}
	return nil
}

func (s *PostgresStorage) Ping(ctx context.Context) error {
	if err := s.db.PingContext(ctx); err != nil {
		return fmt.Errorf("postgres: ping: %w", err)
	}
	return nil
}

func (s *PostgresStorage) BeginTx(ctx context.Context) (storage.Tx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("postgres: begin tx: %w", err)
	}
	return &postgresTx{tx: tx}, nil
}

type postgresTx struct {
	tx *sql.Tx
}

func (t *postgresTx) Enqueue(ctx context.Context, job *storage.Job) (string, error) {
	if job == nil {
		return "", fmt.Errorf("postgres: tx enqueue: job is nil")
	}
	if job.Type == "" {
		return "", fmt.Errorf("postgres: tx enqueue: job type is required")
	}
	return insertJob(ctx, t.tx, job)
}

func (t *postgresTx) Commit() error {
	if err := t.tx.Commit(); err != nil {
		return fmt.Errorf("postgres: tx commit: %w", err)
	}
	return nil
}

func (t *postgresTx) Rollback() error {
	if err := t.tx.Rollback(); err != nil {
		if err == sql.ErrTxDone {
			return nil
		}
		return fmt.Errorf("postgres: tx rollback: %w", err)
	}
	return nil
}

type queryRow interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func insertJob(ctx context.Context, runner queryRow, job *storage.Job) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	if err := storage.ValidatePriority(job.Priority); err != nil {
		return "", fmt.Errorf("postgres: enqueue: %w", err)
	}

	if len(job.Payload) > 0 {
		if err := storage.ValidatePayloadSize(job.Payload); err != nil {
			return "", fmt.Errorf("postgres: enqueue: %w", err)
		}
	}

	status := job.Status
	if status == "" {
		status = storage.StatusPending
	}

	scheduledAt := job.ScheduledAt
	if scheduledAt.IsZero() {
		scheduledAt = time.Now().UTC()
	}

	claimedAt := nullTime(job.ClaimedAt)
	completedAt := nullTime(job.CompletedAt)
	errMsg := nullString(job.Error)

	maxRetries := job.MaxRetries
	switch maxRetries {
	case 0:
		maxRetries = 3
	case -1:
		maxRetries = 0
	}

	payload := job.Payload
	if len(payload) == 0 {
		payload = []byte("{}")
	}

	const query = `
        INSERT INTO jobs (type, payload, status, priority, max_retries, retry_count, scheduled_at, claimed_at, completed_at, error)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        RETURNING id, scheduled_at, created_at
    `

	row := runner.QueryRowContext(ctx, query,
		job.Type,
		payload,
		status,
		job.Priority,
		maxRetries,
		job.RetryCount,
		scheduledAt,
		claimedAt,
		completedAt,
		errMsg,
	)

	var (
		id          int64
		storedSched time.Time
		createdAt   time.Time
	)

	if err := row.Scan(&id, &storedSched, &createdAt); err != nil {
		return "", fmt.Errorf("postgres: enqueue scan: %w", err)
	}

	job.ID = strconv.FormatInt(id, 10)
	job.Status = status
	job.MaxRetries = maxRetries
	job.ScheduledAt = storedSched
	job.CreatedAt = createdAt
	job.ClaimedAt = timePtrFromNull(claimedAt)
	job.CompletedAt = timePtrFromNull(completedAt)
	job.Error = stringPtrFromNull(errMsg)

	return job.ID, nil
}

func scanJob(scanner interface{ Scan(dest ...any) error }) (*storage.Job, error) {
	var (
		id          int64
		jobType     string
		payload     []byte
		status      string
		priority    int
		maxRetries  int
		retryCount  int
		scheduledAt time.Time
		claimedAt   sql.NullTime
		completedAt sql.NullTime
		errMsg      sql.NullString
		createdAt   time.Time
	)

	if err := scanner.Scan(
		&id,
		&jobType,
		&payload,
		&status,
		&priority,
		&maxRetries,
		&retryCount,
		&scheduledAt,
		&claimedAt,
		&completedAt,
		&errMsg,
		&createdAt,
	); err != nil {
		return nil, err
	}

	job := &storage.Job{
		ID:          strconv.FormatInt(id, 10),
		Type:        jobType,
		Payload:     payload,
		Status:      storage.JobStatus(status),
		Priority:    priority,
		MaxRetries:  maxRetries,
		RetryCount:  retryCount,
		ScheduledAt: scheduledAt,
		CreatedAt:   createdAt,
	}

	if claimedAt.Valid {
		ts := claimedAt.Time
		job.ClaimedAt = &ts
	}

	if completedAt.Valid {
		ts := completedAt.Time
		job.CompletedAt = &ts
	}

	if errMsg.Valid {
		msg := errMsg.String
		job.Error = &msg
	}

	return job, nil
}

func (s *PostgresStorage) DeleteJobs(ctx context.Context, filter storage.JobFilter) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	query := `DELETE FROM jobs WHERE true`
	args := []interface{}{}
	argNum := 1

	if len(filter.IDs) > 0 {
		idNums := make([]int64, 0, len(filter.IDs))
		for _, id := range filter.IDs {
			num, err := parseJobID(id)
			if err != nil {
				return 0, fmt.Errorf("postgres: invalid job id in filter: %w", err)
			}
			idNums = append(idNums, num)
		}
		query += fmt.Sprintf(" AND id = ANY($%d)", argNum)
		args = append(args, pq.Array(idNums))
		argNum++
	}

	if len(filter.Types) > 0 {
		query += fmt.Sprintf(" AND type = ANY($%d)", argNum)
		args = append(args, pq.Array(filter.Types))
		argNum++
	}

	if len(filter.Statuses) > 0 {
		statuses := make([]string, len(filter.Statuses))
		for i, s := range filter.Statuses {
			statuses[i] = string(s)
		}
		query += fmt.Sprintf(" AND status = ANY($%d)", argNum)
		args = append(args, pq.Array(statuses))
		argNum++
	}

	if filter.ScheduledBefore != nil {
		query += fmt.Sprintf(" AND scheduled_at < $%d", argNum)
		args = append(args, filter.ScheduledBefore.UTC())
		argNum++
	}

	if filter.ScheduledAfter != nil {
		query += fmt.Sprintf(" AND scheduled_at > $%d", argNum)
		args = append(args, filter.ScheduledAfter.UTC())
		argNum++
	}

	if filter.CompletedBefore != nil {
		query += fmt.Sprintf(" AND completed_at < $%d", argNum)
		args = append(args, filter.CompletedBefore.UTC())
		argNum++
	}

	if filter.CompletedAfter != nil {
		query += fmt.Sprintf(" AND completed_at > $%d", argNum)
		args = append(args, filter.CompletedAfter.UTC())
		argNum++
	}

	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("postgres: delete jobs: %w", err)
	}

	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("postgres: get rows affected: %w", err)
	}

	return deleted, nil
}

func (s *PostgresStorage) RecoverStaleJobs(ctx context.Context, staleDuration time.Duration) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	staleThreshold := time.Now().Add(-staleDuration).UTC()

	query := `
		UPDATE jobs
		SET status = $1,
		    claimed_at = NULL,
		    error = NULL
		WHERE status = $2
		  AND claimed_at < $3
	`

	result, err := s.db.ExecContext(ctx, query, storage.StatusPending, storage.StatusRunning, staleThreshold)
	if err != nil {
		return 0, fmt.Errorf("postgres: recover stale jobs: %w", err)
	}

	recovered, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("postgres: get rows affected: %w", err)
	}

	return recovered, nil
}

func parseJobID(jobID string) (int64, error) {
	if jobID == "" {
		return 0, fmt.Errorf("invalid job id: empty")
	}
	id, err := strconv.ParseInt(jobID, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid job id %q: %w", jobID, err)
	}
	return id, nil
}

func nullTime(ts *time.Time) sql.NullTime {
	if ts == nil {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: ts.UTC(), Valid: true}
}

func nullString(s *string) sql.NullString {
	if s == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: *s, Valid: true}
}

func timePtrFromNull(nt sql.NullTime) *time.Time {
	if !nt.Valid {
		return nil
	}
	ts := nt.Time
	return &ts
}

func stringPtrFromNull(ns sql.NullString) *string {
	if !ns.Valid {
		return nil
	}
	s := ns.String
	return &s
}
