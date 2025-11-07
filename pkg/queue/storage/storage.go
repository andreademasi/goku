package storage

import (
	"context"
	"errors"
	"time"
)

// Storage defines backend operations for the job queue.
// Implementations must be safe for concurrent use.
type Storage interface {
	// Enqueue persists a new job and returns its unique identifier.
	//
	// The job's Status will be set to StatusPending if not already set.
	// The job's ScheduledAt will be set to time.Now() if zero.
	// The job's MaxRetries will be set to 3 if zero.
	// Empty or nil Payload will be treated as an empty JSON object "{}".
	//
	// Returns:
	// - The unique job ID (backend-specific format)
	// - An error if the job cannot be persisted
	//
	// The returned job ID must be usable in all other Storage methods.
	Enqueue(ctx context.Context, job *Job) (string, error)
	Dequeue(ctx context.Context) (*Job, error)
	Complete(ctx context.Context, jobID string) error
	Fail(ctx context.Context, jobID string, errMsg string) error
	Retry(ctx context.Context, jobID string) error
	GetJob(ctx context.Context, jobID string) (*Job, error)
	ListJobs(ctx context.Context, filter JobFilter) ([]*Job, error)
	Close() error
	Ping(ctx context.Context) error
	DeleteJobs(ctx context.Context, filter JobFilter) (int64, error)
	RecoverStaleJobs(ctx context.Context, staleDuration time.Duration) (int64, error)
}

var (
	// ErrJobNotFound is returned when a job cannot be located in storage.
	ErrJobNotFound = errors.New("job not found")
)

// TransactionalStorage is implemented by backends that provide transactional enqueue semantics.
type TransactionalStorage interface {
	Storage
	BeginTx(ctx context.Context) (Tx, error)
}

// Tx represents a backend-specific transaction handle used for enqueue operations.
type Tx interface {
	Enqueue(ctx context.Context, job *Job) (string, error)
	Commit() error
	Rollback() error
}
