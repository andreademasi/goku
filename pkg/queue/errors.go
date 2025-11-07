package queue

import "errors"

var (
	// ErrStorageRequired is returned when a nil storage backend is provided.
	ErrStorageRequired = errors.New("queue: storage is required")

	// ErrInvalidWorkerCount is returned when worker count is negative.
	ErrInvalidWorkerCount = errors.New("queue: worker count must be non-negative")

	// ErrInvalidPollInterval is returned when poll interval is negative.
	ErrInvalidPollInterval = errors.New("queue: poll interval must be non-negative")

	// ErrInvalidShutdownTimeout is returned when shutdown timeout is negative.
	ErrInvalidShutdownTimeout = errors.New("queue: shutdown timeout must be non-negative")

	// ErrQueueNotStarted is returned when an operation requires the queue to be running.
	ErrQueueNotStarted = errors.New("queue: queue is not started")

	// ErrQueueAlreadyStarted is returned when Start is called on an already running queue.
	ErrQueueAlreadyStarted = errors.New("queue: queue is already started")

	// ErrHandlerNotFound is returned when no handler is registered for a job type.
	ErrHandlerNotFound = errors.New("queue: no handler registered for job type")

	// ErrHandlerAlreadyRegistered is returned when attempting to register a duplicate handler.
	ErrHandlerAlreadyRegistered = errors.New("queue: handler already registered for job type")
)
