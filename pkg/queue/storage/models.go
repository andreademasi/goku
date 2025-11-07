package storage

import (
	"errors"
	"fmt"
	"time"
)

// Validation constants
const (
	// MaxPayloadSize is the maximum allowed size for job payloads (10MB)
	MaxPayloadSize = 10 * 1024 * 1024

	// MaxJobIDLength is the maximum length for job IDs
	MaxJobIDLength = 128

	// MinPriority is the minimum allowed priority value
	MinPriority = 0

	// MaxPriority is the maximum allowed priority value
	MaxPriority = 1000

	// DefaultPriority is the default priority if not specified
	DefaultPriority = 0
)

// Job represents a unit of work persisted by a storage backend.
type Job struct {
	ID          string     `json:"id"`
	Type        string     `json:"type"`
	Payload     []byte     `json:"payload"`
	Status      JobStatus  `json:"status"`
	Priority    int        `json:"priority"`
	MaxRetries  int        `json:"max_retries"`
	RetryCount  int        `json:"retry_count"`
	ScheduledAt time.Time  `json:"scheduled_at"`
	ClaimedAt   *time.Time `json:"claimed_at"`
	CompletedAt *time.Time `json:"completed_at"`
	Error       *string    `json:"error"`
	CreatedAt   time.Time  `json:"created_at"`
}

// JobStatus represents the lifecycle state of a job.
type JobStatus string

const (
	StatusPending   JobStatus = "pending"
	StatusRunning   JobStatus = "running"
	StatusCompleted JobStatus = "completed"
	StatusFailed    JobStatus = "failed"
)

// JobFilter describes query parameters for listing jobs from storage.
type JobFilter struct {
	IDs             []string
	Types           []string
	Statuses        []JobStatus
	ScheduledBefore *time.Time
	ScheduledAfter  *time.Time
	CompletedBefore *time.Time
	CompletedAfter  *time.Time
	Limit           int
	Offset          int
}

func ValidateJobID(id string) error {
	if id == "" {
		return errors.New("job ID is empty")
	}
	if len(id) > MaxJobIDLength {
		return fmt.Errorf("job ID exceeds maximum length of %d characters", MaxJobIDLength)
	}
	return nil
}

func ValidatePriority(priority int) error {
	if priority < MinPriority || priority > MaxPriority {
		return fmt.Errorf("priority %d outside valid range [%d, %d]",
			priority, MinPriority, MaxPriority)
	}
	return nil
}

func ValidatePayloadSize(payload []byte) error {
	if len(payload) > MaxPayloadSize {
		return fmt.Errorf("payload size %d bytes exceeds maximum %d bytes",
			len(payload), MaxPayloadSize)
	}
	return nil
}
