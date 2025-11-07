package queue

import (
	"time"

	"github.com/andreademasi/goku/pkg/queue/storage"
)

// Config contains configuration for the Queue.
type Config struct {
	Storage         storage.Storage
	Workers         int
	PollInterval    time.Duration
	ShutdownTimeout time.Duration
	RetryStrategy   RetryStrategy
	Logger          Logger
	CleanupInterval time.Duration
	CleanupAge      time.Duration
	StaleJobTimeout time.Duration
}

// WithDefaults returns a new Config with default values applied for unset fields.
func (c Config) WithDefaults() Config {
	if c.Workers <= 0 {
		c.Workers = 5
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 1 * time.Second
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = 30 * time.Second
	}
	if c.RetryStrategy == nil {
		c.RetryStrategy = ExponentialBackoff{}
	}
	if c.CleanupInterval > 0 && c.CleanupAge <= 0 {
		c.CleanupAge = 24 * time.Hour
	}
	return c
}

// Validate checks if the configuration is valid.
func (c Config) Validate() error {
	if c.Storage == nil {
		return ErrStorageRequired
	}
	if c.Workers < 0 {
		return ErrInvalidWorkerCount
	}
	if c.PollInterval < 0 {
		return ErrInvalidPollInterval
	}
	if c.ShutdownTimeout < 0 {
		return ErrInvalidShutdownTimeout
	}
	return nil
}

// RetryStrategy calculates the delay before retrying a failed job.
type RetryStrategy interface {
	NextRetry(retryCount int) time.Duration
}

// Logger defines the logging interface used by Queue.
type Logger interface {
	Debug(msg string, keysAndValues ...interface{})
	Info(msg string, keysAndValues ...interface{})
	Error(msg string, keysAndValues ...interface{})
}
