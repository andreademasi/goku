package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/andreademasi/goku/pkg/queue/storage"
)

// Queue manages job enqueueing and processing with a pool of workers.
type Queue struct {
	config   Config
	storage  storage.Storage
	handlers *handlerRegistry

	workers   []*worker
	workerWg  sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	startOnce sync.Once
	stopOnce  sync.Once
	started   bool
	mu        sync.RWMutex
}

// New creates a new Queue with the provided configuration.
func New(config Config) (*Queue, error) {
	// Apply defaults
	config = config.WithDefaults()

	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid queue configuration: %w", err)
	}

	return &Queue{
		config:   config,
		storage:  config.Storage,
		handlers: newHandlerRegistry(),
	}, nil
}

// Register adds a handler for a specific job type.
func (q *Queue) Register(jobType string, handler Handler) error {
	return q.handlers.register(jobType, handler)
}

// Enqueue adds a new job to the queue with default options.
func (q *Queue) Enqueue(ctx context.Context, jobType string, payload interface{}) (string, error) {
	return q.EnqueueWithOptions(ctx, jobType, payload, EnqueueOptions{})
}

// EnqueueOptions contains optional parameters for job enqueuing.
type EnqueueOptions struct {
	Priority    int
	MaxRetries  int
	ScheduledAt time.Time
}

// EnqueueWithOptions adds a new job with additional options.
func (q *Queue) EnqueueWithOptions(ctx context.Context, jobType string, payload interface{}, opts EnqueueOptions) (string, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal job payload: %w", err)
	}

	if err := storage.ValidatePayloadSize(data); err != nil {
		return "", err
	}

	if opts.Priority != 0 {
		if err := storage.ValidatePriority(opts.Priority); err != nil {
			return "", err
		}
	}

	job := &storage.Job{
		Type:       jobType,
		Payload:    data,
		Priority:   opts.Priority,
		MaxRetries: opts.MaxRetries,
	}

	if !opts.ScheduledAt.IsZero() {
		job.ScheduledAt = opts.ScheduledAt
	}

	jobID, err := q.storage.Enqueue(ctx, job)
	if err != nil {
		return "", fmt.Errorf("failed to enqueue job: %w", err)
	}

	q.log("Info", "job enqueued", "job_id", jobID, "job_type", jobType, "priority", opts.Priority)

	return jobID, nil
}

// Start begins processing jobs with the configured number of workers.
func (q *Queue) Start() error {
	var err error
	q.startOnce.Do(func() {
		q.mu.Lock()
		defer q.mu.Unlock()

		if q.started {
			err = ErrQueueAlreadyStarted
			return
		}

		q.ctx, q.cancel = context.WithCancel(context.Background())

		q.workers = make([]*worker, q.config.Workers)
		for i := 0; i < q.config.Workers; i++ {
			w := newWorker(i+1, q)
			q.workers[i] = w

			q.workerWg.Add(1)
			go func(worker *worker) {
				defer q.workerWg.Done()
				worker.run(q.ctx)
			}(w)
		}

		if q.config.CleanupInterval > 0 {
			q.workerWg.Add(1)
			go func() {
				defer q.workerWg.Done()
				q.runCleanup(q.ctx)
			}()
			q.log("Info", "cleanup enabled",
				"interval", q.config.CleanupInterval,
				"age", q.config.CleanupAge)
		}

		if q.config.StaleJobTimeout > 0 {
			q.workerWg.Add(1)
			go func() {
				defer q.workerWg.Done()
				q.runStaleRecovery(q.ctx)
			}()
			q.log("Info", "stale job recovery enabled",
				"timeout", q.config.StaleJobTimeout)
		}

		q.started = true
		q.log("Info", "queue started", "workers", q.config.Workers)
	})

	return err
}

func (q *Queue) runCleanup(ctx context.Context) {
	ticker := time.NewTicker(q.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			deleted, err := q.cleanup(ctx)
			if err != nil {
				q.log("Error", "cleanup failed", "error", err)
			} else if deleted > 0 {
				q.log("Info", "cleanup completed", "deleted", deleted)
			}
		}
	}
}

func (q *Queue) cleanup(ctx context.Context) (int64, error) {
	cutoff := time.Now().Add(-q.config.CleanupAge)

	deletedCompleted, err := q.storage.DeleteJobs(ctx, storage.JobFilter{
		Statuses:        []storage.JobStatus{storage.StatusCompleted},
		CompletedBefore: &cutoff,
	})
	if err != nil {
		return 0, fmt.Errorf("delete completed jobs: %w", err)
	}

	deletedFailed, err := q.storage.DeleteJobs(ctx, storage.JobFilter{
		Statuses:        []storage.JobStatus{storage.StatusFailed},
		CompletedBefore: &cutoff,
	})
	if err != nil {
		return deletedCompleted, fmt.Errorf("delete failed jobs: %w", err)
	}

	return deletedCompleted + deletedFailed, nil
}

func (q *Queue) runStaleRecovery(ctx context.Context) {
	interval := q.config.StaleJobTimeout / 2
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			recovered, err := q.storage.RecoverStaleJobs(ctx, q.config.StaleJobTimeout)
			if err != nil {
				q.log("Error", "stale job recovery failed", "error", err)
			} else if recovered > 0 {
				q.log("Info", "stale jobs recovered", "count", recovered)
			}
		}
	}
}

// Stop gracefully shuts down the queue, waiting for workers to finish.
func (q *Queue) Stop() error {
	var err error
	q.stopOnce.Do(func() {
		q.mu.Lock()
		if !q.started {
			q.mu.Unlock()
			err = ErrQueueNotStarted
			return
		}
		q.mu.Unlock()

		q.log("Info", "stopping queue", "timeout", q.config.ShutdownTimeout)

		q.cancel()

		done := make(chan struct{})
		go func() {
			q.workerWg.Wait()
			close(done)
		}()

		select {
		case <-done:
			q.log("Info", "queue stopped gracefully")
		case <-time.After(q.config.ShutdownTimeout):
			q.log("Error", "shutdown timeout exceeded", "timeout", q.config.ShutdownTimeout)
			err = fmt.Errorf("shutdown timeout exceeded after %s", q.config.ShutdownTimeout)
		}

		q.mu.Lock()
		q.started = false
		q.mu.Unlock()
	})

	return err
}

// IsRunning returns true if the queue is currently processing jobs.
func (q *Queue) IsRunning() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.started
}

// Ping checks if the storage backend is healthy.
func (q *Queue) Ping(ctx context.Context) error {
	return q.storage.Ping(ctx)
}

// RegisteredTypes returns a list of all registered job types.
func (q *Queue) RegisteredTypes() []string {
	return q.handlers.types()
}

func (q *Queue) log(level string, msg string, keysAndValues ...interface{}) {
	if q.config.Logger == nil {
		return
	}

	switch level {
	case "Debug":
		q.config.Logger.Debug(msg, keysAndValues...)
	case "Info":
		q.config.Logger.Info(msg, keysAndValues...)
	case "Error":
		q.config.Logger.Error(msg, keysAndValues...)
	}
}
