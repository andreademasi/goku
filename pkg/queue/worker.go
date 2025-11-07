package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/andreademasi/goku/pkg/queue/storage"
)

type worker struct {
	id    int
	queue *Queue
}

func newWorker(id int, queue *Queue) *worker {
	return &worker{
		id:    id,
		queue: queue,
	}
}

func (w *worker) run(ctx context.Context) {
	w.log("Debug", "worker started")
	defer w.log("Debug", "worker stopped")

	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := w.processNext(ctx); err != nil {
				w.log("Error", "error processing job", "error", err)
			}
		}
	}
}

func (w *worker) processNext(ctx context.Context) error {
	job, err := w.queue.storage.Dequeue(ctx)
	if err != nil {
		return fmt.Errorf("dequeue failed: %w", err)
	}

	if job == nil {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(w.queue.config.PollInterval):
			return nil
		}
	}

	w.log("Debug", "processing job", "job_id", job.ID, "job_type", job.Type, "retry_count", job.RetryCount)

	err = w.executeJob(ctx, job)
	if err != nil {
		w.handleFailure(ctx, job, err)
		return nil
	}

	if err := w.queue.storage.Complete(ctx, job.ID); err != nil {
		return fmt.Errorf("failed to mark job as complete: %w", err)
	}

	w.log("Info", "job completed", "job_id", job.ID, "job_type", job.Type)
	return nil
}

func (w *worker) executeJob(ctx context.Context, job *storage.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in handler: %v", r)
			w.log("Error", "handler panicked",
				"job_id", job.ID,
				"job_type", job.Type,
				"panic", r)
		}
	}()

	err = w.queue.handlers.processJob(ctx, job)
	if err != nil {
		return fmt.Errorf("handler failed: %w", err)
	}

	return nil
}

func (w *worker) handleFailure(ctx context.Context, job *storage.Job, execErr error) {
	w.log("Error", "job failed",
		"job_id", job.ID,
		"job_type", job.Type,
		"retry_count", job.RetryCount,
		"error", execErr)

	if job.RetryCount < job.MaxRetries {
		delay := w.queue.config.RetryStrategy.NextRetry(job.RetryCount)
		scheduledAt := time.Now().Add(delay)

		w.log("Info", "scheduling retry",
			"job_id", job.ID,
			"retry_count", job.RetryCount+1,
			"max_retries", job.MaxRetries,
			"delay", delay,
			"scheduled_at", scheduledAt)

		if err := w.queue.storage.Fail(ctx, job.ID, execErr.Error()); err != nil {
			w.log("Error", "failed to mark job as failed", "job_id", job.ID, "error", err)
			return
		}

		if err := w.queue.storage.Retry(ctx, job.ID); err != nil {
			w.log("Error", "failed to schedule retry", "job_id", job.ID, "error", err)
			return
		}
	} else {
		w.log("Error", "job permanently failed",
			"job_id", job.ID,
			"job_type", job.Type,
			"retry_count", job.RetryCount,
			"max_retries", job.MaxRetries)

		if err := w.queue.storage.Fail(ctx, job.ID, execErr.Error()); err != nil {
			w.log("Error", "failed to mark job as failed", "job_id", job.ID, "error", err)
		}
	}
}

func (w *worker) log(level string, msg string, keysAndValues ...interface{}) {
	args := append([]interface{}{"worker_id", w.id}, keysAndValues...)
	w.queue.log(level, msg, args...)
}
