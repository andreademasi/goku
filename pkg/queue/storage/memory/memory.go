package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/andreademasi/goku/pkg/queue/storage"
)

type MemoryStorage struct {
	mu       sync.RWMutex
	jobs     map[string]*storage.Job
	sequence int64
}

func New() *MemoryStorage {
	return &MemoryStorage{
		jobs: make(map[string]*storage.Job),
	}
}

func (s *MemoryStorage) nextID() string {
	id := atomic.AddInt64(&s.sequence, 1)
	return fmt.Sprintf("mem-%d", id)
}

func (s *MemoryStorage) Enqueue(ctx context.Context, job *storage.Job) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	if job == nil {
		return "", fmt.Errorf("memory: enqueue: job is nil")
	}

	if job.Type == "" {
		return "", fmt.Errorf("memory: enqueue: job type is required")
	}

	if err := storage.ValidatePriority(job.Priority); err != nil {
		return "", fmt.Errorf("memory: enqueue: %w", err)
	}

	if len(job.Payload) > 0 {
		if err := storage.ValidatePayloadSize(job.Payload); err != nil {
			return "", fmt.Errorf("memory: enqueue: %w", err)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

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

	job.ID = s.nextID()
	job.CreatedAt = time.Now().UTC()

	jobCopy := *job
	s.jobs[job.ID] = &jobCopy

	return job.ID, nil
}

func (s *MemoryStorage) Dequeue(ctx context.Context) (*storage.Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()

	var bestJob *storage.Job
	for _, job := range s.jobs {
		if job.Status != storage.StatusPending {
			continue
		}

		if job.ScheduledAt.After(now) {
			continue
		}

		if bestJob == nil {
			bestJob = job
		} else if job.Priority > bestJob.Priority {
			bestJob = job
		} else if job.Priority == bestJob.Priority && job.ID < bestJob.ID {
			bestJob = job
		}
	}

	if bestJob == nil {
		return nil, nil
	}

	bestJob.Status = storage.StatusRunning
	bestJob.ClaimedAt = &now

	jobCopy := *bestJob
	return &jobCopy, nil
}

func (s *MemoryStorage) Complete(ctx context.Context, jobID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	job, exists := s.jobs[jobID]
	if !exists {
		return storage.ErrJobNotFound
	}

	now := time.Now().UTC()
	job.Status = storage.StatusCompleted
	job.CompletedAt = &now
	job.Error = nil

	return nil
}

func (s *MemoryStorage) Fail(ctx context.Context, jobID string, errMsg string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	job, exists := s.jobs[jobID]
	if !exists {
		return storage.ErrJobNotFound
	}

	now := time.Now().UTC()
	job.Status = storage.StatusFailed
	job.CompletedAt = &now
	job.Error = &errMsg

	return nil
}

func (s *MemoryStorage) Retry(ctx context.Context, jobID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	job, exists := s.jobs[jobID]
	if !exists {
		return storage.ErrJobNotFound
	}

	job.RetryCount++
	job.Status = storage.StatusPending
	job.ScheduledAt = time.Now().UTC()
	job.ClaimedAt = nil
	job.CompletedAt = nil
	job.Error = nil

	return nil
}

func (s *MemoryStorage) GetJob(ctx context.Context, jobID string) (*storage.Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	job, exists := s.jobs[jobID]
	if !exists {
		return nil, storage.ErrJobNotFound
	}

	jobCopy := *job
	return &jobCopy, nil
}

func (s *MemoryStorage) ListJobs(ctx context.Context, filter storage.JobFilter) ([]*storage.Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []*storage.Job

	for _, job := range s.jobs {
		results = append(results, job)
	}

	results = s.applyFilters(results, filter)

	sort.Slice(results, func(i, j int) bool {
		if results[i].Priority != results[j].Priority {
			return results[i].Priority > results[j].Priority
		}
		return results[i].ID < results[j].ID
	})

	results = s.applyPagination(results, filter)

	copies := make([]*storage.Job, len(results))
	for i, job := range results {
		jobCopy := *job
		copies[i] = &jobCopy
	}

	return copies, nil
}

func (s *MemoryStorage) applyFilters(jobs []*storage.Job, filter storage.JobFilter) []*storage.Job {
	var filtered []*storage.Job

	for _, job := range jobs {
		if len(filter.IDs) > 0 {
			found := false
			for _, id := range filter.IDs {
				if job.ID == id {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		if len(filter.Types) > 0 {
			found := false
			for _, t := range filter.Types {
				if job.Type == t {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		if len(filter.Statuses) > 0 {
			found := false
			for _, s := range filter.Statuses {
				if job.Status == s {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		if filter.ScheduledBefore != nil {
			if job.ScheduledAt.After(*filter.ScheduledBefore) {
				continue
			}
		}

		if filter.ScheduledAfter != nil {
			if job.ScheduledAt.Before(*filter.ScheduledAfter) {
				continue
			}
		}

		filtered = append(filtered, job)
	}

	return filtered
}

func (s *MemoryStorage) applyPagination(jobs []*storage.Job, filter storage.JobFilter) []*storage.Job {
	if filter.Offset > 0 {
		if filter.Offset >= len(jobs) {
			return []*storage.Job{}
		}
		jobs = jobs[filter.Offset:]
	}

	if filter.Limit > 0 && filter.Limit < len(jobs) {
		jobs = jobs[:filter.Limit]
	}

	return jobs
}

func (s *MemoryStorage) DeleteJobs(ctx context.Context, filter storage.JobFilter) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var deleted int64
	for id, job := range s.jobs {
		if s.jobMatchesFilter(job, filter) {
			delete(s.jobs, id)
			deleted++
		}
	}

	return deleted, nil
}

func (s *MemoryStorage) jobMatchesFilter(job *storage.Job, filter storage.JobFilter) bool {
	if len(filter.IDs) > 0 {
		found := false
		for _, id := range filter.IDs {
			if job.ID == id {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	if len(filter.Types) > 0 {
		found := false
		for _, t := range filter.Types {
			if job.Type == t {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	if len(filter.Statuses) > 0 {
		found := false
		for _, s := range filter.Statuses {
			if job.Status == s {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	if filter.ScheduledBefore != nil {
		if job.ScheduledAt.After(*filter.ScheduledBefore) {
			return false
		}
	}

	if filter.ScheduledAfter != nil {
		if job.ScheduledAt.Before(*filter.ScheduledAfter) {
			return false
		}
	}

	if filter.CompletedBefore != nil {
		if job.CompletedAt == nil || job.CompletedAt.After(*filter.CompletedBefore) {
			return false
		}
	}

	if filter.CompletedAfter != nil {
		if job.CompletedAt == nil || job.CompletedAt.Before(*filter.CompletedAfter) {
			return false
		}
	}

	return true
}

func (s *MemoryStorage) RecoverStaleJobs(ctx context.Context, staleDuration time.Duration) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	staleThreshold := now.Add(-staleDuration)
	var recovered int64

	for _, job := range s.jobs {
		if job.Status == storage.StatusRunning &&
			job.ClaimedAt != nil &&
			job.ClaimedAt.Before(staleThreshold) {

			job.Status = storage.StatusPending
			job.ClaimedAt = nil
			job.Error = nil
			recovered++
		}
	}

	return recovered, nil
}

func (s *MemoryStorage) Close() error {
	return nil
}

func (s *MemoryStorage) Ping(ctx context.Context) error {
	return nil
}
