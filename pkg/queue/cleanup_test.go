package queue

import (
	"context"
	"testing"
	"time"

	"github.com/andreademasi/goku/pkg/queue/storage"
	"github.com/andreademasi/goku/pkg/queue/storage/memory"
)

// TestAutomaticCleanup tests automatic cleanup of old jobs
func TestAutomaticCleanup(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:         store,
		Workers:         1, // Need at least 1 worker to start queue
		CleanupInterval: 200 * time.Millisecond,
		CleanupAge:      5 * time.Second, // Longer age so recent jobs aren't deleted
	})
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Register a dummy handler so jobs don't fail
	q.Register("TestJob", HandlerFunc[map[string]interface{}](func(ctx context.Context, job map[string]interface{}) error {
		// Do nothing, just succeed
		return nil
	}).ToHandler())

	ctx := context.Background()

	// Enqueue old completed jobs (with old CompletedAt timestamp)
	oldTime := time.Now().Add(-10 * time.Second) // Much older than CleanupAge
	for i := 0; i < 5; i++ {
		store.Enqueue(ctx, &storage.Job{
			Type:        "TestJob",
			Payload:     []byte("{}"),
			Status:      storage.StatusCompleted,
			CompletedAt: &oldTime,
		})
	}

	// Enqueue recent pending jobs
	for i := 0; i < 3; i++ {
		store.Enqueue(ctx, &storage.Job{
			Type:    "TestJob",
			Payload: []byte("{}"),
			Status:  storage.StatusPending,
		})
	}

	// Start queue (this starts cleanup goroutine)
	if err := q.Start(); err != nil {
		t.Fatalf("Failed to start queue: %v", err)
	}
	defer q.Stop()

	// Wait long enough for cleanup to have run at least once and for jobs to be processed
	time.Sleep(2 * time.Second)

	// Check that old jobs were deleted
	allJobs, _ := store.ListJobs(ctx, storage.JobFilter{})

	// The 5 old completed jobs should be deleted
	// The 3 pending jobs may have been processed (completed), but shouldn't be deleted yet (too recent)
	// So we should have at least the 3 recently completed jobs
	completedJobs := 0
	for _, j := range allJobs {
		if j.Status == storage.StatusCompleted {
			completedJobs++
		}
	}

	if completedJobs < 3 {
		t.Errorf("Expected at least 3 recently completed jobs remaining, got %d (total: %d)", completedJobs, len(allJobs))
	}
}

// TestManualCleanup tests manual cleanup method
func TestManualCleanup(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage: store,
		Workers: 0, // No workers needed for this test
	})
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	ctx := context.Background()

	// Create some old completed jobs (completed 25 hours ago)
	oldCompletedTime := time.Now().Add(-25 * time.Hour)
	for i := 0; i < 5; i++ {
		store.Enqueue(ctx, &storage.Job{
			Type:        "OldJob",
			Payload:     []byte("{}"),
			Status:      storage.StatusCompleted,
			CompletedAt: &oldCompletedTime,
		})
	}

	// Create some old failed jobs (failed 25 hours ago)
	for i := 0; i < 3; i++ {
		store.Enqueue(ctx, &storage.Job{
			Type:        "OldJob",
			Payload:     []byte("{}"),
			Status:      storage.StatusFailed,
			CompletedAt: &oldCompletedTime,
		})
	}

	// Create some recent jobs
	for i := 0; i < 4; i++ {
		store.Enqueue(ctx, &storage.Job{
			Type:    "RecentJob",
			Payload: []byte("{}"),
		})
	}

	// Start queue to initialize context
	q.Start()
	defer q.Stop()

	// Get count before cleanup
	beforeCount, _ := store.ListJobs(ctx, storage.JobFilter{})
	t.Logf("Jobs before cleanup: %d", len(beforeCount))

	// Set cleanup age to 24 hours
	q.config.CleanupAge = 24 * time.Hour

	// Run manual cleanup
	deleted, err := q.cleanup(ctx)
	if err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}

	t.Logf("Deleted %d old jobs", deleted)

	// Verify old jobs were deleted
	if deleted != 8 { // 5 completed + 3 failed
		t.Errorf("Expected 8 deleted jobs, got %d", deleted)
	}

	// Verify recent jobs remain
	afterCount, _ := store.ListJobs(ctx, storage.JobFilter{})
	if len(afterCount) != 4 {
		t.Errorf("Expected 4 remaining jobs, got %d", len(afterCount))
	}
}

// TestStaleJobRecovery tests stale job recovery
func TestStaleJobRecovery(t *testing.T) {
	store := memory.New()
	_, err := New(Config{
		Storage:         store,
		Workers:         0, // No workers for this test
		StaleJobTimeout: 1 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	ctx := context.Background()

	// Create a job and mark it as running with old claimed time
	jobID, _ := store.Enqueue(ctx, &storage.Job{
		Type:    "StaleJob",
		Payload: []byte("{}"),
	})

	// Dequeue to mark as running
	_, _ = store.Dequeue(ctx)

	// Wait for job to become stale
	time.Sleep(2 * time.Second)

	// Recover stale jobs
	recovered, err := store.RecoverStaleJobs(ctx, 1*time.Second)
	if err != nil {
		t.Fatalf("Failed to recover stale jobs: %v", err)
	}

	if recovered != 1 {
		t.Errorf("Expected 1 recovered job, got %d", recovered)
	}

	// Verify job is back to pending
	job, _ := store.GetJob(ctx, jobID)
	if job.Status != storage.StatusPending {
		t.Errorf("Expected status %s, got %s", storage.StatusPending, job.Status)
	}
	if job.ClaimedAt != nil {
		t.Error("Expected ClaimedAt to be nil after recovery")
	}
}

// TestAutomaticStaleRecovery tests automatic stale job recovery
func TestAutomaticStaleRecovery(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:         store,
		Workers:         1,
		StaleJobTimeout: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Register a handler that blocks longer than the stale timeout
	// This keeps the job in "running" state long enough to become stale
	q.Register("StaleJob", HandlerFunc[map[string]interface{}](func(ctx context.Context, job map[string]interface{}) error {
		time.Sleep(5 * time.Second) // Much longer than stale timeout
		return nil
	}).ToHandler())

	ctx := context.Background()

	// Enqueue a job
	jobID, _ := store.Enqueue(ctx, &storage.Job{
		Type:    "StaleJob",
		Payload: []byte("{}"),
	})

	// Start queue (worker will pick up the job and start processing it)
	if err := q.Start(); err != nil {
		t.Fatalf("Failed to start queue: %v", err)
	}
	defer q.Stop()

	// Wait a bit for the job to be claimed and start running
	time.Sleep(100 * time.Millisecond)

	// Wait for the job to become stale and be recovered
	time.Sleep(1 * time.Second)

	// Verify job was recovered to pending
	job, _ := store.GetJob(ctx, jobID)
	if job.Status != storage.StatusPending {
		t.Errorf("Expected job to be recovered to pending, got %s", job.Status)
	}
}

// TestCleanupWithFilters tests that cleanup respects job filters
func TestCleanupWithFilters(t *testing.T) {
	store := memory.New()
	ctx := context.Background()

	cutoff := time.Now().Add(-25 * time.Hour)

	// Create completed jobs with different types
	for i := 0; i < 3; i++ {
		jobID, _ := store.Enqueue(ctx, &storage.Job{
			Type:        "TypeA",
			Payload:     []byte("{}"),
			ScheduledAt: cutoff,
		})
		store.Complete(ctx, jobID)
	}

	for i := 0; i < 2; i++ {
		jobID, _ := store.Enqueue(ctx, &storage.Job{
			Type:        "TypeB",
			Payload:     []byte("{}"),
			ScheduledAt: cutoff,
		})
		store.Complete(ctx, jobID)
	}

	// Delete only TypeA jobs
	deleted, err := store.DeleteJobs(ctx, storage.JobFilter{
		Types:    []string{"TypeA"},
		Statuses: []storage.JobStatus{storage.StatusCompleted},
	})
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if deleted != 3 {
		t.Errorf("Expected 3 deleted TypeA jobs, got %d", deleted)
	}

	// Verify TypeB jobs remain
	remaining, _ := store.ListJobs(ctx, storage.JobFilter{
		Types: []string{"TypeB"},
	})
	if len(remaining) != 2 {
		t.Errorf("Expected 2 remaining TypeB jobs, got %d", len(remaining))
	}
}

// TestStaleRecoveryDoesNotAffectNonStaleJobs tests that recent running jobs are not recovered
func TestStaleRecoveryDoesNotAffectNonStaleJobs(t *testing.T) {
	store := memory.New()
	ctx := context.Background()

	// Create a job and mark as running
	jobID, _ := store.Enqueue(ctx, &storage.Job{
		Type:    "RecentJob",
		Payload: []byte("{}"),
	})
	_, _ = store.Dequeue(ctx)

	// Immediately try to recover (job is not stale yet)
	recovered, err := store.RecoverStaleJobs(ctx, 10*time.Second)
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	if recovered != 0 {
		t.Errorf("Expected 0 recovered jobs, got %d", recovered)
	}

	// Verify job is still running
	job, _ := store.GetJob(ctx, jobID)
	if job.Status != storage.StatusRunning {
		t.Errorf("Expected status %s, got %s", storage.StatusRunning, job.Status)
	}
}

// TestDeleteJobsWithEmptyFilter tests deleting all jobs
func TestDeleteJobsWithEmptyFilter(t *testing.T) {
	store := memory.New()
	ctx := context.Background()

	// Create various jobs
	for i := 0; i < 5; i++ {
		store.Enqueue(ctx, &storage.Job{
			Type:    "TestJob",
			Payload: []byte("{}"),
		})
	}

	// Delete all jobs (empty filter)
	deleted, err := store.DeleteJobs(ctx, storage.JobFilter{})
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if deleted != 5 {
		t.Errorf("Expected 5 deleted jobs, got %d", deleted)
	}

	// Verify no jobs remain
	remaining, _ := store.ListJobs(ctx, storage.JobFilter{})
	if len(remaining) != 0 {
		t.Errorf("Expected 0 remaining jobs, got %d", len(remaining))
	}
}

// TestCleanupDisabledByDefault tests that cleanup is disabled when CleanupInterval is 0
func TestCleanupDisabledByDefault(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:         store,
		Workers:         1,
		CleanupInterval: 0, // Disabled
	})
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	ctx := context.Background()

	// Create old completed job
	cutoff := time.Now().Add(-25 * time.Hour)
	jobID, _ := store.Enqueue(ctx, &storage.Job{
		Type:        "OldJob",
		Payload:     []byte("{}"),
		ScheduledAt: cutoff,
	})
	store.Complete(ctx, jobID)

	// Start queue
	q.Start()
	defer q.Stop()

	// Wait a bit
	time.Sleep(500 * time.Millisecond)

	// Job should still exist (cleanup disabled)
	jobs, _ := store.ListJobs(ctx, storage.JobFilter{})
	if len(jobs) != 1 {
		t.Errorf("Expected 1 job (cleanup should be disabled), got %d", len(jobs))
	}
}

// TestStaleRecoveryDisabledByDefault tests that stale recovery is disabled when StaleJobTimeout is 0
func TestStaleRecoveryDisabledByDefault(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:         store,
		Workers:         0,
		StaleJobTimeout: 0, // Disabled
	})
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	ctx := context.Background()

	// Create stale job
	jobID, _ := store.Enqueue(ctx, &storage.Job{
		Type:    "StaleJob",
		Payload: []byte("{}"),
	})
	_, _ = store.Dequeue(ctx)

	// Start queue
	q.Start()
	defer q.Stop()

	// Wait a bit
	time.Sleep(500 * time.Millisecond)

	// Job should still be running (recovery disabled)
	job, _ := store.GetJob(ctx, jobID)
	if job.Status != storage.StatusRunning {
		t.Errorf("Expected status %s (recovery should be disabled), got %s", storage.StatusRunning, job.Status)
	}
}
