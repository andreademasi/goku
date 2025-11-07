package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/andreademasi/goku/pkg/queue/storage"
	"github.com/andreademasi/goku/pkg/queue/storage/memory"
)

// TestEmailJob is a test job type
type TestEmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

// TestNew validates queue creation and configuration
func TestNew(t *testing.T) {
	store := memory.New()

	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: Config{
				Storage: store,
				Workers: 3,
			},
			wantErr: false,
		},
		{
			name: "nil storage",
			config: Config{
				Storage: nil,
				Workers: 3,
			},
			wantErr: true,
		},
		{
			name: "defaults applied",
			config: Config{
				Storage: store,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := New(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && q == nil {
				t.Error("New() returned nil queue")
			}
		})
	}
}

// TestQueueEnqueueAndProcess tests basic enqueue and processing
func TestQueueEnqueueAndProcess(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      2,
		PollInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Track processed jobs
	var processed atomic.Int32
	var mu sync.Mutex
	processedJobs := make(map[string]TestEmailJob)

	// Register handler
	err = q.Register("SendEmail", HandlerFunc[TestEmailJob](func(ctx context.Context, job TestEmailJob) error {
		mu.Lock()
		processedJobs[job.To] = job
		mu.Unlock()
		processed.Add(1)
		return nil
	}).ToHandler())
	if err != nil {
		t.Fatalf("Failed to register handler: %v", err)
	}

	// Start queue
	if err := q.Start(); err != nil {
		t.Fatalf("Failed to start queue: %v", err)
	}
	defer q.Stop()

	// Enqueue jobs
	ctx := context.Background()
	jobs := []TestEmailJob{
		{To: "alice@example.com", Subject: "Hello Alice", Body: "Test 1"},
		{To: "bob@example.com", Subject: "Hello Bob", Body: "Test 2"},
		{To: "charlie@example.com", Subject: "Hello Charlie", Body: "Test 3"},
	}

	for _, job := range jobs {
		_, err := q.Enqueue(ctx, "SendEmail", job)
		if err != nil {
			t.Fatalf("Failed to enqueue job: %v", err)
		}
	}

	// Wait for processing
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if processed.Load() == int32(len(jobs)) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Verify all jobs were processed
	if processed.Load() != int32(len(jobs)) {
		t.Errorf("Expected %d jobs processed, got %d", len(jobs), processed.Load())
	}

	// Verify job data
	mu.Lock()
	defer mu.Unlock()
	for _, expected := range jobs {
		actual, exists := processedJobs[expected.To]
		if !exists {
			t.Errorf("Job for %s was not processed", expected.To)
			continue
		}
		if actual.Subject != expected.Subject {
			t.Errorf("Subject mismatch: got %s, want %s", actual.Subject, expected.Subject)
		}
	}
}

// TestQueueRetry tests job retry logic
func TestQueueRetry(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:       store,
		Workers:       1,
		PollInterval:  50 * time.Millisecond,
		RetryStrategy: ConstantBackoff{Delay: 100 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Track attempts
	var attempts atomic.Int32
	testErr := errors.New("temporary failure")

	// Register handler that fails twice, then succeeds
	err = q.Register("FlakeyJob", HandlerFunc[TestEmailJob](func(ctx context.Context, job TestEmailJob) error {
		count := attempts.Add(1)
		if count <= 2 {
			return testErr
		}
		return nil
	}).ToHandler())
	if err != nil {
		t.Fatalf("Failed to register handler: %v", err)
	}

	// Start queue
	if err := q.Start(); err != nil {
		t.Fatalf("Failed to start queue: %v", err)
	}
	defer q.Stop()

	// Enqueue job with retries
	ctx := context.Background()
	jobID, err := q.EnqueueWithOptions(ctx, "FlakeyJob", TestEmailJob{
		To: "retry@example.com",
	}, EnqueueOptions{
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("Failed to enqueue job: %v", err)
	}

	// Wait for retries
	time.Sleep(2 * time.Second)

	// Verify job eventually succeeded
	if attempts.Load() < 3 {
		t.Errorf("Expected at least 3 attempts, got %d", attempts.Load())
	}

	// Verify job is completed
	job, err := store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("Failed to get job: %v", err)
	}

	if job.Status != storage.StatusCompleted {
		t.Errorf("Expected job status %s, got %s", storage.StatusCompleted, job.Status)
	}
}

// TestQueuePriority tests priority ordering
func TestQueuePriority(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      1, // Single worker to ensure order
		PollInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Track processing order
	var mu sync.Mutex
	var processOrder []int

	// Register handler
	err = q.Register("PriorityJob", HandlerFunc[int](func(ctx context.Context, priority int) error {
		mu.Lock()
		processOrder = append(processOrder, priority)
		mu.Unlock()
		time.Sleep(100 * time.Millisecond) // Ensure sequential processing
		return nil
	}).ToHandler())
	if err != nil {
		t.Fatalf("Failed to register handler: %v", err)
	}

	ctx := context.Background()

	// Enqueue jobs with different priorities (out of order)
	priorities := []int{1, 100, 5, 50, 10}
	for _, p := range priorities {
		_, err := q.EnqueueWithOptions(ctx, "PriorityJob", p, EnqueueOptions{
			Priority: p,
		})
		if err != nil {
			t.Fatalf("Failed to enqueue job: %v", err)
		}
	}

	// Start processing after all jobs are enqueued
	if err := q.Start(); err != nil {
		t.Fatalf("Failed to start queue: %v", err)
	}

	// Wait for processing
	time.Sleep(3 * time.Second)
	q.Stop()

	// Verify processing order (highest priority first)
	mu.Lock()
	defer mu.Unlock()

	if len(processOrder) != len(priorities) {
		t.Fatalf("Expected %d jobs processed, got %d", len(priorities), len(processOrder))
	}

	// First should be highest priority (100)
	if processOrder[0] != 100 {
		t.Errorf("Expected first job priority 100, got %d", processOrder[0])
	}

	// Verify descending order
	for i := 0; i < len(processOrder)-1; i++ {
		if processOrder[i] < processOrder[i+1] {
			t.Errorf("Jobs not processed in priority order: %v", processOrder)
			break
		}
	}
}

// TestQueueGracefulShutdown tests graceful shutdown
func TestQueueGracefulShutdown(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:         store,
		Workers:         2,
		PollInterval:    50 * time.Millisecond,
		ShutdownTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Track when jobs start and finish
	var started, finished atomic.Int32

	// Register long-running handler
	err = q.Register("LongJob", HandlerFunc[time.Duration](func(ctx context.Context, duration time.Duration) error {
		started.Add(1)
		time.Sleep(duration)
		finished.Add(1)
		return nil
	}).ToHandler())
	if err != nil {
		t.Fatalf("Failed to register handler: %v", err)
	}

	// Start queue
	if err := q.Start(); err != nil {
		t.Fatalf("Failed to start queue: %v", err)
	}

	// Enqueue jobs
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		_, err := q.Enqueue(ctx, "LongJob", 500*time.Millisecond)
		if err != nil {
			t.Fatalf("Failed to enqueue job: %v", err)
		}
	}

	// Wait for jobs to start
	time.Sleep(200 * time.Millisecond)

	// Stop queue (should wait for current jobs to finish)
	stopStart := time.Now()
	if err := q.Stop(); err != nil {
		t.Fatalf("Failed to stop queue: %v", err)
	}
	stopDuration := time.Since(stopStart)

	// Verify jobs that started also finished
	if started.Load() != finished.Load() {
		t.Errorf("Started %d jobs but only finished %d", started.Load(), finished.Load())
	}

	// Verify stop waited for jobs (but not too long)
	if stopDuration < 100*time.Millisecond {
		t.Error("Stop returned too quickly, didn't wait for jobs")
	}
	if stopDuration > 3*time.Second {
		t.Error("Stop took too long, should have finished jobs quickly")
	}
}

// TestQueueConcurrentWorkers tests multiple workers processing jobs
func TestQueueConcurrentWorkers(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      5,
		PollInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Track concurrent executions
	var active, maxActive atomic.Int32
	var mu sync.Mutex

	// Register handler that tracks concurrency
	err = q.Register("ConcurrentJob", HandlerFunc[int](func(ctx context.Context, id int) error {
		current := active.Add(1)
		defer active.Add(-1)

		// Update max
		mu.Lock()
		if current > maxActive.Load() {
			maxActive.Store(current)
		}
		mu.Unlock()

		time.Sleep(200 * time.Millisecond)
		return nil
	}).ToHandler())
	if err != nil {
		t.Fatalf("Failed to register handler: %v", err)
	}

	// Start queue
	if err := q.Start(); err != nil {
		t.Fatalf("Failed to start queue: %v", err)
	}
	defer q.Stop()

	// Enqueue many jobs
	ctx := context.Background()
	jobCount := 20
	for i := 0; i < jobCount; i++ {
		_, err := q.Enqueue(ctx, "ConcurrentJob", i)
		if err != nil {
			t.Fatalf("Failed to enqueue job: %v", err)
		}
	}

	// Wait for all jobs to process
	time.Sleep(3 * time.Second)

	// Verify we had concurrent execution
	if maxActive.Load() < 2 {
		t.Errorf("Expected concurrent execution, max active was %d", maxActive.Load())
	}

	// Should not exceed worker count
	if maxActive.Load() > 5 {
		t.Errorf("Exceeded worker count: max active was %d, workers is 5", maxActive.Load())
	}
}

// TestHandlerNotFound tests missing handler error
func TestHandlerNotFound(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      1,
		PollInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Don't register any handlers
	if err := q.Start(); err != nil {
		t.Fatalf("Failed to start queue: %v", err)
	}
	defer q.Stop()

	// Enqueue job with no handler
	ctx := context.Background()
	jobID, err := q.Enqueue(ctx, "UnknownJobType", map[string]string{"foo": "bar"})
	if err != nil {
		t.Fatalf("Failed to enqueue job: %v", err)
	}

	// Wait for processing attempt
	time.Sleep(500 * time.Millisecond)

	// Job should be failed due to missing handler
	job, err := store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("Failed to get job: %v", err)
	}

	if job.Status != storage.StatusFailed {
		t.Errorf("Expected job status %s, got %s", storage.StatusFailed, job.Status)
	}

	if job.Error == nil || *job.Error == "" {
		t.Error("Expected error message about missing handler")
	}
}

// TestDoubleStart tests that Start is idempotent
func TestDoubleStart(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage: store,
		Workers: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// First start should succeed
	if err := q.Start(); err != nil {
		t.Fatalf("First Start() failed: %v", err)
	}

	// Second start should be no-op (no error)
	if err := q.Start(); err != nil {
		t.Errorf("Second Start() should be no-op, got error: %v", err)
	}

	q.Stop()
}

// TestStopBeforeStart tests stopping before starting
func TestStopBeforeStart(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage: store,
		Workers: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Stop without start should error
	err = q.Stop()
	if err == nil {
		t.Error("Expected error when stopping queue that was never started")
	}
	if !errors.Is(err, ErrQueueNotStarted) {
		t.Errorf("Expected ErrQueueNotStarted, got: %v", err)
	}
}

// TestIsRunning tests the IsRunning method
func TestIsRunning(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage: store,
		Workers: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Initially not running
	if q.IsRunning() {
		t.Error("Queue should not be running before Start()")
	}

	// Start queue
	if err := q.Start(); err != nil {
		t.Fatalf("Failed to start queue: %v", err)
	}

	// Should be running
	if !q.IsRunning() {
		t.Error("Queue should be running after Start()")
	}

	// Stop queue
	if err := q.Stop(); err != nil {
		t.Fatalf("Failed to stop queue: %v", err)
	}

	// Should not be running
	if q.IsRunning() {
		t.Error("Queue should not be running after Stop()")
	}
}

// TestRegisteredTypes tests the RegisteredTypes method
func TestRegisteredTypes(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage: store,
		Workers: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Initially empty
	if len(q.RegisteredTypes()) != 0 {
		t.Error("Expected no registered types initially")
	}

	// Register some handlers
	q.Register("Job1", HandlerFunc[string](func(ctx context.Context, data string) error { return nil }).ToHandler())
	q.Register("Job2", HandlerFunc[int](func(ctx context.Context, data int) error { return nil }).ToHandler())
	q.Register("Job3", HandlerFunc[bool](func(ctx context.Context, data bool) error { return nil }).ToHandler())

	types := q.RegisteredTypes()
	if len(types) != 3 {
		t.Errorf("Expected 3 registered types, got %d", len(types))
	}

	// Check all types are present
	typeSet := make(map[string]bool)
	for _, t := range types {
		typeSet[t] = true
	}

	for _, expected := range []string{"Job1", "Job2", "Job3"} {
		if !typeSet[expected] {
			t.Errorf("Expected type %s to be registered", expected)
		}
	}
}

// TestDuplicateRegistration tests that duplicate handler registration fails
func TestDuplicateRegistration(t *testing.T) {
	store := memory.New()
	q, err := New(Config{
		Storage: store,
		Workers: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Register first handler
	err = q.Register("DupeJob", HandlerFunc[string](func(ctx context.Context, data string) error {
		return nil
	}).ToHandler())
	if err != nil {
		t.Fatalf("First registration failed: %v", err)
	}

	// Try to register again
	err = q.Register("DupeJob", HandlerFunc[string](func(ctx context.Context, data string) error {
		return nil
	}).ToHandler())
	if err == nil {
		t.Error("Expected error when registering duplicate handler")
	}
	if !errors.Is(err, ErrHandlerAlreadyRegistered) {
		t.Errorf("Expected ErrHandlerAlreadyRegistered, got: %v", err)
	}
}

// Benchmark for enqueue performance
func BenchmarkEnqueue(b *testing.B) {
	store := memory.New()
	q, err := New(Config{
		Storage: store,
		Workers: 0, // Don't start workers
	})
	if err != nil {
		b.Fatalf("Failed to create queue: %v", err)
	}

	ctx := context.Background()
	job := TestEmailJob{To: "bench@example.com", Subject: "Benchmark"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := q.Enqueue(ctx, "BenchJob", job)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark for end-to-end processing
func BenchmarkProcessing(b *testing.B) {
	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      4,
		PollInterval: 10 * time.Millisecond,
	})
	if err != nil {
		b.Fatalf("Failed to create queue: %v", err)
	}

	// Simple handler
	var processed atomic.Int32
	q.Register("BenchJob", HandlerFunc[TestEmailJob](func(ctx context.Context, job TestEmailJob) error {
		processed.Add(1)
		return nil
	}).ToHandler())

	// Start queue
	if err := q.Start(); err != nil {
		b.Fatalf("Failed to start queue: %v", err)
	}
	defer q.Stop()

	ctx := context.Background()
	job := TestEmailJob{To: "bench@example.com", Subject: "Benchmark"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := q.Enqueue(ctx, "BenchJob", job)
		if err != nil {
			b.Fatal(err)
		}
	}

	// Wait for all jobs to be processed
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if processed.Load() == int32(b.N) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if processed.Load() != int32(b.N) {
		b.Fatalf("Not all jobs processed: %d/%d", processed.Load(), b.N)
	}
}

// Example test demonstrating usage
func ExampleQueue() {
	// Create storage backend
	store := memory.New()

	// Create queue
	q, _ := New(Config{
		Storage: store,
		Workers: 3,
	})

	// Register handler
	q.Register("SendEmail", HandlerFunc[TestEmailJob](func(ctx context.Context, job TestEmailJob) error {
		fmt.Printf("Sending email to %s\n", job.To)
		return nil
	}).ToHandler())

	// Start processing
	q.Start()
	defer q.Stop()

	// Enqueue a job
	ctx := context.Background()
	jobID, _ := q.Enqueue(ctx, "SendEmail", TestEmailJob{
		To:      "user@example.com",
		Subject: "Hello!",
		Body:    "This is a test email",
	})

	fmt.Printf("Job enqueued: %s\n", jobID)

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Output:
	// Job enqueued: mem-1
	// Sending email to user@example.com
}
