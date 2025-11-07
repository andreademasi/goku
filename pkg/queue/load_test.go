package queue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/andreademasi/goku/pkg/queue/storage"
	"github.com/andreademasi/goku/pkg/queue/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHighThroughput tests queue with high job throughput
func TestHighThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}

	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      10,
		PollInterval: 10 * time.Millisecond,
	})
	require.NoError(t, err)

	var processed atomic.Int64
	var failed atomic.Int64

	q.Register("ThroughputJob", HandlerFunc[int](func(ctx context.Context, id int) error {
		processed.Add(1)
		return nil
	}).ToHandler())

	q.Start()
	defer q.Stop()

	ctx := context.Background()
	totalJobs := 10000

	// Enqueue many jobs quickly
	start := time.Now()
	for i := 0; i < totalJobs; i++ {
		_, err := q.Enqueue(ctx, "ThroughputJob", i)
		if err != nil {
			failed.Add(1)
		}
	}
	enqueueTime := time.Since(start)

	t.Logf("Enqueued %d jobs in %v (%.0f jobs/sec)",
		totalJobs, enqueueTime, float64(totalJobs)/enqueueTime.Seconds())

	// Wait for processing
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if processed.Load() == int64(totalJobs) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	processTime := time.Since(start)
	t.Logf("Processed %d jobs in %v (%.0f jobs/sec)",
		processed.Load(), processTime, float64(processed.Load())/processTime.Seconds())

	assert.Equal(t, int64(0), failed.Load(), "no enqueue failures")
	assert.Equal(t, int64(totalJobs), processed.Load(), "all jobs should be processed")
}

// TestSustainedLoad tests queue under sustained load
func TestSustainedLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}

	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      5,
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	var processed atomic.Int64

	q.Register("SustainedJob", HandlerFunc[int](func(ctx context.Context, id int) error {
		time.Sleep(10 * time.Millisecond) // Simulate work
		processed.Add(1)
		return nil
	}).ToHandler())

	q.Start()
	defer q.Stop()

	// Run for 10 seconds, enqueueing continuously
	duration := 10 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var enqueued atomic.Int64
	var wg sync.WaitGroup

	// Producer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		id := 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
				_, err := q.Enqueue(context.Background(), "SustainedJob", id)
				if err == nil {
					enqueued.Add(1)
				}
				id++
				time.Sleep(5 * time.Millisecond) // ~200 jobs/sec
			}
		}
	}()

	wg.Wait()

	// Give queue time to finish remaining jobs
	time.Sleep(5 * time.Second)

	t.Logf("Sustained load: enqueued %d jobs, processed %d jobs over %v",
		enqueued.Load(), processed.Load(), duration)

	// Most jobs should be processed (allow some slack for timing)
	assert.GreaterOrEqual(t, processed.Load(), int64(float64(enqueued.Load())*0.8),
		"should process at least 80%% of jobs")
}

// TestConcurrentEnqueueers tests multiple goroutines enqueuing simultaneously
func TestConcurrentEnqueueers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}

	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      5,
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	var processed atomic.Int64

	q.Register("ConcurrentJob", HandlerFunc[int](func(ctx context.Context, id int) error {
		processed.Add(1)
		return nil
	}).ToHandler())

	q.Start()
	defer q.Stop()

	ctx := context.Background()

	// Many concurrent enqueuers
	const numEnqueuers = 20
	const jobsPerEnqueuer = 100
	totalJobs := numEnqueuers * jobsPerEnqueuer

	var wg sync.WaitGroup
	var enqueueErrors atomic.Int64

	for i := 0; i < numEnqueuers; i++ {
		wg.Add(1)
		go func(enqueuerID int) {
			defer wg.Done()
			for j := 0; j < jobsPerEnqueuer; j++ {
				jobID := enqueuerID*jobsPerEnqueuer + j
				_, err := q.Enqueue(ctx, "ConcurrentJob", jobID)
				if err != nil {
					enqueueErrors.Add(1)
				}
			}
		}(i)
	}

	wg.Wait()

	// Wait for processing
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if processed.Load() == int64(totalJobs) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Logf("Concurrent enqueuers: %d enqueuers × %d jobs = %d total jobs, %d processed, %d errors",
		numEnqueuers, jobsPerEnqueuer, totalJobs, processed.Load(), enqueueErrors.Load())

	assert.Equal(t, int64(0), enqueueErrors.Load(), "no enqueue errors")
	assert.Equal(t, int64(totalJobs), processed.Load(), "all jobs should be processed")
}

// TestMemoryUnderLoad tests memory usage under load
func TestMemoryUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}

	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      10,
		PollInterval: 10 * time.Millisecond,
	})
	require.NoError(t, err)

	var processed atomic.Int64

	q.Register("MemoryJob", HandlerFunc[[]byte](func(ctx context.Context, data []byte) error {
		// Simulate some work
		_ = len(data)
		processed.Add(1)
		return nil
	}).ToHandler())

	q.Start()
	defer q.Stop()

	ctx := context.Background()

	// Enqueue jobs with medium-sized payloads
	const numJobs = 5000
	payload := make([]byte, 10*1024) // 10KB payload

	for i := 0; i < numJobs; i++ {
		_, err := q.Enqueue(ctx, "MemoryJob", payload)
		require.NoError(t, err)
	}

	// Wait for processing
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if processed.Load() == numJobs {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	assert.Equal(t, int64(numJobs), processed.Load(), "all jobs should be processed")
}

// TestBurstLoad tests queue handling of burst traffic
func TestBurstLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}

	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      5,
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	var processed atomic.Int64

	q.Register("BurstJob", HandlerFunc[int](func(ctx context.Context, id int) error {
		time.Sleep(50 * time.Millisecond)
		processed.Add(1)
		return nil
	}).ToHandler())

	q.Start()
	defer q.Stop()

	ctx := context.Background()

	// Simulate burst traffic: quick burst, then idle, then burst again
	bursts := []int{100, 0, 0, 200, 0, 0, 150}
	totalJobs := 0

	for burst := 0; burst < len(bursts); burst++ {
		jobsInBurst := bursts[burst]
		totalJobs += jobsInBurst

		// Enqueue burst
		for i := 0; i < jobsInBurst; i++ {
			_, err := q.Enqueue(ctx, "BurstJob", i)
			require.NoError(t, err)
		}

		t.Logf("Burst %d: enqueued %d jobs, total processed so far: %d",
			burst, jobsInBurst, processed.Load())

		// Wait 1 second between bursts
		time.Sleep(1 * time.Second)
	}

	// Wait for all jobs to finish
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if processed.Load() == int64(totalJobs) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	assert.Equal(t, int64(totalJobs), processed.Load(), "all burst jobs should be processed")
}

// TestPriorityUnderLoad tests priority ordering under heavy load
func TestPriorityUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}

	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      2, // Few workers to see priority effect
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	var mu sync.Mutex
	var processOrder []int

	q.Register("PriorityJob", HandlerFunc[int](func(ctx context.Context, priority int) error {
		mu.Lock()
		processOrder = append(processOrder, priority)
		mu.Unlock()
		time.Sleep(100 * time.Millisecond)
		return nil
	}).ToHandler())

	ctx := context.Background()

	// Enqueue many jobs with different priorities
	priorities := []int{1, 10, 100, 5, 50, 1, 10, 100, 5, 50}
	for _, p := range priorities {
		_, err := q.EnqueueWithOptions(ctx, "PriorityJob", p, EnqueueOptions{
			Priority: p,
		})
		require.NoError(t, err)
	}

	// Start processing after all jobs are enqueued
	q.Start()
	defer q.Stop()

	// Wait for all to process
	time.Sleep(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	t.Logf("Process order: %v", processOrder)

	// Verify high priority jobs were processed first
	// First few jobs should be high priority (100, 100)
	if len(processOrder) >= 2 {
		assert.GreaterOrEqual(t, processOrder[0], 50,
			"first job should be high priority")
	}
}

// TestRetryStorm tests queue with many failing and retrying jobs
func TestRetryStorm(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}

	store := memory.New()
	q, err := New(Config{
		Storage:       store,
		Workers:       5,
		PollInterval:  50 * time.Millisecond,
		RetryStrategy: ConstantBackoff{Delay: 100 * time.Millisecond},
	})
	require.NoError(t, err)

	var attempts atomic.Int64
	var succeeded atomic.Int64

	q.Register("RetryJob", HandlerFunc[int](func(ctx context.Context, id int) error {
		count := attempts.Add(1)
		// Fail first 2 attempts, succeed on 3rd
		if count%3 != 0 {
			return fmt.Errorf("attempt %d failed", count)
		}
		succeeded.Add(1)
		return nil
	}).ToHandler())

	q.Start()
	defer q.Stop()

	ctx := context.Background()

	// Enqueue jobs that will retry
	const numJobs = 50
	for i := 0; i < numJobs; i++ {
		_, err := q.EnqueueWithOptions(ctx, "RetryJob", i, EnqueueOptions{
			MaxRetries: 5,
		})
		require.NoError(t, err)
	}

	// Wait for retries to complete
	time.Sleep(15 * time.Second)

	t.Logf("Retry storm: %d attempts for %d jobs, %d succeeded",
		attempts.Load(), numJobs, succeeded.Load())

	// All jobs should eventually succeed
	assert.GreaterOrEqual(t, succeeded.Load(), int64(numJobs*0.8),
		"most jobs should succeed after retries")
}

// TestWorkerPoolScaling tests queue behavior with different worker counts
func TestWorkerPoolScaling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}

	testCases := []struct {
		workers  int
		jobs     int
		expected time.Duration
	}{
		{1, 10, 1000 * time.Millisecond}, // 1 worker, sequential
		{5, 10, 300 * time.Millisecond},  // 5 workers, some parallelism
		{10, 10, 200 * time.Millisecond}, // 10 workers, high parallelism
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%dWorkers", tc.workers), func(t *testing.T) {
			store := memory.New()
			q, err := New(Config{
				Storage:      store,
				Workers:      tc.workers,
				PollInterval: 10 * time.Millisecond,
			})
			require.NoError(t, err)

			var processed atomic.Int64

			q.Register("ScaleJob", HandlerFunc[int](func(ctx context.Context, id int) error {
				time.Sleep(100 * time.Millisecond)
				processed.Add(1)
				return nil
			}).ToHandler())

			q.Start()
			defer q.Stop()

			ctx := context.Background()

			// Enqueue jobs
			start := time.Now()
			for i := 0; i < tc.jobs; i++ {
				_, err := q.Enqueue(ctx, "ScaleJob", i)
				require.NoError(t, err)
			}

			// Wait for completion
			deadline := time.Now().Add(30 * time.Second)
			for time.Now().Before(deadline) {
				if processed.Load() == int64(tc.jobs) {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}

			duration := time.Since(start)
			t.Logf("%d workers processed %d jobs in %v", tc.workers, tc.jobs, duration)

			assert.Equal(t, int64(tc.jobs), processed.Load())

			// More workers should complete faster (with some tolerance)
			if tc.workers > 1 {
				assert.Less(t, duration, tc.expected*2,
					"more workers should complete faster")
			}
		})
	}
}

// TestLongRunningJobs tests queue with jobs that take a long time
func TestLongRunningJobs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}

	store := memory.New()
	q, err := New(Config{
		Storage:         store,
		Workers:         3,
		PollInterval:    50 * time.Millisecond,
		ShutdownTimeout: 10 * time.Second,
	})
	require.NoError(t, err)

	var completed atomic.Int64

	q.Register("LongJob", HandlerFunc[int](func(ctx context.Context, duration int) error {
		select {
		case <-time.After(time.Duration(duration) * time.Millisecond):
			completed.Add(1)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}).ToHandler())

	q.Start()

	ctx := context.Background()

	// Enqueue jobs with varying durations
	durations := []int{1000, 2000, 500, 1500, 3000}
	for _, d := range durations {
		_, err := q.Enqueue(ctx, "LongJob", d)
		require.NoError(t, err)
	}

	// Wait for completion
	time.Sleep(15 * time.Second)

	// Stop and verify graceful handling
	err = q.Stop()
	assert.NoError(t, err)

	t.Logf("Long-running jobs: %d/%d completed", completed.Load(), len(durations))
	assert.GreaterOrEqual(t, completed.Load(), int64(3),
		"most long jobs should complete")
}

// TestQueueDepth tests monitoring queue depth under load
func TestQueueDepth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}

	store := memory.New()
	q, err := New(Config{
		Storage:      store,
		Workers:      2, // Few workers to build up queue
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	q.Register("DepthJob", HandlerFunc[int](func(ctx context.Context, id int) error {
		time.Sleep(200 * time.Millisecond)
		return nil
	}).ToHandler())

	ctx := context.Background()

	// Enqueue many jobs quickly
	for i := 0; i < 50; i++ {
		_, err := q.Enqueue(ctx, "DepthJob", i)
		require.NoError(t, err)
	}

	// Start processing
	q.Start()
	defer q.Stop()

	// Monitor queue depth
	maxDepth := int64(0)
	for i := 0; i < 20; i++ {
		jobs, err := store.ListJobs(ctx, storage.JobFilter{
			Statuses: []storage.JobStatus{storage.StatusPending, storage.StatusRunning},
		})
		if err == nil {
			depth := int64(len(jobs))
			if depth > maxDepth {
				maxDepth = depth
			}
			t.Logf("Queue depth at %ds: %d", i, depth)
		}
		time.Sleep(500 * time.Millisecond)
	}

	t.Logf("Max queue depth: %d", maxDepth)
	assert.Greater(t, maxDepth, int64(0), "queue should have had jobs")
}

// BenchmarkEnqueueThroughput benchmarks enqueue throughput
func BenchmarkEnqueueThroughput(b *testing.B) {
	store := memory.New()
	q, _ := New(Config{
		Storage: store,
		Workers: 0, // No workers, just enqueue
	})

	ctx := context.Background()
	payload := map[string]string{"test": "data"}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := q.Enqueue(ctx, "BenchJob", payload)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "enqueues/sec")
}

// BenchmarkProcessThroughput benchmarks end-to-end processing throughput
func BenchmarkProcessThroughput(b *testing.B) {
	store := memory.New()
	q, _ := New(Config{
		Storage:      store,
		Workers:      10,
		PollInterval: 1 * time.Millisecond,
	})

	var processed atomic.Int64

	q.Register("BenchJob", HandlerFunc[map[string]string](func(ctx context.Context, data map[string]string) error {
		processed.Add(1)
		return nil
	}).ToHandler())

	q.Start()
	defer q.Stop()

	ctx := context.Background()
	payload := map[string]string{"test": "data"}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := q.Enqueue(ctx, "BenchJob", payload)
		if err != nil {
			b.Fatal(err)
		}
	}

	// Wait for all to be processed
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if processed.Load() == int64(b.N) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	b.StopTimer()

	if processed.Load() != int64(b.N) {
		b.Fatalf("Not all jobs processed: %d/%d", processed.Load(), b.N)
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "jobs/sec")
}

