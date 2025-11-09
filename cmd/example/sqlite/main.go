package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/andreademasi/goku/pkg/queue/storage"
	"github.com/andreademasi/goku/pkg/queue/storage/sqlite"
)

// EmailJob represents an email to be sent
type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func main() {
	fmt.Println("🥋 Goku SQLite Storage Example")
	fmt.Println("================================")
	fmt.Println()

	// Create SQLite storage
	dbPath := "example.db"
	store, err := sqlite.New(dbPath)
	if err != nil {
		log.Fatalf("Failed to create SQLite storage: %v", err)
	}
	defer store.Close()

	fmt.Printf("✅ Database created: %s\n", dbPath)

	// Initialize schema (safe to call multiple times)
	ctx := context.Background()
	if err := store.InitSchema(ctx); err != nil {
		log.Fatalf("Failed to initialize schema: %v", err)
	}

	fmt.Println("✅ Schema initialized")
	fmt.Println()

	// Example 1: Basic enqueue and dequeue
	fmt.Println("Example 1: Basic Enqueue/Dequeue")
	fmt.Println("---------------------------------")
	basicExample(ctx, store)

	// Example 2: Priority jobs
	fmt.Println("\nExample 2: Priority Jobs")
	fmt.Println("-------------------------")
	priorityExample(ctx, store)

	// Example 3: Scheduled jobs
	fmt.Println("\nExample 3: Scheduled Jobs")
	fmt.Println("-------------------------")
	scheduledExample(ctx, store)

	// Example 4: Transactions
	fmt.Println("\nExample 4: Transactions")
	fmt.Println("-----------------------")
	transactionExample(ctx, store)

	// Example 5: Concurrent workers
	fmt.Println("\nExample 5: Concurrent Workers")
	fmt.Println("------------------------------")
	concurrentExample(ctx, store)

	// Example 6: Job lifecycle
	fmt.Println("\nExample 6: Job Lifecycle (Retry)")
	fmt.Println("---------------------------------")
	lifecycleExample(ctx, store)

	// Statistics
	fmt.Println("\nDatabase Statistics")
	fmt.Println("-------------------")
	showStatistics(ctx, store)

	fmt.Println("\n✅ All examples completed!")
	fmt.Println("\nPress Ctrl+C to exit and clean up...")

	// Wait for interrupt
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	<-sigCh

	fmt.Println("\n🛑 Cleaning up...")

	// Optional: Delete the example database
	fmt.Printf("Database file: %s (%.2f KB)\n", dbPath, getFileSize(dbPath)/1024.0)
	fmt.Println("You can delete it manually or keep it for inspection")
	fmt.Println("✅ Goodbye!")
}

func basicExample(ctx context.Context, store *sqlite.SQLiteStorage) {
	// Create and enqueue a job
	emailData := EmailJob{
		To:      "alice@example.com",
		Subject: "Welcome!",
		Body:    "Welcome to Goku job queue",
	}

	payload, _ := json.Marshal(emailData)
	job := &storage.Job{
		Type:    "SendEmail",
		Payload: payload,
	}

	jobID, err := store.Enqueue(ctx, job)
	if err != nil {
		log.Fatalf("Failed to enqueue: %v", err)
	}

	fmt.Printf("📬 Enqueued job: %s\n", jobID)

	// Dequeue the job
	dequeuedJob, err := store.Dequeue(ctx)
	if err != nil {
		log.Fatalf("Failed to dequeue: %v", err)
	}

	if dequeuedJob != nil {
		var email EmailJob
		json.Unmarshal(dequeuedJob.Payload, &email)
		fmt.Printf("📧 Processing: Send email to %s\n", email.To)

		// Simulate work
		time.Sleep(100 * time.Millisecond)

		// Complete the job
		err = store.Complete(ctx, dequeuedJob.ID)
		if err != nil {
			log.Fatalf("Failed to complete: %v", err)
		}

		fmt.Printf("✅ Job %s completed\n", dequeuedJob.ID)
	}
}

func priorityExample(ctx context.Context, store *sqlite.SQLiteStorage) {
	// Enqueue jobs with different priorities
	jobs := []struct {
		email    EmailJob
		priority int
	}{
		{EmailJob{To: "user1@example.com", Subject: "Low Priority"}, 1},
		{EmailJob{To: "user2@example.com", Subject: "High Priority"}, 10},
		{EmailJob{To: "user3@example.com", Subject: "Medium Priority"}, 5},
		{EmailJob{To: "user4@example.com", Subject: "Urgent!"}, 100},
	}

	for _, j := range jobs {
		payload, _ := json.Marshal(j.email)
		job := &storage.Job{
			Type:     "SendEmail",
			Payload:  payload,
			Priority: j.priority,
		}

		jobID, _ := store.Enqueue(ctx, job)
		fmt.Printf("📬 Enqueued (priority %d): %s - %s\n", j.priority, jobID, j.email.Subject)
	}

	fmt.Println("\nProcessing in priority order (highest first):")

	// Dequeue and process in priority order
	for i := 0; i < 4; i++ {
		job, _ := store.Dequeue(ctx)
		if job != nil {
			var email EmailJob
			json.Unmarshal(job.Payload, &email)
			fmt.Printf("  %d. Priority %d: %s\n", i+1, job.Priority, email.Subject)
			store.Complete(ctx, job.ID)
		}
	}
}

func scheduledExample(ctx context.Context, store *sqlite.SQLiteStorage) {
	now := time.Now()

	// Schedule jobs for different times
	schedules := []struct {
		delay   time.Duration
		message string
	}{
		{0, "Immediate job"},
		{2 * time.Second, "Job scheduled for 2 seconds from now"},
		{5 * time.Second, "Job scheduled for 5 seconds from now"},
	}

	for _, s := range schedules {
		emailData := EmailJob{
			To:      "scheduler@example.com",
			Subject: s.message,
		}
		payload, _ := json.Marshal(emailData)

		job := &storage.Job{
			Type:        "SendEmail",
			Payload:     payload,
			ScheduledAt: now.Add(s.delay),
		}

		jobID, _ := store.Enqueue(ctx, job)
		if s.delay == 0 {
			fmt.Printf("📬 Enqueued immediate job: %s\n", jobID)
		} else {
			fmt.Printf("⏰ Scheduled job %s for %v from now\n", jobID, s.delay)
		}
	}

	// Try to dequeue - only immediate job should be available
	job, _ := store.Dequeue(ctx)
	if job != nil {
		var email EmailJob
		json.Unmarshal(job.Payload, &email)
		fmt.Printf("✅ Dequeued: %s\n", email.Subject)
		store.Complete(ctx, job.ID)
	}

	// Try again - should be nil (future jobs not ready)
	job, _ = store.Dequeue(ctx)
	if job == nil {
		fmt.Println("⏳ No jobs available yet (future jobs are scheduled)")
	}

	fmt.Println("   (In production, workers would poll and wait for scheduled jobs)")
}

func transactionExample(ctx context.Context, store *sqlite.SQLiteStorage) {
	// Example: Enqueue multiple jobs atomically
	tx, err := store.BeginTx(ctx)
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	fmt.Println("Starting transaction...")

	// Enqueue 3 jobs in a transaction
	for i := 1; i <= 3; i++ {
		emailData := EmailJob{
			To:      fmt.Sprintf("user%d@example.com", i),
			Subject: fmt.Sprintf("Batch email %d", i),
		}
		payload, _ := json.Marshal(emailData)

		job := &storage.Job{
			Type:    "SendEmail",
			Payload: payload,
		}

		jobID, err := tx.Enqueue(ctx, job)
		if err != nil {
			tx.Rollback()
			log.Fatalf("Failed to enqueue in transaction: %v", err)
		}

		fmt.Printf("  📬 Added to transaction: %s\n", jobID)
	}

	// Commit all jobs at once
	if err := tx.Commit(); err != nil {
		log.Fatalf("Failed to commit transaction: %v", err)
	}

	fmt.Println("✅ Transaction committed - all 3 jobs enqueued atomically")

	// Verify
	jobs, _ := store.ListJobs(ctx, storage.JobFilter{
		Types:    []string{"SendEmail"},
		Statuses: []storage.JobStatus{storage.StatusPending},
	})
	fmt.Printf("   Total pending SendEmail jobs: %d\n", len(jobs))

	// Clean up
	for _, job := range jobs {
		store.Dequeue(ctx)
		store.Complete(ctx, job.ID)
	}
}

func concurrentExample(ctx context.Context, store *sqlite.SQLiteStorage) {
	// Enqueue 10 jobs
	jobCount := 10
	fmt.Printf("Enqueuing %d jobs...\n", jobCount)

	for i := 1; i <= jobCount; i++ {
		emailData := EmailJob{
			To:      fmt.Sprintf("user%d@example.com", i),
			Subject: fmt.Sprintf("Concurrent job %d", i),
		}
		payload, _ := json.Marshal(emailData)

		job := &storage.Job{
			Type:    "SendEmail",
			Payload: payload,
		}

		store.Enqueue(ctx, job)
	}

	fmt.Printf("✅ %d jobs enqueued\n", jobCount)

	// Start 5 concurrent workers
	workerCount := 5
	fmt.Printf("Starting %d workers...\n", workerCount)

	var wg sync.WaitGroup
	processedCount := 0
	var mu sync.Mutex

	for w := 1; w <= workerCount; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for {
				job, err := store.Dequeue(ctx)
				if err != nil {
					log.Printf("Worker %d error: %v", workerID, err)
					return
				}

				if job == nil {
					// No more jobs
					return
				}

				var email EmailJob
				json.Unmarshal(job.Payload, &email)

				// Simulate work
				time.Sleep(50 * time.Millisecond)

				// Complete job
				store.Complete(ctx, job.ID)

				mu.Lock()
				processedCount++
				fmt.Printf("  Worker %d processed: %s\n", workerID, email.Subject)
				mu.Unlock()
			}
		}(w)
	}

	wg.Wait()
	fmt.Printf("✅ All workers finished - processed %d jobs\n", processedCount)
}

func lifecycleExample(ctx context.Context, store *sqlite.SQLiteStorage) {
	// Enqueue a job
	emailData := EmailJob{
		To:      "retry@example.com",
		Subject: "Job that will fail and retry",
	}
	payload, _ := json.Marshal(emailData)

	job := &storage.Job{
		Type:       "SendEmail",
		Payload:    payload,
		MaxRetries: 3,
	}

	jobID, _ := store.Enqueue(ctx, job)
	fmt.Printf("📬 Enqueued job: %s (max retries: %d)\n", jobID, job.MaxRetries)

	// Dequeue and fail
	dequeuedJob, _ := store.Dequeue(ctx)
	fmt.Printf("📧 Processing job %s...\n", dequeuedJob.ID)
	fmt.Println("❌ Job failed: network timeout")

	// Mark as failed
	store.Fail(ctx, dequeuedJob.ID, "network timeout")

	// Retry the job
	store.Retry(ctx, dequeuedJob.ID)
	fmt.Println("🔄 Job retried - back in pending state")

	// Dequeue again
	retriedJob, _ := store.Dequeue(ctx)
	if retriedJob != nil {
		fmt.Printf("📧 Processing retry attempt %d...\n", retriedJob.RetryCount)
		fmt.Println("✅ Job succeeded this time!")
		store.Complete(ctx, retriedJob.ID)
	}

	// Check final state
	finalJob, _ := store.GetJob(ctx, jobID)
	fmt.Printf("Final state: status=%s, retries=%d/%d\n",
		finalJob.Status, finalJob.RetryCount, finalJob.MaxRetries)
}

func showStatistics(ctx context.Context, store *sqlite.SQLiteStorage) {
	// Count jobs by status
	allJobs, _ := store.ListJobs(ctx, storage.JobFilter{})

	statusCounts := make(map[storage.JobStatus]int)
	for _, job := range allJobs {
		statusCounts[job.Status]++
	}

	fmt.Printf("Total jobs: %d\n", len(allJobs))
	fmt.Printf("  Pending:   %d\n", statusCounts[storage.StatusPending])
	fmt.Printf("  Running:   %d\n", statusCounts[storage.StatusRunning])
	fmt.Printf("  Completed: %d\n", statusCounts[storage.StatusCompleted])
	fmt.Printf("  Failed:    %d\n", statusCounts[storage.StatusFailed])
}

func getFileSize(path string) float64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return float64(info.Size())
}
