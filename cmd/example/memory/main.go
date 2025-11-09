package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/andreademasi/goku/pkg/queue"
	"github.com/andreademasi/goku/pkg/queue/storage/memory"
)

type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

type DataProcessingJob struct {
	DataID     string   `json:"data_id"`
	Operations []string `json:"operations"`
}

func main() {
	fmt.Println("🥋 Goku In-Memory Example")
	fmt.Println("===========================")
	fmt.Println()

	// Create in-memory storage
	store := memory.New()

	fmt.Println("✅ In-memory storage created")

	// Create queue
	ctx := context.Background()
	q, err := queue.New(queue.Config{
		Storage:       store,
		Workers:       3,
		PollInterval:  100 * time.Millisecond, // Fast polling for memory backend
		RetryStrategy: queue.ExponentialBackoff{},
		Logger:        &SimpleLogger{},
	})
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}

	fmt.Println("✅ Queue created with 3 workers")
	fmt.Println()

	// Register handlers
	registerHandlers(q)

	// Start processing
	if err := q.Start(); err != nil {
		log.Fatalf("Failed to start queue: %v", err)
	}

	fmt.Println("✅ Queue started - workers processing jobs")
	fmt.Println()

	// Enqueue example jobs
	enqueueExampleJobs(ctx, q)

	// Wait for a bit to process jobs
	fmt.Println("\n⏳ Processing jobs for 10 seconds...")
	time.Sleep(10 * time.Second)

	// Wait for interrupt
	fmt.Println("\n💡 Press Ctrl+C to shutdown...")
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	<-sigCh

	fmt.Println("\n🛑 Shutting down...")

	// Graceful shutdown
	if err := q.Stop(); err != nil {
		log.Printf("Warning: shutdown error: %v", err)
	}

	fmt.Println("✅ Shutdown complete")
}

func registerHandlers(q *queue.Queue) {
	fmt.Println("📝 Registering job handlers...")

	q.Register("SendEmail", queue.HandlerFunc[EmailJob](func(ctx context.Context, job EmailJob) error {
		fmt.Printf("   📧 Sending email to %s: %s\n", job.To, job.Subject)
		time.Sleep(150 * time.Millisecond)
		return nil
	}).ToHandler())

	q.Register("ProcessData", queue.HandlerFunc[DataProcessingJob](func(ctx context.Context, job DataProcessingJob) error {
		fmt.Printf("   🔄 Processing data %s: %v\n", job.DataID, job.Operations)
		time.Sleep(300 * time.Millisecond)
		return nil
	}).ToHandler())

	// Simulate a job that occasionally fails
	var attemptCount int
	q.Register("RandomFailJob", queue.HandlerFunc[string](func(ctx context.Context, message string) error {
		attemptCount++
		if attemptCount%4 == 0 { // Fail every 4th attempt
			fmt.Printf("   ❌ RandomFailJob failed: %s\n", message)
			return fmt.Errorf("random failure")
		}
		fmt.Printf("   ✅ RandomFailJob succeeded: %s\n", message)
		return nil
	}).ToHandler())

	fmt.Println("   ✅ 3 handlers registered")
	fmt.Println()
}

func enqueueExampleJobs(ctx context.Context, q *queue.Queue) {
	fmt.Println("📬 Enqueuing example jobs...")
	fmt.Println()

	// Example 1: Batch email jobs
	fmt.Println("1️⃣  Batch email jobs")
	emails := []string{"alice@example.com", "bob@example.com", "charlie@example.com", "diana@example.com"}
	for i, email := range emails {
		jobID, err := q.Enqueue(ctx, "SendEmail", EmailJob{
			To:      email,
			Subject: fmt.Sprintf("Welcome! #%d", i+1),
			Body:    "Welcome to Goku job queue",
		})
		if err != nil {
			log.Printf("Failed to enqueue: %v", err)
			continue
		}
		fmt.Printf("   Enqueued: %s\n", jobID)
	}
	fmt.Println()

	// Example 2: Priority jobs
	fmt.Println("2️⃣  Priority jobs (high priority first)")
	priorities := []struct {
		level   int
		subject string
	}{
		{1, "Low Priority Newsletter"},
		{100, "CRITICAL: System Alert"},
		{50, "Important Update"},
		{10, "Regular Notification"},
	}

	for _, p := range priorities {
		jobID, err := q.EnqueueWithOptions(ctx, "SendEmail", EmailJob{
			To:      "priority@example.com",
			Subject: p.subject,
			Body:    fmt.Sprintf("Priority: %d", p.level),
		}, queue.EnqueueOptions{
			Priority: p.level,
		})
		if err != nil {
			log.Printf("Failed to enqueue: %v", err)
			continue
		}
		fmt.Printf("   Priority %d: %s\n", p.level, jobID)
	}
	fmt.Println()

	// Example 3: Data processing jobs
	fmt.Println("3️⃣  Data processing jobs")
	for i := 1; i <= 3; i++ {
		jobID, err := q.Enqueue(ctx, "ProcessData", DataProcessingJob{
			DataID:     fmt.Sprintf("DATA-%03d", i),
			Operations: []string{"validate", "transform", "store"},
		})
		if err != nil {
			log.Printf("Failed to enqueue: %v", err)
			continue
		}
		fmt.Printf("   Enqueued: %s\n", jobID)
	}
	fmt.Println()

	// Example 4: Scheduled job
	fmt.Println("4️⃣  Scheduled job (runs in 3 seconds)")
	futureTime := time.Now().Add(3 * time.Second)
	jobID, err := q.EnqueueWithOptions(ctx, "SendEmail", EmailJob{
		To:      "scheduled@example.com",
		Subject: "Scheduled Email",
		Body:    "This was scheduled",
	}, queue.EnqueueOptions{
		ScheduledAt: futureTime,
	})
	if err != nil {
		log.Printf("Failed to enqueue: %v", err)
	} else {
		fmt.Printf("   Scheduled for %s: %s\n", futureTime.Format("15:04:05"), jobID)
	}
	fmt.Println()

	// Example 5: Jobs with retries
	fmt.Println("5️⃣  Jobs with retries")
	for i := 1; i <= 2; i++ {
		jobID, err := q.EnqueueWithOptions(ctx, "RandomFailJob",
			fmt.Sprintf("Retry test %d", i),
			queue.EnqueueOptions{
				MaxRetries: 3,
			})
		if err != nil {
			log.Printf("Failed to enqueue: %v", err)
			continue
		}
		fmt.Printf("   Enqueued with retries: %s\n", jobID)
	}
	fmt.Println()

	fmt.Println("✅ All jobs enqueued")
	fmt.Println()
	fmt.Println("💡 Note: In-memory storage is extremely fast!")
	fmt.Println("   Jobs are processed almost instantly")
}

type SimpleLogger struct{}

func (l *SimpleLogger) Debug(msg string, keysAndValues ...any) {}

func (l *SimpleLogger) Info(msg string, keysAndValues ...any) {}

func (l *SimpleLogger) Warn(msg string, keysAndValues ...any) {}

func (l *SimpleLogger) Error(msg string, keysAndValues ...any) {
	fmt.Printf("ERROR: %s", msg)
	for i := 0; i < len(keysAndValues); i += 2 {
		if i+1 < len(keysAndValues) {
			fmt.Printf(" %v=%v", keysAndValues[i], keysAndValues[i+1])
		}
	}
	fmt.Println()
}
