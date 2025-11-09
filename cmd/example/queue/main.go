package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/andreademasi/goku/pkg/queue"
	"github.com/andreademasi/goku/pkg/queue/storage/sqlite"
)

// EmailJob represents an email to be sent
type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

// ReportJob represents a report generation task
type ReportJob struct {
	ReportID string   `json:"report_id"`
	UserID   string   `json:"user_id"`
	Format   string   `json:"format"`
	Filters  []string `json:"filters"`
}

// ImageJob represents an image processing task
type ImageJob struct {
	ImageURL string `json:"image_url"`
	Width    int    `json:"width"`
	Height   int    `json:"height"`
	Quality  int    `json:"quality"`
}

func main() {
	fmt.Println("🥋 Goku Queue API Example")
	fmt.Println("=========================")
	fmt.Println()

	// Create storage backend
	store, err := sqlite.New("queue_example.db")
	if err != nil {
		log.Fatalf("Failed to create storage: %v", err)
	}
	defer store.Close()

	// Initialize schema
	ctx := context.Background()
	if err := store.InitSchema(ctx); err != nil {
		log.Fatalf("Failed to initialize schema: %v", err)
	}

	fmt.Println("✅ Storage initialized")

	// Create queue with custom configuration
	q, err := queue.New(queue.Config{
		Storage:       store,
		Workers:       3,
		PollInterval:  500 * time.Millisecond,
		RetryStrategy: queue.ExponentialBackoff{},
		Logger:        &SimpleLogger{},
	})
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}

	fmt.Println("✅ Queue created with 3 workers")
	fmt.Println()

	// Register job handlers with type safety
	registerHandlers(q)

	// Start processing jobs
	if err := q.Start(); err != nil {
		log.Fatalf("Failed to start queue: %v", err)
	}
	fmt.Println("✅ Queue started - workers processing jobs")
	fmt.Println()

	// Enqueue various jobs
	enqueueExampleJobs(ctx, q)

	// Wait for interrupt signal
	fmt.Println("\n💡 Press Ctrl+C to gracefully shutdown...")
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	<-sigCh

	fmt.Println("\n🛑 Shutting down gracefully...")

	// Graceful shutdown
	if err := q.Stop(); err != nil {
		log.Printf("Warning: shutdown error: %v", err)
	}

	fmt.Println("✅ Shutdown complete")
	fmt.Printf("   Database: queue_example.db\n")
	fmt.Println("\n👋 Goodbye!")
}

func registerHandlers(q *queue.Queue) {
	fmt.Println("📝 Registering job handlers...")

	// Register email handler
	q.Register("SendEmail", queue.HandlerFunc[EmailJob](func(ctx context.Context, job EmailJob) error {
		fmt.Printf("   📧 Sending email to %s: %s\n", job.To, job.Subject)
		time.Sleep(200 * time.Millisecond) // Simulate work
		return nil
	}).ToHandler())

	// Register report handler
	q.Register("GenerateReport", queue.HandlerFunc[ReportJob](func(ctx context.Context, job ReportJob) error {
		fmt.Printf("   📊 Generating %s report for user %s\n", job.Format, job.UserID)
		time.Sleep(500 * time.Millisecond) // Simulate work
		return nil
	}).ToHandler())

	// Register image processing handler
	q.Register("ProcessImage", queue.HandlerFunc[ImageJob](func(ctx context.Context, job ImageJob) error {
		fmt.Printf("   🖼️  Processing image: %s (%dx%d)\n", job.ImageURL, job.Width, job.Height)
		time.Sleep(300 * time.Millisecond) // Simulate work
		return nil
	}).ToHandler())

	// Register a handler that demonstrates retries
	attemptCount := 0
	q.Register("FlakeyJob", queue.HandlerFunc[string](func(ctx context.Context, message string) error {
		attemptCount++
		if attemptCount < 3 {
			fmt.Printf("   ⚠️  FlakeyJob attempt %d failed: %s\n", attemptCount, message)
			return fmt.Errorf("temporary failure on attempt %d", attemptCount)
		}
		fmt.Printf("   ✅ FlakeyJob succeeded on attempt %d: %s\n", attemptCount, message)
		return nil
	}).ToHandler())

	fmt.Println("   ✅ 4 handlers registered")
	fmt.Println()
}

func enqueueExampleJobs(ctx context.Context, q *queue.Queue) {
	fmt.Println("📬 Enqueuing example jobs...")
	fmt.Println()

	// Example 1: Simple email jobs
	fmt.Println("1️⃣  Simple email jobs (default priority)")
	for i, email := range []string{"alice@example.com", "bob@example.com", "charlie@example.com"} {
		jobID, err := q.Enqueue(ctx, "SendEmail", EmailJob{
			To:      email,
			Subject: fmt.Sprintf("Welcome! #%d", i+1),
			Body:    "Welcome to Goku job queue",
		})
		if err != nil {
			log.Printf("Failed to enqueue email: %v", err)
			continue
		}
		fmt.Printf("   Enqueued: %s\n", jobID)
	}
	fmt.Println()

	// Example 2: Priority jobs
	fmt.Println("2️⃣  Priority jobs (high priority processed first)")
	priorities := []struct {
		level   int
		name    string
		subject string
	}{
		{10, "normal", "Regular Update"},
		{100, "urgent", "URGENT: Security Alert"},
		{50, "high", "Important: Action Required"},
	}

	for _, p := range priorities {
		jobID, err := q.EnqueueWithOptions(ctx, "SendEmail", EmailJob{
			To:      fmt.Sprintf("%s@example.com", p.name),
			Subject: p.subject,
			Body:    fmt.Sprintf("Priority level: %d", p.level),
		}, queue.EnqueueOptions{
			Priority: p.level,
		})
		if err != nil {
			log.Printf("Failed to enqueue priority job: %v", err)
			continue
		}
		fmt.Printf("   Priority %d: %s\n", p.level, jobID)
	}
	fmt.Println()

	// Example 3: Scheduled job
	fmt.Println("3️⃣  Scheduled job (runs in 5 seconds)")
	futureTime := time.Now().Add(5 * time.Second)
	jobID, err := q.EnqueueWithOptions(ctx, "SendEmail", EmailJob{
		To:      "future@example.com",
		Subject: "Scheduled Email",
		Body:    "This email was scheduled",
	}, queue.EnqueueOptions{
		ScheduledAt: futureTime,
	})
	if err != nil {
		log.Printf("Failed to enqueue scheduled job: %v", err)
	} else {
		fmt.Printf("   Scheduled for %s: %s\n", futureTime.Format("15:04:05"), jobID)
	}
	fmt.Println()

	// Example 4: Report generation
	fmt.Println("4️⃣  Report generation job")
	jobID, err = q.Enqueue(ctx, "GenerateReport", ReportJob{
		ReportID: "RPT-2025-001",
		UserID:   "user_123",
		Format:   "PDF",
		Filters:  []string{"last_month", "active_users"},
	})
	if err != nil {
		log.Printf("Failed to enqueue report: %v", err)
	} else {
		fmt.Printf("   Enqueued: %s\n", jobID)
	}
	fmt.Println()

	// Example 5: Image processing
	fmt.Println("5️⃣  Image processing job")
	jobID, err = q.Enqueue(ctx, "ProcessImage", ImageJob{
		ImageURL: "https://example.com/photo.jpg",
		Width:    800,
		Height:   600,
		Quality:  85,
	})
	if err != nil {
		log.Printf("Failed to enqueue image job: %v", err)
	} else {
		fmt.Printf("   Enqueued: %s\n", jobID)
	}
	fmt.Println()

	// Example 6: Job with retries
	fmt.Println("6️⃣  Flakey job (will retry until success)")
	jobID, err = q.EnqueueWithOptions(ctx, "FlakeyJob", "This job will fail twice", queue.EnqueueOptions{
		MaxRetries: 5,
	})
	if err != nil {
		log.Printf("Failed to enqueue flakey job: %v", err)
	} else {
		fmt.Printf("   Enqueued with 5 retries: %s\n", jobID)
	}
	fmt.Println()

	fmt.Println("✅ All jobs enqueued")
}

// SimpleLogger implements the queue.Logger interface
type SimpleLogger struct{}

func (l *SimpleLogger) Debug(msg string, keysAndValues ...any) {
	// Uncomment to see debug logs
	// fmt.Printf("DEBUG: %s", msg)
	// for i := 0; i < len(keysAndValues); i += 2 {
	//	if i+1 < len(keysAndValues) {
	//		fmt.Printf(" %v=%v", keysAndValues[i], keysAndValues[i+1])
	//	}
	// }
	// fmt.Println()
}

func (l *SimpleLogger) Info(msg string, keysAndValues ...any) {
	// Log important events
	if msg == "job enqueued" || msg == "queue started" || msg == "queue stopped gracefully" {
		// fmt.Printf("INFO: %s", msg)
		// ... print keysAndValues if needed
	}
}

func (l *SimpleLogger) Warn(msg string, keysAndValues ...any) {
	// Uncomment to see warning logs
	// fmt.Printf("WARN: %s", msg)
	// ... print keysAndValues if needed
}

func (l *SimpleLogger) Error(msg string, keysAndValues ...any) {
	fmt.Printf("ERROR: %s", msg)
	for i := 0; i < len(keysAndValues); i += 2 {
		if i+1 < len(keysAndValues) {
			fmt.Printf(" %v=%v", keysAndValues[i], keysAndValues[i+1])
		}
	}
	fmt.Println()
}
