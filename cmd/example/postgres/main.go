package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	_ "github.com/lib/pq"

	"github.com/andreademasi/goku/pkg/queue"
	"github.com/andreademasi/goku/pkg/queue/storage/postgres"
)

type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

type ReportJob struct {
	ReportID string `json:"report_id"`
	UserID   string `json:"user_id"`
	Format   string `json:"format"`
}

func main() {
	fmt.Println("🥋 Goku Postgres Example")
	fmt.Println("==========================")
	fmt.Println()

	// Configure Postgres connection
	// Adjust this connection string for your environment
	connStr := os.Getenv("POSTGRES_URL")
	if connStr == "" {
		connStr = "postgres://postgres:postgres@localhost:5432/goku_queue?sslmode=disable"
		fmt.Println("💡 Using default connection string")
		fmt.Println("   Set POSTGRES_URL environment variable to customize")
		fmt.Println()
	}

	// Connect to Postgres
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to Postgres: %v", err)
	}
	defer db.Close()

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test connection
	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("Failed to ping Postgres: %v\n\nMake sure Postgres is running:\n  docker run -d --name postgres-goku -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:15", err)
	}

	fmt.Println("✅ Connected to Postgres")

	// Create storage backend
	store, err := postgres.New(db)
	if err != nil {
		log.Fatalf("Failed to create storage: %v", err)
	}

	// Initialize schema
	if err := postgres.InitSchema(ctx, db); err != nil {
		log.Fatalf("Failed to initialize schema: %v", err)
	}

	fmt.Println("✅ Schema initialized")
	fmt.Println()

	// Create queue
	q, err := queue.New(queue.Config{
		Storage:       store,
		Workers:       5,
		PollInterval:  500 * time.Millisecond,
		RetryStrategy: queue.ExponentialBackoff{},
		Logger:        &SimpleLogger{},
	})
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}

	fmt.Println("✅ Queue created with 5 workers")
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

	// Wait for interrupt
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
}

func registerHandlers(q *queue.Queue) {
	fmt.Println("📝 Registering job handlers...")

	q.Register("SendEmail", queue.HandlerFunc[EmailJob](func(ctx context.Context, job EmailJob) error {
		fmt.Printf("   📧 Sending email to %s: %s\n", job.To, job.Subject)
		time.Sleep(200 * time.Millisecond)
		return nil
	}).ToHandler())

	q.Register("GenerateReport", queue.HandlerFunc[ReportJob](func(ctx context.Context, job ReportJob) error {
		fmt.Printf("   📊 Generating %s report for user %s\n", job.Format, job.UserID)
		time.Sleep(500 * time.Millisecond)
		return nil
	}).ToHandler())

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

	fmt.Println("   ✅ 3 handlers registered")
	fmt.Println()
}

func enqueueExampleJobs(ctx context.Context, q *queue.Queue) {
	fmt.Println("📬 Enqueuing example jobs...")
	fmt.Println()

	// High priority emails
	fmt.Println("1️⃣  Priority jobs (processed by priority)")
	priorities := []struct {
		level   int
		subject string
	}{
		{10, "Regular Update"},
		{100, "URGENT: Security Alert"},
		{50, "Important: Action Required"},
	}

	for _, p := range priorities {
		jobID, err := q.EnqueueWithOptions(ctx, "SendEmail", EmailJob{
			To:      "user@example.com",
			Subject: p.subject,
			Body:    fmt.Sprintf("Priority level: %d", p.level),
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

	// Scheduled job
	fmt.Println("2️⃣  Scheduled job (runs in 5 seconds)")
	futureTime := time.Now().Add(5 * time.Second)
	jobID, err := q.EnqueueWithOptions(ctx, "SendEmail", EmailJob{
		To:      "future@example.com",
		Subject: "Scheduled Email",
		Body:    "This email was scheduled",
	}, queue.EnqueueOptions{
		ScheduledAt: futureTime,
	})
	if err != nil {
		log.Printf("Failed to enqueue: %v", err)
	} else {
		fmt.Printf("   Scheduled for %s: %s\n", futureTime.Format("15:04:05"), jobID)
	}
	fmt.Println()

	// Report generation
	fmt.Println("3️⃣  Report generation")
	jobID, err = q.Enqueue(ctx, "GenerateReport", ReportJob{
		ReportID: "RPT-2025-001",
		UserID:   "user_123",
		Format:   "PDF",
	})
	if err != nil {
		log.Printf("Failed to enqueue: %v", err)
	} else {
		fmt.Printf("   Enqueued: %s\n", jobID)
	}
	fmt.Println()

	// Job with retries
	fmt.Println("4️⃣  Flakey job (will retry until success)")
	jobID, err = q.EnqueueWithOptions(ctx, "FlakeyJob", "This job will fail twice", queue.EnqueueOptions{
		MaxRetries: 5,
	})
	if err != nil {
		log.Printf("Failed to enqueue: %v", err)
	} else {
		fmt.Printf("   Enqueued with 5 retries: %s\n", jobID)
	}
	fmt.Println()

	fmt.Println("✅ All jobs enqueued")
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
