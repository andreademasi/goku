package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/andreademasi/goku/pkg/queue"
	"github.com/andreademasi/goku/pkg/queue/storage"
	"github.com/andreademasi/goku/pkg/queue/storage/sqlite"
)

type OrderJob struct {
	OrderID     string  `json:"order_id"`
	CustomerID  string  `json:"customer_id"`
	TotalAmount float64 `json:"total_amount"`
}

type NotificationJob struct {
	Type    string `json:"type"`
	UserID  string `json:"user_id"`
	Message string `json:"message"`
}

type AuditJob struct {
	Action   string `json:"action"`
	EntityID string `json:"entity_id"`
	UserID   string `json:"user_id"`
}

func main() {
	fmt.Println("🥋 Goku Transactional Enqueuing Example")
	fmt.Println("==========================================")
	fmt.Println()
	fmt.Println("This example demonstrates atomic transactional")
	fmt.Println("enqueuing using SQLite. The same patterns work")
	fmt.Println("with Postgres as well.")
	fmt.Println()

	// Create SQLite storage
	dbPath := "transactions_example.db"
	store, err := sqlite.New(dbPath)
	if err != nil {
		log.Fatalf("Failed to create storage: %v", err)
	}
	defer store.Close()

	fmt.Printf("✅ Database created: %s\n", dbPath)

	// Initialize schema
	ctx := context.Background()
	if err := store.InitSchema(ctx); err != nil {
		log.Fatalf("Failed to initialize schema: %v", err)
	}

	fmt.Println("✅ Schema initialized")
	fmt.Println()

	// Create queue
	q, err := queue.New(queue.Config{
		Storage:       store,
		Workers:       2,
		PollInterval:  500 * time.Millisecond,
		RetryStrategy: queue.ExponentialBackoff{},
		Logger:        &SimpleLogger{},
	})
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}

	fmt.Println("✅ Queue created with 2 workers")
	fmt.Println()

	// Register handlers
	registerHandlers(q)

	// Start processing
	if err := q.Start(); err != nil {
		log.Fatalf("Failed to start queue: %v", err)
	}

	fmt.Println("✅ Queue started")
	fmt.Println()

	// Run examples
	runExamples(ctx, store)

	// Wait a bit for jobs to process
	fmt.Println("\n⏳ Processing jobs...")
	time.Sleep(3 * time.Second)

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
	fmt.Printf("\n💡 Database: %s\n", dbPath)
	fmt.Println("   You can inspect it with sqlite3 or delete it")
	fmt.Println("\n👋 Goodbye!")
}

func registerHandlers(q *queue.Queue) {
	fmt.Println("📝 Registering job handlers...")

	q.Register("ProcessOrder", queue.HandlerFunc[OrderJob](func(ctx context.Context, job OrderJob) error {
		fmt.Printf("   💳 Processing order %s for customer %s ($%.2f)\n",
			job.OrderID, job.CustomerID, job.TotalAmount)
		time.Sleep(200 * time.Millisecond)
		return nil
	}).ToHandler())

	q.Register("SendNotification", queue.HandlerFunc[NotificationJob](func(ctx context.Context, job NotificationJob) error {
		fmt.Printf("   📧 Sending %s notification to %s: %s\n",
			job.Type, job.UserID, job.Message)
		time.Sleep(100 * time.Millisecond)
		return nil
	}).ToHandler())

	q.Register("AuditLog", queue.HandlerFunc[AuditJob](func(ctx context.Context, job AuditJob) error {
		fmt.Printf("   📝 Audit: %s on %s by %s\n",
			job.Action, job.EntityID, job.UserID)
		time.Sleep(50 * time.Millisecond)
		return nil
	}).ToHandler())

	fmt.Println("   ✅ 3 handlers registered")
	fmt.Println()
}

func runExamples(ctx context.Context, store *sqlite.SQLiteStorage) {
	// Example 1: Simple transaction - all or nothing
	fmt.Println("Example 1: Atomic Batch Enqueuing")
	fmt.Println("-----------------------------------")
	fmt.Println("Enqueuing 3 related jobs atomically...")
	fmt.Println("Either all 3 jobs are enqueued, or none are.")
	fmt.Println()

	if err := example1AtomicBatch(ctx, store); err != nil {
		log.Printf("Example 1 failed: %v", err)
	}

	// Example 2: Transaction with business logic
	fmt.Println("\nExample 2: Order Processing Pipeline")
	fmt.Println("--------------------------------------")
	fmt.Println("Simulating an order that creates multiple jobs atomically.")
	fmt.Println()

	if err := example2OrderProcessing(ctx, store); err != nil {
		log.Printf("Example 2 failed: %v", err)
	}

	// Example 3: Transaction rollback on error
	fmt.Println("\nExample 3: Transaction Rollback")
	fmt.Println("---------------------------------")
	fmt.Println("Demonstrating rollback when something goes wrong.")
	fmt.Println()

	if err := example3RollbackDemo(ctx, store); err != nil {
		fmt.Printf("✅ Expected error (transaction rolled back): %v\n", err)
	}
}

func example1AtomicBatch(ctx context.Context, store *sqlite.SQLiteStorage) error {
	// Begin transaction
	tx, err := store.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}

	// Enqueue multiple jobs in the transaction
	jobs := []struct {
		jobType string
		payload interface{}
	}{
		{"SendNotification", NotificationJob{
			Type:    "email",
			UserID:  "user_001",
			Message: "Welcome to our service!",
		}},
		{"SendNotification", NotificationJob{
			Type:    "sms",
			UserID:  "user_001",
			Message: "Your account is ready",
		}},
		{"AuditLog", AuditJob{
			Action:   "user_signup",
			EntityID: "user_001",
			UserID:   "system",
		}},
	}

	fmt.Println("📬 Adding jobs to transaction...")
	for i, j := range jobs {
		job := &storage.Job{
			Type: j.jobType,
		}

		// Encode payload
		payload, err := encodePayload(j.payload)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to encode payload: %w", err)
		}
		job.Payload = payload

		jobID, err := tx.Enqueue(ctx, job)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to enqueue job %d: %w", i+1, err)
		}
		fmt.Printf("   %d. Added to tx: %s (%s)\n", i+1, jobID, j.jobType)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit tx: %w", err)
	}

	fmt.Println("✅ Transaction committed - all 3 jobs enqueued atomically")
	return nil
}

func example2OrderProcessing(ctx context.Context, store *sqlite.SQLiteStorage) error {
	// Simulate processing an order
	order := OrderJob{
		OrderID:     "ORD-2025-001",
		CustomerID:  "CUST-123",
		TotalAmount: 149.99,
	}

	fmt.Printf("📦 Processing order: %s\n", order.OrderID)

	// Begin transaction
	tx, err := store.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}

	// Create all related jobs in a transaction
	// If any fails, none will be created

	// 1. Process the order
	orderPayload, _ := encodePayload(order)
	job1 := &storage.Job{
		Type:    "ProcessOrder",
		Payload: orderPayload,
	}
	jobID1, err := tx.Enqueue(ctx, job1)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to enqueue order job: %w", err)
	}
	fmt.Printf("   1. Order processing: %s\n", jobID1)

	// 2. Send confirmation email
	emailPayload, _ := encodePayload(NotificationJob{
		Type:    "email",
		UserID:  order.CustomerID,
		Message: fmt.Sprintf("Your order %s has been confirmed!", order.OrderID),
	})
	job2 := &storage.Job{
		Type:    "SendNotification",
		Payload: emailPayload,
	}
	jobID2, err := tx.Enqueue(ctx, job2)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to enqueue notification: %w", err)
	}
	fmt.Printf("   2. Email notification: %s\n", jobID2)

	// 3. Create audit log
	auditPayload, _ := encodePayload(AuditJob{
		Action:   "order_created",
		EntityID: order.OrderID,
		UserID:   order.CustomerID,
	})
	job3 := &storage.Job{
		Type:    "AuditLog",
		Payload: auditPayload,
	}
	jobID3, err := tx.Enqueue(ctx, job3)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to enqueue audit: %w", err)
	}
	fmt.Printf("   3. Audit log: %s\n", jobID3)

	// Commit all jobs together
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	fmt.Println("✅ Order pipeline created atomically")
	fmt.Println("   All 3 jobs are guaranteed to exist or none do")
	return nil
}

func example3RollbackDemo(ctx context.Context, store *sqlite.SQLiteStorage) error {
	fmt.Println("🔄 Starting transaction...")

	tx, err := store.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}

	// Add some jobs
	fmt.Println("   Adding job 1...")
	payload1, _ := encodePayload(NotificationJob{
		Type:    "email",
		UserID:  "user_002",
		Message: "This job will never be processed",
	})
	job1 := &storage.Job{
		Type:    "SendNotification",
		Payload: payload1,
	}
	jobID1, _ := tx.Enqueue(ctx, job1)
	fmt.Printf("   Added to tx: %s\n", jobID1)

	fmt.Println("   Adding job 2...")
	payload2, _ := encodePayload(NotificationJob{
		Type:    "sms",
		UserID:  "user_002",
		Message: "This one won't be processed either",
	})
	job2 := &storage.Job{
		Type:    "SendNotification",
		Payload: payload2,
	}
	jobID2, _ := tx.Enqueue(ctx, job2)
	fmt.Printf("   Added to tx: %s\n", jobID2)

	// Simulate an error (e.g., business logic validation fails)
	fmt.Println("   ❌ Simulating error in business logic...")
	fmt.Println("   Rolling back transaction...")

	if err := tx.Rollback(); err != nil {
		return fmt.Errorf("rollback failed: %w", err)
	}

	fmt.Println("✅ Transaction rolled back - no jobs were persisted")
	return fmt.Errorf("simulated business logic error")
}

func encodePayload(v interface{}) ([]byte, error) {
	// In a real app, you'd use json.Marshal
	// For this example, we'll keep it simple
	return []byte(fmt.Sprintf("%+v", v)), nil
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
