package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/andreademasi/goku/pkg/queue"
	"github.com/andreademasi/goku/pkg/queue/storage/memory"
	"go.uber.org/zap"
)

// ZapAdapter adapts zap.Logger to queue.Logger interface
type ZapAdapter struct {
	logger *zap.Logger
}

func NewZapAdapter(logger *zap.Logger) *ZapAdapter {
	return &ZapAdapter{logger: logger}
}

func (a *ZapAdapter) Debug(msg string, keysAndValues ...any) {
	a.logger.Debug(msg, convertToZapFields(keysAndValues)...)
}

func (a *ZapAdapter) Info(msg string, keysAndValues ...any) {
	a.logger.Info(msg, convertToZapFields(keysAndValues)...)
}

func (a *ZapAdapter) Warn(msg string, keysAndValues ...any) {
	a.logger.Warn(msg, convertToZapFields(keysAndValues)...)
}

func (a *ZapAdapter) Error(msg string, keysAndValues ...any) {
	a.logger.Error(msg, convertToZapFields(keysAndValues)...)
}

func convertToZapFields(keysAndValues []any) []zap.Field {
	fields := make([]zap.Field, 0, len(keysAndValues)/2)
	for i := 0; i < len(keysAndValues); i += 2 {
		if i+1 < len(keysAndValues) {
			key := fmt.Sprintf("%v", keysAndValues[i])
			fields = append(fields, zap.Any(key, keysAndValues[i+1]))
		}
	}
	return fields
}

type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

type ReportJob struct {
	ReportType string `json:"report_type"`
	UserID     int    `json:"user_id"`
}

func main() {
	fmt.Println("=== Goku with Zap Logger Example ===")
	fmt.Println()

	fmt.Println("1. Using default zap logger (no logger specified):")
	runWithDefaultLogger()

	fmt.Println("\n" + strings.Repeat("-", 60) + "\n")

	fmt.Println("2. Using custom zap development logger:")
	runWithDevelopmentLogger()

	fmt.Println("\n" + strings.Repeat("-", 60) + "\n")

	fmt.Println("3. Using fully custom zap logger:")
	runWithCustomLogger()
}

func runWithDefaultLogger() {
	store := memory.New()

	q, err := queue.New(queue.Config{
		Storage: store,
		Workers: 2,
	})
	if err != nil {
		fmt.Printf("Error creating queue: %v\n", err)
		return
	}

	q.Register("SendEmail", queue.HandlerFunc[EmailJob](
		func(ctx context.Context, job EmailJob) error {
			fmt.Printf("  → Sending email to %s\n", job.To)
			time.Sleep(50 * time.Millisecond)
			return nil
		},
	).ToHandler())

	if err := q.Start(); err != nil {
		fmt.Printf("Error starting queue: %v\n", err)
		return
	}
	defer q.Stop()

	ctx := context.Background()
	jobID, _ := q.Enqueue(ctx, "SendEmail", EmailJob{
		To:      "user@example.com",
		Subject: "Welcome",
		Body:    "Thanks for signing up!",
	})

	fmt.Printf("  Job enqueued: %s\n", jobID)
	time.Sleep(200 * time.Millisecond)
}

func runWithDevelopmentLogger() {
	store := memory.New()

	zapLogger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Printf("Error creating logger: %v\n", err)
		return
	}

	logger := NewZapAdapter(zapLogger)

	q, err := queue.New(queue.Config{
		Storage: store,
		Workers: 2,
		Logger:  logger,
	})
	if err != nil {
		fmt.Printf("Error creating queue: %v\n", err)
		return
	}

	q.Register("GenerateReport", queue.HandlerFunc[ReportJob](
		func(ctx context.Context, job ReportJob) error {
			fmt.Printf("  → Generating %s report for user %d\n", job.ReportType, job.UserID)
			time.Sleep(50 * time.Millisecond)
			return nil
		},
	).ToHandler())

	if err := q.Start(); err != nil {
		fmt.Printf("Error starting queue: %v\n", err)
		return
	}
	defer q.Stop()

	ctx := context.Background()
	jobID, _ := q.Enqueue(ctx, "GenerateReport", ReportJob{
		ReportType: "Monthly Sales",
		UserID:     123,
	})

	fmt.Printf("  Job enqueued: %s\n", jobID)
	time.Sleep(200 * time.Millisecond)
}

func runWithCustomLogger() {
	store := memory.New()

	zapLogger, err := zap.Config{
		Level:            zap.NewAtomicLevelAt(zap.InfoLevel),
		Development:      false,
		Encoding:         "json",
		EncoderConfig:    zap.NewProductionEncoderConfig(),
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}.Build()
	if err != nil {
		fmt.Printf("Error creating zap logger: %v\n", err)
		return
	}
	defer zapLogger.Sync()

	logger := NewZapAdapter(zapLogger)

	q, err := queue.New(queue.Config{
		Storage: store,
		Workers: 2,
		Logger:  logger,
	})
	if err != nil {
		fmt.Printf("Error creating queue: %v\n", err)
		return
	}

	q.Register("SendEmail", queue.HandlerFunc[EmailJob](
		func(ctx context.Context, job EmailJob) error {
			fmt.Printf("  → Sending email to %s\n", job.To)
			time.Sleep(50 * time.Millisecond)
			return nil
		},
	).ToHandler())

	if err := q.Start(); err != nil {
		fmt.Printf("Error starting queue: %v\n", err)
		return
	}
	defer q.Stop()

	ctx := context.Background()
	jobID, _ := q.Enqueue(ctx, "SendEmail", EmailJob{
		To:      "admin@example.com",
		Subject: "Alert",
		Body:    "System alert notification",
	})

	fmt.Printf("  Job enqueued: %s\n", jobID)
	time.Sleep(200 * time.Millisecond)
}

func runFullExample() {
	zapLogger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Printf("Error creating logger: %v\n", err)
		os.Exit(1)
	}

	logger := NewZapAdapter(zapLogger)

	store := memory.New()

	q, err := queue.New(queue.Config{
		Storage:         store,
		Workers:         3,
		Logger:          logger,
		PollInterval:    500 * time.Millisecond,
		ShutdownTimeout: 10 * time.Second,
	})
	if err != nil {
		fmt.Printf("Error creating queue: %v\n", err)
		os.Exit(1)
	}

	q.Register("SendEmail", queue.HandlerFunc[EmailJob](
		func(ctx context.Context, job EmailJob) error {
			fmt.Printf("  → Sending email to %s\n", job.To)
			time.Sleep(50 * time.Millisecond)
			return nil
		},
	).ToHandler())

	q.Register("GenerateReport", queue.HandlerFunc[ReportJob](
		func(ctx context.Context, job ReportJob) error {
			fmt.Printf("  → Generating %s report for user %d\n", job.ReportType, job.UserID)
			time.Sleep(50 * time.Millisecond)
			return nil
		},
	).ToHandler())

	if err := q.Start(); err != nil {
		fmt.Printf("Error starting queue: %v\n", err)
		os.Exit(1)
	}
	defer q.Stop()

	zapLogger.Info("queue started", zap.Int("workers", 3))

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		q.Enqueue(ctx, "SendEmail", EmailJob{
			To:      fmt.Sprintf("user%d@example.com", i),
			Subject: "Test Email",
			Body:    "This is a test",
		})
	}

	for i := 0; i < 2; i++ {
		q.Enqueue(ctx, "GenerateReport", ReportJob{
			ReportType: "Sales Report",
			UserID:     100 + i,
		})
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	<-sigCh

	zapLogger.Info("shutting down gracefully")
}
