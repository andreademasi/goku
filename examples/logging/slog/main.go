package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/andreademasi/goku/pkg/queue"
	"github.com/andreademasi/goku/pkg/queue/storage/memory"
)

type SlogAdapter struct {
	logger *slog.Logger
}

func NewSlogAdapter(logger *slog.Logger) *SlogAdapter {
	return &SlogAdapter{logger: logger}
}

func (a *SlogAdapter) Debug(msg string, keysAndValues ...any) {
	a.logger.Debug(msg, keysAndValues...)
}

func (a *SlogAdapter) Info(msg string, keysAndValues ...any) {
	a.logger.Info(msg, keysAndValues...)
}

func (a *SlogAdapter) Warn(msg string, keysAndValues ...any) {
	a.logger.Warn(msg, keysAndValues...)
}

func (a *SlogAdapter) Error(msg string, keysAndValues ...any) {
	a.logger.Error(msg, keysAndValues...)
}

type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	gokuLogger := NewSlogAdapter(logger)

	store := memory.New()

	q, err := queue.New(queue.Config{
		Storage: store,
		Workers: 3,
		Logger:  gokuLogger,
	})
	if err != nil {
		logger.Error("failed to create queue", "error", err)
		os.Exit(1)
	}

	// Register handler
	q.Register("SendEmail", queue.HandlerFunc[EmailJob](
		func(ctx context.Context, job EmailJob) error {
			logger.Info("sending email",
				slog.String("to", job.To),
				slog.String("subject", job.Subject),
			)
			time.Sleep(100 * time.Millisecond)
			return nil
		},
	).ToHandler())

	// Start queue
	if err := q.Start(); err != nil {
		logger.Error("failed to start queue", "error", err)
		os.Exit(1)
	}
	defer q.Stop()

	logger.Info("queue started", slog.Int("workers", 3))

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		jobID, err := q.Enqueue(ctx, "SendEmail", EmailJob{
			To:      "user@example.com",
			Subject: "Test Email",
			Body:    "This is a test email",
		})
		if err != nil {
			logger.Error("failed to enqueue job", "error", err)
			continue
		}
		logger.Info("job enqueued", slog.String("job_id", jobID))
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	<-sigCh

	logger.Info("shutting down")
}
