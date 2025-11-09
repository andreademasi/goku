package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/andreademasi/goku/pkg/queue"
	"github.com/andreademasi/goku/pkg/queue/storage/memory"
	"github.com/sirupsen/logrus"
)

type LogrusAdapter struct {
	logger *logrus.Logger
}

func NewLogrusAdapter(logger *logrus.Logger) *LogrusAdapter {
	return &LogrusAdapter{logger: logger}
}

func (a *LogrusAdapter) Debug(msg string, keysAndValues ...any) {
	a.logger.WithFields(a.toFields(keysAndValues)).Debug(msg)
}

func (a *LogrusAdapter) Info(msg string, keysAndValues ...any) {
	a.logger.WithFields(a.toFields(keysAndValues)).Info(msg)
}

func (a *LogrusAdapter) Warn(msg string, keysAndValues ...any) {
	a.logger.WithFields(a.toFields(keysAndValues)).Warn(msg)
}

func (a *LogrusAdapter) Error(msg string, keysAndValues ...any) {
	a.logger.WithFields(a.toFields(keysAndValues)).Error(msg)
}

func (a *LogrusAdapter) toFields(keysAndValues []any) logrus.Fields {
	fields := make(logrus.Fields)
	for i := 0; i < len(keysAndValues); i += 2 {
		if i+1 < len(keysAndValues) {
			key := fmt.Sprintf("%v", keysAndValues[i])
			fields[key] = keysAndValues[i+1]
		}
	}
	return fields
}

type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func main() {
	logger := logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: time.RFC3339,
	})
	logger.SetLevel(logrus.DebugLevel)
	logger.SetOutput(os.Stdout)

	gokuLogger := NewLogrusAdapter(logger)

	store := memory.New()

	q, err := queue.New(queue.Config{
		Storage: store,
		Workers: 3,
		Logger:  gokuLogger,
	})
	if err != nil {
		logger.WithError(err).Error("failed to create queue")
		os.Exit(1)
	}

	// Register handler
	q.Register("SendEmail", queue.HandlerFunc[EmailJob](
		func(ctx context.Context, job EmailJob) error {
			logger.WithFields(logrus.Fields{
				"to":      job.To,
				"subject": job.Subject,
			}).Info("sending email")
			time.Sleep(100 * time.Millisecond)
			return nil
		},
	).ToHandler())

	// Start queue
	if err := q.Start(); err != nil {
		logger.WithError(err).Error("failed to start queue")
		os.Exit(1)
	}
	defer q.Stop()

	logger.WithField("workers", 3).Info("queue started")

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		jobID, err := q.Enqueue(ctx, "SendEmail", EmailJob{
			To:      "user@example.com",
			Subject: "Test Email",
			Body:    "This is a test email",
		})
		if err != nil {
			logger.WithError(err).Error("failed to enqueue job")
			continue
		}
		logger.WithField("job_id", jobID).Info("job enqueued")
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	<-sigCh

	logger.Info("shutting down")
}
