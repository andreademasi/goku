package queue

import (
	"strings"
)

type Logger interface {
	Debug(msg string, keysAndValues ...any)
	Info(msg string, keysAndValues ...any)
	Warn(msg string, keysAndValues ...any)
	Error(msg string, keysAndValues ...any)
}

type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
	LogLevelNone
)

func (l LogLevel) String() string {
	switch l {
	case LogLevelDebug:
		return "DEBUG"
	case LogLevelInfo:
		return "INFO"
	case LogLevelWarn:
		return "WARN"
	case LogLevelError:
		return "ERROR"
	case LogLevelNone:
		return "NONE"
	default:
		return "UNKNOWN"
	}
}

func ParseLogLevel(s string) LogLevel {
	switch strings.ToUpper(s) {
	case "DEBUG":
		return LogLevelDebug
	case "INFO":
		return LogLevelInfo
	case "WARN", "WARNING":
		return LogLevelWarn
	case "ERROR":
		return LogLevelError
	case "NONE":
		return LogLevelNone
	default:
		return LogLevelInfo
	}
}

type NoopLogger struct{}

func NewNoopLogger() *NoopLogger {
	return &NoopLogger{}
}

func (l *NoopLogger) Debug(msg string, keysAndValues ...any) {}
func (l *NoopLogger) Info(msg string, keysAndValues ...any)  {}
func (l *NoopLogger) Warn(msg string, keysAndValues ...any)  {}
func (l *NoopLogger) Error(msg string, keysAndValues ...any) {}

func defaultLogger() Logger {
	return NewNoopLogger()
}
