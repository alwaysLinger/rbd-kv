package log

import (
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Logger interface {
	Debug(msg string, fields ...Field)
	Info(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	Error(msg string, fields ...Field)
	Fatal(msg string, fields ...Field)
	With(fields ...Field) Logger
	Sync() error
}

type Field = zap.Field

var (
	String  = zap.String
	Int     = zap.Int
	Int64   = zap.Int64
	Uint64  = zap.Uint64
	Float64 = zap.Float64
	Bool    = zap.Bool
	Error   = zap.Error
	Any     = zap.Any
)

type zapLogger struct {
	logger *zap.Logger
}

func NewLogger() (Logger, error) {
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "l",
		CallerKey:      "at",
		MessageKey:     "msg",
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.TimeEncoderOfLayout(time.DateTime + ".000"),
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	config := zap.Config{
		Level:            zap.NewAtomicLevelAt(zap.InfoLevel),
		Development:      false,
		Encoding:         "json",
		EncoderConfig:    encoderConfig,
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}

	logger, err := config.Build(zap.AddCallerSkip(1))
	if err != nil {
		return nil, err
	}

	return &zapLogger{logger: logger}, nil
}

func (l *zapLogger) Debug(msg string, fields ...Field) {
	l.logger.Debug(msg, fields...)
}

func (l *zapLogger) Info(msg string, fields ...Field) {
	l.logger.Info(msg, fields...)
}

func (l *zapLogger) Warn(msg string, fields ...Field) {
	l.logger.Warn(msg, fields...)
}

func (l *zapLogger) Error(msg string, fields ...Field) {
	l.logger.Error(msg, fields...)
}

func (l *zapLogger) Fatal(msg string, fields ...Field) {
	l.logger.Fatal(msg, fields...)
}

func (l *zapLogger) With(fields ...Field) Logger {
	return &zapLogger{logger: l.logger.With(fields...)}
}

func (l *zapLogger) Sync() error {
	return l.logger.Sync()
}

var NopLogger = &nopLogger{}

type nopLogger struct{}

func (l *nopLogger) Debug(msg string, fields ...Field) {}
func (l *nopLogger) Info(msg string, fields ...Field)  {}
func (l *nopLogger) Warn(msg string, fields ...Field)  {}
func (l *nopLogger) Error(msg string, fields ...Field) {}
func (l *nopLogger) Fatal(msg string, fields ...Field) {}
func (l *nopLogger) With(fields ...Field) Logger       { return l }
func (l *nopLogger) Sync() error                       { return nil }
