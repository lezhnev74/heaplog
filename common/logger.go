package common

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func init() {
	// Initialize the logger with default settings
	logger, err := NewLogger()
	if err != nil {
		panic(err)
	}
	zap.ReplaceGlobals(logger)
}

func NewLogger() (*zap.Logger, error) {
	config := zap.NewProductionConfig()
	config.OutputPaths = []string{"stdout"}
	config.Encoding = "console"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	config.DisableStacktrace = true
	config.InitialFields = map[string]any{}

	l, err := config.Build()
	if err != nil {
		return nil, err
	}
	return l, err
}
