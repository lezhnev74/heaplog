package internal

import (
	"go.uber.org/zap"
)

func NewLogger(env string) (*zap.Logger, error) {
	switch env {
	case "prod", "production":
		return zap.NewProduction()
	case "dev", "development":
		return zap.NewDevelopment()
	default:
		// In tests or unknown env, use development config but without noisy stack traces
		cfg := zap.NewDevelopmentConfig()
		cfg.DisableStacktrace = true
		return cfg.Build()
	}
}
