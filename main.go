package main

import (
	"embed"
	"log"
	"os"

	"go.uber.org/zap"

	"heaplog_2024/internal"
	"heaplog_2024/internal/common"
	"heaplog_2024/internal/ui"
)

//go:embed frontend/public
var frontendPublic embed.FS

func main() {
	logger, err := internal.NewLogger("prod")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = logger.Sync() }()

	ctx := common.WaitSignal()
	console := ui.NewConsole(ctx, logger, frontendPublic)
	if err = console.Run(ctx, os.Args); err != nil {
		logger.Error("heaplog failed", zap.Error(err))
	}
}
