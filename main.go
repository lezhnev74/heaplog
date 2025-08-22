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
	logger, err := internal.NewLogger("dev")
	if err != nil {
		log.Fatal(err)
	}
	defer logger.Sync()

	ctx := common.WaitSignal()
	//heaplog := ui.NewHeaplog(ctx)
	//httpApp := ui.NewHttpApp(ctx, http.FS(frontendPublic), heaplog)
	//httpApp.Listen(":3000")

	console := ui.NewConsole(ctx, logger)
	if err := console.Run(ctx, os.Args); err != nil {
		logger.Error("application failed", zap.Error(err))
	}
}
