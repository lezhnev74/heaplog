package main

import (
	"embed"
	"net/http"

	"heaplog_2024/internal/common"
	"heaplog_2024/internal/ui"
)

//go:embed frontend/public
var frontendPublic embed.FS

func main() {
	ctx := common.WaitSignal()
	heaplog := ui.NewHeaplog(ctx)
	httpApp := ui.NewHttpApp(http.FS(frontendPublic), heaplog)
	httpApp.Listen(":3000")
}
