package main

import (
	"log"
	"os"

	"heaplog_2024/ui"
)

func main() {
	consoleApp := ui.PrepareConsoleApp()
	if err := consoleApp.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
