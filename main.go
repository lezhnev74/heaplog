package main

import (
	"heaplog_2024/ui"
	"log"
	"os"
)

func main() {
	consoleApp := ui.PrepareConsoleApp()
	if err := consoleApp.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
