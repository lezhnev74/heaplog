package ui

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHappPage(t *testing.T) {
	storageRoot := "/home/dmitry/Code/go/src/heaplog/heaplog_2024/_local/local_test/storage"

	cfg := DefaultCfg
	cfg.StoragePath = storageRoot
	happ, err := NewHeaplog(context.Background(), cfg, false)
	require.NoError(t, err)

	//pf, err := os.Create("/home/dmitry/Code/go/src/heaplog/heaplog_2024/_local/local_test/cpu.pprof")
	//require.NoError(t, err)
	//pprof.StartCPUProfile(pf)
	//defer pprof.StopCPUProfile()

	ms, err := happ.Page(4, nil, nil, 2, 1000, 0)
	require.NoError(t, err)

	for _, m := range ms {
		fmt.Println(len(m))
	}
	log.Printf("Found messages: %d\n", len(ms))
}
