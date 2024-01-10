package ingest

import (
	"slices"
	"sync"
)

type blacklistedFiles struct {
	files []string
	sync.Mutex
}

func (bf *blacklistedFiles) add(file string) {
	bf.Lock()
	defer bf.Unlock()
	if !slices.Contains(bf.files, file) {
		bf.files = append(bf.files, file)
	}
}

func (bf *blacklistedFiles) test(file string) bool {
	bf.Lock()
	defer bf.Unlock()
	return slices.Contains(bf.files, file)
}
