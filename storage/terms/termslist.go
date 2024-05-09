package terms

import (
	"slices"
	"sync"
)

type termsFile struct {
	lock sync.RWMutex
	// Full path to the file
	path string
	// len is used in merging policy to merge the smallest files first
	len int64
}

func (f *termsFile) safeRead(fn func()) {
	f.lock.RLock()
	defer f.lock.RUnlock()

	fn()
}

func (f *termsFile) safeWrite(fn func()) {
	f.lock.Lock()
	defer f.lock.Unlock()

	fn()
}

type TermsFileList struct {
	files []*termsFile
	lock  sync.RWMutex
}

func NewTermsList() *TermsFileList {
	return &TermsFileList{
		files: make([]*termsFile, 0),
		lock:  sync.RWMutex{},
	}
}

func (f *TermsFileList) safeRead(fn func()) {
	f.lock.RLock()
	defer f.lock.RUnlock()

	fn()
}

func (f *TermsFileList) safeWrite(fn func()) {
	f.lock.Lock()
	defer f.lock.Unlock()

	fn()
}

func (f *TermsFileList) putFile(newFile *termsFile) {
	// For the purposes of merging, files are to be sorted by len asc
	pos, _ := slices.BinarySearchFunc(f.files, newFile, func(a, b *termsFile) int {
		if a.len < b.len {
			return -1
		} else if a.len > b.len {
			return 1
		}
		return 0
	})

	f.files = append(
		f.files[:pos],
		append([]*termsFile{newFile}, f.files[pos:]...)...,
	)
}
