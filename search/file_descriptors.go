package search

import (
	"heaplog/common"
	"os"
	"sync"
)

type FileDescriptors struct {
	descriptors   map[common.DataSourceHash][]*os.File
	lock          sync.Mutex
	getPathByHash func(hash common.DataSourceHash) (string, error)
}

func NewFileDescriptors(getFileByHash func(hash common.DataSourceHash) (string, error)) *FileDescriptors {
	return &FileDescriptors{
		make(map[common.DataSourceHash][]*os.File),
		sync.Mutex{},
		getFileByHash,
	}
}

func (fd *FileDescriptors) newDescriptor(ds common.DataSourceHash) (*os.File, error) {
	path, err := fd.getPathByHash(ds)
	if err != nil {
		return nil, err
	}
	return os.Open(path) // note: here we detect a missing file (this can happen mid-fight)
}

func (fd *FileDescriptors) getDescriptor(ds common.DataSourceHash) (descriptor *os.File, err error) {
	fd.lock.Lock()
	defer fd.lock.Unlock()

	dsDescriptors := fd.descriptors[ds]
	if len(dsDescriptors) == 0 {
		return fd.newDescriptor(ds)
	}

	lastPos := len(fd.descriptors[ds]) - 1
	descriptor, fd.descriptors[ds] = dsDescriptors[lastPos], fd.descriptors[ds][:lastPos]

	return
}

func (fd *FileDescriptors) returnDescriptor(ds common.DataSourceHash, descriptor *os.File) {
	fd.lock.Lock()
	defer fd.lock.Unlock()

	fd.descriptors[ds] = append(fd.descriptors[ds], descriptor)
}

func (fd *FileDescriptors) close() {
	fd.lock.Lock()
	defer fd.lock.Unlock()

	for _, descriptors := range fd.descriptors {
		for _, descriptor := range descriptors {
			_ = descriptor.Close()
		}
	}
}
