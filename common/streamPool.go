package common

import (
	"errors"
	"log"
	"os"
	"sync"
)

type Stream struct {
	*os.File
	Path string
}

// StreamsPool is an evictable cache of file descriptors.
// it maintains the max number of open descriptors and evicts(closes) old ones.
type StreamsPool struct {
	streams []*Stream
	sync.Locker
}

// Get checks the pool and return a descriptor associated with the same path.
// If nothing found, it makes a new descriptor and returns to the client.
func (p *StreamsPool) Get(path string) (Stream, error) {
	p.Lock()

	for i, s := range p.streams {
		if s == nil {
			continue
		}
		if s.Path == path {
			copy(p.streams[i:], p.streams[i+1:])
			p.streams[len(p.streams)-1] = nil
			p.Unlock() // early return
			return *s, nil
		}
	}

	p.Unlock() // let others continue working with the cache

	// otherwise, open a new descriptor:
	f, err := os.Open(path)
	if err != nil {
		return Stream{}, err
	}
	return Stream{File: f, Path: path}, nil
}

// Put returns a stream to the pool, possibly evicting the oldest existing one
func (p *StreamsPool) Put(s Stream) error {
	p.Lock()
	defer p.Unlock()

	// evict the last one
	if last := p.streams[len(p.streams)-1]; last != nil {
		if err := last.Close(); err != nil {
			return err
		}
	}

	// shift
	copy(p.streams[1:], p.streams[:])

	// put at the head
	p.streams[0] = &s

	return nil
}

// Close the pool and all existing descriptors
func (p *StreamsPool) Close() error {
	p.Lock()
	defer p.Unlock()

	for i, s := range p.streams {
		if s == nil {
			continue
		}
		if err := s.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			return err
		}
		p.streams[i] = nil
	}

	return nil
}

func NewStreamsPool(cap int) *StreamsPool {
	if cap < 1 {
		log.Fatal("cap is below 1")
	}
	return &StreamsPool{
		make([]*Stream, cap),
		&sync.Mutex{},
	}
}
