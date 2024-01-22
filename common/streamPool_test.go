package common

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"path"
	"testing"
)

func makeTempFiles(t *testing.T) []string {
	root, err := os.MkdirTemp("", "")
	require.NoError(t, err)

	files := []string{
		path.Join(root, "file1"),
		path.Join(root, "file2"),
	}

	for _, f := range files {
		_ = os.WriteFile(f, []byte(fmt.Sprintf("data %d", rand.Int())), os.ModePerm)
	}

	return files
}

func TestReuse(t *testing.T) {
	files := makeTempFiles(t)

	pool := NewStreamsPool(1)

	s1, err := pool.Get(files[0])
	require.NoError(t, err)

	err = pool.Put(s1)
	require.NoError(t, err)

	s2, err := pool.Get(files[0])
	require.NoError(t, err)
	require.EqualValues(t, s1, s2)
}

func TestEvict(t *testing.T) {
	files := makeTempFiles(t)
	pool := NewStreamsPool(1)

	s1, err := pool.Get(files[0])
	require.NoError(t, err)
	s2, err := pool.Get(files[0])
	require.NoError(t, err)

	require.NoError(t, pool.Put(s1))
	require.NoError(t, pool.Put(s2))

	s3, err := pool.Get(files[0])
	require.NoError(t, err)
	require.EqualValues(t, s2, s3)
}

func TestClosesIdempotent(t *testing.T) {
	files := makeTempFiles(t)
	pool := NewStreamsPool(1)

	s1, err := pool.Get(files[0])
	require.NoError(t, err)

	require.NoError(t, pool.Put(s1))

	require.NoError(t, pool.Close())
	require.NoError(t, pool.Close()) // idempotent?
}

func TestClosesAlreadyClosed(t *testing.T) {
	files := makeTempFiles(t)
	pool := NewStreamsPool(1)

	s1, err := pool.Get(files[0])
	require.NoError(t, err)
	s1.File.Close()

	require.NoError(t, pool.Put(s1))

	require.NoError(t, pool.Close()) // double close
}

func TestClosesOnEviction(t *testing.T) {
	files := makeTempFiles(t)
	pool := NewStreamsPool(1)

	var s1, s2 Stream

	// 1. Put one
	s1, err := pool.Get(files[0])
	require.NoError(t, err)
	require.NoError(t, pool.Put(s1))

	// 2. Put second (evicts first)
	s2, err = pool.Get(files[1])
	require.NoError(t, err)
	require.NoError(t, pool.Put(s2))

	require.ErrorIs(t, s1.File.Close(), os.ErrClosed)
}
