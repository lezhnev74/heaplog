package common

import (
	"encoding/binary"
	"fmt"
	"github.com/ronanh/intcomp"
	"hash/crc32"
	"os"
	"path"
	"time"
)

var (
	crc32t = crc32.MakeTable(0xD5828281)
)

// HashString is a quick and idempotent hashing
func HashString(s string) string {
	h := crc32.Checksum([]byte(s), crc32t)
	return fmt.Sprintf("%d", h)
}

func SliceToAny[T any](in []T) (ret []any) {
	ret = make([]any, len(in))
	for i, v := range in {
		ret[i] = v
	}
	return
}

func FilterSliceInPlace[T any](input []T, filter func(elem T) bool) []T {
	n := 0
	for _, elem := range input {
		if filter(elem) {
			input[n] = elem
			n++
		}
	}
	return input[:n]
}

func MakeTime(format, value string) time.Time {
	t, err := time.ParseInLocation(format, value, time.UTC)
	if err != nil {
		panic(err)
	}
	return t
}

func GetFilenames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	files := make([]string, 0)
	for _, de := range entries {
		if de.IsDir() {
			continue
		}

		files = append(files, path.Join(dir, de.Name()))
	}

	return files, nil
}

func CompressUint64(items []uint64) ([]byte, error) {
	b := make([]byte, 8)

	encoded := intcomp.CompressUint64(items, nil)
	out := make([]byte, 0, len(encoded)*8)

	for _, u := range encoded {
		binary.BigEndian.PutUint64(b[:8], u)
		out = append(out, b[:8]...)
	}

	return out, nil
}

func DecompressUint64(data []byte) (items []uint64, err error) {
	valueInts := make([]uint64, 0)

	for i := 0; i < len(data); i += 8 {
		valueInts = append(valueInts, binary.BigEndian.Uint64(data[i:i+8]))
	}

	items = intcomp.UncompressUint64(valueInts, nil)

	return
}
