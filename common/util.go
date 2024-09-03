package common

import (
	"fmt"
	"golang.org/x/xerrors"
	"hash/crc32"
	"os"
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

func GroupSlice[T any](in []T, groupBy func(T) string) (groups [][]T) {
	var (
		curGroup  []T
		lastGroup string
	)
	for i, t := range in {
		if i == 0 {
			curGroup = append(curGroup, t)
			lastGroup = groupBy(t)
			continue
		}

		g := groupBy(t)
		if lastGroup == g {
			curGroup = append(curGroup, t)
			continue
		}

		groups = append(groups, curGroup)
		curGroup = []T{t}
		lastGroup = g
	}

	if len(curGroup) != 0 {
		groups = append(groups, curGroup)
	}

	return
}

func MakeTime(format, value string) time.Time {
	t, err := time.ParseInLocation(format, value, time.UTC)
	if err != nil {
		panic(err)
	}
	return t
}

func FileSize(path string) (uint64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		err = xerrors.Errorf("all files: %w", err)
		return 0, err
	}
	return uint64(fi.Size()), nil
}
