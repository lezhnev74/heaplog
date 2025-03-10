package common

import (
	"context"
	"database/sql"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"time"

	"golang.org/x/xerrors"
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

// InstantTick is a forever ticker that starts instantly
func InstantTick(d time.Duration) chan time.Time {
	tick := time.NewTicker(d)
	ret := make(chan time.Time)
	go func() {
		ret <- time.Now() // instant tick happens here
		for {
			ret <- <-tick.C // then we just connect it to the ticker
		}
	}()
	return ret
}

func PrintMem(db *sql.DB) (rss uint64) {
	var (
		name, dbSize, blockSize, walSize, memSize, memLimit string
		totalBlocks, usedBlocks, freeBlocks                 int64
	)
	r := db.QueryRow(`PRAGMA database_size`)
	err := r.Scan(&name, &dbSize, &blockSize, &totalBlocks, &usedBlocks, &freeBlocks, &walSize, &memSize, &memLimit)
	if err != nil {
		log.Print(err)
		return
	}
	freeBlocksPct := 0
	if totalBlocks > 0 {
		freeBlocksPct = int(float64(freeBlocks) / float64(totalBlocks) * 100)
	}
	Out("DuckDB: fileSize:%s, walSize:%s, mem:%s/%s, Blcs[total/used/free/freePcs]:%d,%d,%d,%d%% ",
		dbSize, walSize, memSize, memLimit, totalBlocks, usedBlocks, freeBlocks, freeBlocksPct)

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	Out(
		"System: %s, %s",
		fmt.Sprintf("RSS:%dMiB", m.Sys/1024/1024),             // total virtual memory reserved from OS
		fmt.Sprintf("HeapAlloc:%dMiB", m.HeapAlloc/1024/1024), // HeapAlloc is bytes of allocated heap objects.
	)

	return m.Sys
}

var EnableLogging bool

func Out(pattern string, args ...any) {
	if EnableLogging {
		log.Printf(pattern, args...)
	}
}

func CleanMem() {
	runtime.GC()
	debug.FreeOSMemory()

	// since the main runtime for the app is Docker, clearing caches allows the app to stay below the max mem limit.
	ctx := context.Background()
	cmd := exec.CommandContext(ctx, "bash", "-c", `echo 3 > /proc/sys/vm/drop_caches`)
	out, err := cmd.CombinedOutput()
	Out("cleanmem: %s", out)
	if err != nil {
		Out("cleanmem error: %s", err)
		return
	}
}

func DumpMemoryIn(d time.Duration) {
	time.Sleep(d)
	f2, err := os.Create(fmt.Sprintf("/storage/%s_profile_mem.tmp", time.Now().Format("150405")))
	if err != nil {
		log.Fatal("could not create mem profile: ", err)
	}
	if err := pprof.WriteHeapProfile(f2); err != nil {
		log.Fatal("could not start mem profile: ", err)
	}
	f2.Close()
}

func ProfileCPU(fn func()) {
	tt := time.Now()
	f, err := os.Create(fmt.Sprintf("./%s_profile_cpu.tmp", time.Now().Format("150405")))
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}

	fn()

	log.Printf("profiled in %s", time.Since(tt).String())
	pprof.StopCPUProfile()
	defer func() { _ = f.Close() }()
}
