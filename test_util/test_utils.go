package test_util

import (
	"database/sql"
	"fmt"
	"github.com/prometheus/procfs"
	"heaplog_2024/common"
	"log"
	"os"
	"path"
	"regexp"
	"time"
)

var (
	// these are default formats used in my tests:
	messageStartPattern = regexp.MustCompile(`(?m)^\[(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}\.?(\d{6}([+-]\d\d:\d\d)?)?)]`)
	dateLayout          = "2006-01-02T15:04:05.000000-07:00"
)

func MakeTimeV(value string) time.Time {
	t, err := time.ParseInLocation(dateLayout, value, time.UTC)
	if err != nil {
		panic(err)
	}
	return t
}

func MakeTimeP(value string) *time.Time {
	t := MakeTimeV(value)
	return &t
}

func DumpTable(db *sql.DB, table string, arguments int) {
	fmt.Printf("table %s\n", table)
	rows, err := db.Query(fmt.Sprintf(`SELECT * FROM %s`, table))
	if err != nil {
		panic(err)
	}
	vars := make([]any, arguments)
	row := 1
	for rows.Next() {
		switch arguments {
		case 1:
			err = rows.Scan(&vars[0])
			if err != nil {
				panic(err)
			}
		case 2:
			err = rows.Scan(&vars[0], &vars[1])
			if err != nil {
				panic(err)
			}
		case 3:
			err = rows.Scan(&vars[0], &vars[1], &vars[2])
			if err != nil {
				panic(err)
			}
		case 4:
			err = rows.Scan(&vars[0], &vars[1], &vars[2], &vars[3])
			if err != nil {
				panic(err)
			}
		case 5:
			err = rows.Scan(&vars[0], &vars[1], &vars[2], &vars[3], &vars[4])
			if err != nil {
				panic(err)
			}
		case 6:
			err = rows.Scan(&vars[0], &vars[1], &vars[2], &vars[3], &vars[4], &vars[5])
			if err != nil {
				panic(err)
			}
		}

		fmt.Printf("%s %d: %v\n", table, row, vars)
		row++
	}
}

func PopulateFile(root string, content []byte) string {
	fname := path.Join(root, fmt.Sprintf("%d.log", time.Now().UnixMicro()))
	os.WriteFile(fname, content, os.ModePerm)
	return fname
}

func BatchSlice[T any](in []T, l int) [][]T {
	r := make([][]T, 0)
	cur := make([]T, 0)
	for i := 0; i < len(in); i++ {
		cur = append(cur, in[i])
		i++

		if i%l == 0 {
			r = append(r, cur)
			cur = make([]T, 0)
		}
	}
	return r
}

func ProcStat() {
	p, err := procfs.Self()
	if err != nil {
		log.Fatalf("could not get process: %s", err)
	}
	stat, err := p.Stat()
	if err != nil {
		log.Fatalf("could not get process stat: %s", err)
	}
	common.Out("RSS: %dMb\n", stat.ResidentMemory()/1024/1024)
}
