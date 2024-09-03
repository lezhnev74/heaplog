package scanner

import (
	"bufio"
	"bytes"
	"context"
	go_iterators "github.com/lezhnev74/go-iterators"
	"golang.org/x/xerrors"
	"heaplog_2024/common"
	"log"
	"os/exec"
	"strconv"
	"unsafe"
)

var (
	// NoMessageStartFound means the scanner failed to locate the start of a message in the stream
	NoMessageStartFound = xerrors.Errorf("unable to find a message in the stream")
)

type MessageLayout struct {
	From, To         uint64 // body in the stream
	DateFrom, DateTo uint64 // [from,to) in the body
	IsTail           bool   // if message ended with EOF
}

// UgScan execs "ug" and channels back each message offsets via the iterator
// based on https://github.com/Genivia/ugrep by Robert A. van Engelen
func UgScan(file string, re string) (go_iterators.Iterator[MessageLayout], error) {
	ctx := context.Background()
	cmd := exec.CommandContext(ctx, "ug", "-P", `--format=%[0]b,%[1]b:%[1]d%~`, re, file)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, xerrors.Errorf("scan ug connect out: %w", err)
	}
	err = cmd.Start()
	if err != nil {
		return nil, xerrors.Errorf("scan ug start: %w", err)
	}

	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		return nil, NoMessageStartFound
	}
	lastLine := scanner.Text()
	fileSize, err := common.FileSize(file)
	if err != nil {
		return nil, xerrors.Errorf("scan ug: %w", err)
	}
	if err := scanner.Err(); err != nil {
		return nil, xerrors.Errorf("scan ug: %w", err)
	}

	it := go_iterators.NewCallbackIterator(
		func() (s MessageLayout, err error) {
			if len(lastLine) == 0 {
				err = go_iterators.EmptyIterator
				return
			}

			if !scanner.Scan() {
				// reached EOF
				m, d, dl := parseLine(lastLine)
				s.From = m
				s.To = fileSize // the last message
				s.DateFrom = d
				s.DateTo = d + dl
				s.IsTail = true
				lastLine = ""
				return
			}

			m, d, dl := parseLine(lastLine)
			lastLine = scanner.Text()
			m1, _, _ := parseLine(lastLine)

			s.From = m
			s.To = m1
			s.DateFrom = d
			s.DateTo = d + dl

			return
		},
		func() (err error) {
			cmd.Cancel() // kill the process even if it has not finished yet
			err = cmd.Wait()
			if err != nil {
				err = xerrors.Errorf("scan ug wait: %w", err)
			}
			return
		},
	)

	return it, nil
}

func parseLine(s string) (messageStart, dateStart, dateLen uint64) {
	l := unsafe.Slice(unsafe.StringData(s), len(s))

	var x int64

	// FIRST NUMBER
	p := bytes.Index(l, []byte(","))
	if p == -1 {
		log.Fatalf("ug produced unexpected format: %s", string(l))
	}
	x, err := strconv.ParseInt(string(l[:p]), 10, 64)
	if err != nil {
		log.Fatalf("ug produced unexpected format: %s: %s", string(l), err)
	}
	messageStart = uint64(x)

	// SECOND NUMBER
	l = l[p+1:]
	p = bytes.Index(l, []byte(":"))
	if p == -1 {
		log.Fatalf("ug produced unexpected format: %s", string(l))
	}
	x, err = strconv.ParseInt(string(l[:p]), 10, 64)
	if err != nil {
		log.Fatalf("ug produced unexpected format: %s: %s", string(l), err)
	}
	dateStart = uint64(x)

	// THIRD NUMBER
	l = l[p+1:]
	x, err = strconv.ParseInt(string(l), 10, 64)
	if err != nil {
		log.Fatalf("ug produced unexpected format: %s: %s", string(l), err)
	}
	dateLen = uint64(x)

	return
}
