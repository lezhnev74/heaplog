package scanner

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
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

// UgScanLocations works as UgScan but only searches the given locations,
// thus greatly reduces the time.
func UgScanLocations(file string, locations []common.Location, re string) (go_iterators.Iterator[MessageLayout], error) {
	var loc common.Location
	nextIt := func() (go_iterators.Iterator[MessageLayout], error) {
		if len(locations) == 0 {
			return nil, go_iterators.EmptyIterator
		}
		loc, locations = locations[0], locations[1:]
		return UgScanLocation(file, loc, re)
	}
	it := go_iterators.NewSequentialDynamicIterator(nextIt)
	return it, nil
}

func UgScanLocation(file string, loc common.Location, re string) (go_iterators.Iterator[MessageLayout], error) {

	ugLoc := fmt.Sprintf(
		`dd skip=%d count=%d bs=1 if=%s | ug -P --format="%s" "%s"`,
		loc.From,
		loc.To-loc.From,
		file,
		`%[0]b,%[1]b:%[1]d%~`,
		re,
	)

	ctx := context.Background()
	cmd := exec.CommandContext(ctx, "bash", "-c", ugLoc)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, xerrors.Errorf("scan ug connect out: %w", err)
	}
	err = cmd.Start()
	if err != nil {
		return nil, xerrors.Errorf("scan ug start: %w", err)
	}

	fileSize, err := common.FileSize(file)
	if err != nil {
		return nil, xerrors.Errorf("scan ug: %w", err)
	}

	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		return nil, NoMessageStartFound
	}
	lastLine := scanner.Text()
	if err := scanner.Err(); err != nil {
		return nil, xerrors.Errorf("scan ug: %w", err)
	}

	it := go_iterators.NewCallbackIterator(
		func() (s MessageLayout, err error) {
			if len(lastLine) == 0 {
				err = go_iterators.EmptyIterator
				return
			}

			m, d, dl := parseLine(lastLine)
			m += loc.From // correct the position relative to the whole file
			d += loc.From // correct the position relative to the whole file

			if !scanner.Scan() {
				// reached EOF
				s.From = m
				s.To = min(fileSize, loc.To) // the last message
				s.DateFrom = d
				s.DateTo = d + dl
				s.IsTail = true
				lastLine = ""
				return
			}

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

// parseLine relies on ug format: "%[0]b,%[1]b:%[1]d%~"
func parseLine(s string) (messageStart, dateStart, dateLen uint64) {
	l := unsafe.Slice(unsafe.StringData(s), len(s))

	var x int64

	// FIRST NUMBER: message start
	p := bytes.Index(l, []byte(","))
	if p == -1 {
		log.Fatalf("ug produced unexpected format: %s", string(l))
	}
	x, err := strconv.ParseInt(string(l[:p]), 10, 64)
	if err != nil {
		log.Fatalf("ug produced unexpected format: %s: %s", string(l), err)
	}
	messageStart = uint64(x)

	// SECOND NUMBER: date start
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

	// THIRD NUMBER: date len
	l = l[p+1:]
	x, err = strconv.ParseInt(string(l), 10, 64)
	if err != nil {
		log.Fatalf("ug produced unexpected format: %s: %s", string(l), err)
	}
	dateLen = uint64(x)

	return
}
