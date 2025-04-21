package scanner

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"unsafe"

	"heaplog_2024/common"
)

var (
	// NoMessageStartFound means the scanner failed to locate the start of a message in the stream
	NoMessageStartFound = fmt.Errorf("unable to find a message in the stream")
)

type MessageLayout struct {
	From, To         uint64 // body in the stream
	DateFrom, DateTo uint64 // [from,to) in the body
	IsTail           bool   // if message ended with EOF
}

// UgScanLocations works as UgScan but only searches the given locations,
// thus greatly reduces the time.
func UgScanLocations(file string, locations []common.Location, re string) (layouts []MessageLayout, err error) {
	var locLayouts []MessageLayout
	for _, loc := range locations {
		locLayouts, err = UgScanLocation(file, loc, re)
		if errors.Is(err, NoMessageStartFound) {
			return layouts, nil
		} else if err != nil {
			return nil, fmt.Errorf("ug scan locations: %w", err)
		}
		layouts = append(layouts, locLayouts...)
	}
	return
}

func UgScanLocation(file string, loc common.Location, re string) (layouts []MessageLayout, err error) {

	// Feed ug with only location bytes,
	// it skips the left file run, and starts at the loc.From
	// when the next message is beyond loc.To, it stops.
	ugLoc := fmt.Sprintf(
		`dd skip=%d bs=1 if=%s | ug -P --format="%s" "%s"`,
		loc.From,
		file,
		`%[0]b,%[1]b:%[1]d%~`,
		re,
	)

	ctx := context.Background()
	cmd := exec.CommandContext(ctx, "bash", "-c", ugLoc)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("scan ug connect out: %w", err)
	}
	err = cmd.Start()
	if err != nil {
		return nil, fmt.Errorf("scan ug start: %w", err)
	}

	putLayout := func(l MessageLayout) bool {
		if l.From >= loc.To {
			return false
		}
		layouts = append(layouts, l)
		return true
	}

	defer func() {
		if err != nil {
			return
		}
		err = cmd.Cancel()
		if err != nil {
			err = fmt.Errorf("ug finish: %w", err)
		}
		_ = cmd.Wait() // as we called Cancel above, so it returns non-nil error
	}()

	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		return nil, NoMessageStartFound
	}

	lastLine := scanner.Text()
	m, d, dl := parseLine(lastLine)

	fileSize, err := common.FileSize(file)
	if err != nil {
		return nil, fmt.Errorf("scan ug: %w", err)
	}

	for {
		l := MessageLayout{}
		l.From = loc.From + m
		l.DateFrom = loc.From + d
		l.DateTo = loc.From + d + dl

		if !scanner.Scan() {
			// reached EOF
			l.To = fileSize // the last message
			l.IsTail = true
			putLayout(l)
			break
		}

		lastLine = scanner.Text()
		m, d, dl = parseLine(lastLine)
		l.To = loc.From + m
		if !putLayout(l) {
			break // should stop
		}
	}

	return
}

// UgScan execs "ug" on the entire file and channels back each message offsets via the iterator
// based on https://github.com/Genivia/ugrep by Robert A. van Engelen
func UgScan(file string, re string, locations []common.Location) (layouts []MessageLayout, err error) {

	ctx := context.Background()
	cmd := exec.CommandContext(ctx, "ug", "-P", `--format=%[0]b,%[1]b:%[1]d%~`, re, file)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("scan ug connect out: %w", err)
	}
	err = cmd.Start()
	if err != nil {
		return nil, fmt.Errorf("scan ug start: %w", err)
	}

	defer func() {
		err = cmd.Wait()
		if err != nil {
			err = fmt.Errorf("ug finish: %w", err)
		}
	}()

	// When a new layout is scanned from the file,
	// here we decide if it within the given locations.
	putLayout := func(l MessageLayout) {
		for _, rloc := range locations {
			if rloc.Contains(l.From) {
				layouts = append(layouts, l) // yes, keep the layout
				return
			}
		}
	}

	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		return nil, NoMessageStartFound
	}

	lastLine := scanner.Text()
	m, d, dl := parseLine(lastLine)

	fileSize, err := common.FileSize(file)
	if err != nil {
		return nil, fmt.Errorf("scan ug: %w", err)
	}

	for {
		l := MessageLayout{}
		l.From = m
		l.DateFrom = d
		l.DateTo = d + dl

		if !scanner.Scan() {
			// reached EOF
			l.To = fileSize // the last message
			l.IsTail = true
			putLayout(l)
			break
		}

		lastLine = scanner.Text()
		m, d, dl = parseLine(lastLine)
		l.To = m
		putLayout(l)
	}

	return
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
