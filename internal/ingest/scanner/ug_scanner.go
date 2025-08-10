package scanner

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"unsafe"

	"heaplog_2024/internal/common"
)

var (
	// NoMessageStartFound means the scanner failed to locate the start of a message in the stream
	NoMessageStartFound = fmt.Errorf("unable to find a message in the stream")
)

type MessageLayout struct {
	From, To         int  // body in the stream
	DateFrom, DateTo int  // [from,to) in the body
	IsTail           bool // if message ended with EOF
}

// Scan execs "ug" on the entire file and returns all message offsets within the given locations.
// The "ug" command is based on https://github.com/Genivia/ugrep by Robert A. van Engelen.
// It uses a custom format to extract message boundaries and date ranges.
// Returns NoMessageStartFound error if no messages are found in the stream.
// Returns error if there are issues executing ug or accessing the file.
func Scan(file string, fileSize int, re string, locations []common.Location) (layouts []MessageLayout, err error) {
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
func parseLine(s string) (messageStart, dateStart, dateLen int) {
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
	messageStart = int(x)

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
	dateStart = int(x)

	// THIRD NUMBER: date len
	l = l[p+1:]
	x, err = strconv.ParseInt(string(l), 10, 64)
	if err != nil {
		log.Fatalf("ug produced unexpected format: %s: %s", string(l), err)
	}
	dateLen = int(x)

	return
}
