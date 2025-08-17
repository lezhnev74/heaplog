package ingest

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"iter"
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

type ScannedMessage struct {
	common.MessageLayout
	IsTail bool
}

func toMessageLayouts(in []ScannedMessage) []common.MessageLayout {
	out := make([]common.MessageLayout, len(in))
	for i, m := range in {
		out[i] = m.MessageLayout
	}
	return out
}

// scan execs "ug" on the entire file and returns all message offsets within the given locations.
// The "ug" command is based on https://github.com/Genivia/ugrep by Robert A. van Engelen.
// It uses a custom format to extract message boundaries and date ranges.
// Returns NoMessageStartFound error if no messages are found in the stream.
// Returns error if there are issues executing ug or accessing the file.
func scan(file string, fileSize int, re string, locations []common.Location) (
	layouts iter.Seq[ScannedMessage],
	err error,
) {
	ctx := context.Background()
	cmd := exec.CommandContext(ctx, "ug", "-P", `--format=%[0]b,%[1]b:%[1]d%~`, re, file)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("ug stdout err: %w", err)
	}
	err = cmd.Start()
	if err != nil {
		return nil, fmt.Errorf("ug exec: %w", err)
	}

	// When a new layout is scanned from the file,
	// here we decide if it within the given locations.
	matched := func(l ScannedMessage) bool {
		for _, rloc := range locations {
			if rloc.Contains(l.Loc.From) {
				return true
			}
		}
		return false
	}

	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		return nil, NoMessageStartFound
	}

	lastLine := scanner.Text()
	m, d, dl := parseLine(lastLine)

	if err != nil {
		return nil, fmt.Errorf("parse line: %w", err)
	}

	return func(yield func(ScannedMessage) bool) {
		defer func() {
			err = cmd.Wait()
			if err != nil {
				panic(fmt.Errorf("ug finish: %w", err))
			}
		}()

		for {
			l := ScannedMessage{}
			l.Loc.From = m
			l.DateLoc.From = d
			l.DateLoc.To = d + dl

			if !scanner.Scan() {
				// reached EOF
				l.Loc.To = fileSize // the last message
				l.IsTail = true
				if matched(l) {
					if !yield(l) {
						return
					}
				}
				break
			}

			lastLine = scanner.Text()
			m, d, dl = parseLine(lastLine)
			l.Loc.To = m
			if matched(l) {
				if !yield(l) {
					return
				}
			}
		}
	}, nil
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
